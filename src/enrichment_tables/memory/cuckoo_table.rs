use std::{hash::RandomState, num::NonZeroUsize, time::Duration};

use async_trait::async_trait;
use bytes::Bytes;
use cuckoo_clock::{
    CuckooFilter,
    config::{CounterConfig, CuckooConfiguration, LruConfig, TtlConfig},
};
use futures::{StreamExt, stream::BoxStream};
use tokio::time::interval;
use tokio_stream::wrappers::IntervalStream;
use vector_config::configurable_component;
use vector_lib::{
    EstimatedJsonEncodedSizeOf,
    enrichment::{Case, Condition, IndexHandle, Table},
    event::{Event, EventStatus, Finalizable},
    internal_event::{
        ByteSize, BytesSent, CountByteSize, EventsSent, InternalEventHandle, Output, Protocol,
    },
    sink::StreamSink,
};
use vrl::value::{KeyString, ObjectMap, Value};

use crate::enrichment_tables::memory::{
    MemoryConfig,
    internal_events::{MemoryEnrichmentTableRead, MemoryEnrichmentTableReadFailed},
};

/// A struct that implements [vector_lib::enrichment::Table] to handle loading enrichment data from a cuckoo table.
#[derive(Clone)]
pub(super) struct CuckooMemoryTable {
    filter: CuckooFilter<RandomState>,
    pub(super) config: MemoryConfig,
}

/// Configuration of cuckoo filter for memory table.
#[configurable_component]
#[derive(Clone, Debug, PartialEq, Eq)]
#[serde(deny_unknown_fields, rename_all = "snake_case", tag = "type")]
pub struct CuckooMemoryConfig {
    /// Number of bits used for fingerprint.
    #[serde(default = "default_cuckoo_fingerprint_bits")]
    pub fingerprint_bits: NonZeroUsize,
    /// Number of slots in each bucket
    #[serde(default = "default_cuckoo_bucket_size")]
    pub bucket_size: NonZeroUsize,
    /// Maximum number of entries that can be stored in the filter (actual capacity will usually be
    /// larger)
    pub max_entries: usize,
    /// Max number of kicks when experiencing hash collisions.
    #[serde(default = "default_cuckoo_max_kicks")]
    pub max_kicks: usize,
    /// Can be set to true to use LRU strategy for kicking.
    #[serde(default = "crate::serde::default_false")]
    pub lru_enabled: bool,
    /// Can be set to true to also track TTL for entries.
    #[serde(default = "crate::serde::default_true")]
    pub ttl_enabled: bool,
    /// Number of bits to use to track TTL. Low bit count will reduce maximum TTL and also require a
    /// worse resolution to keep working.
    #[serde(default = "default_cuckoo_ttl_bits")]
    pub ttl_bits: NonZeroUsize,
    /// Can be set to true to track a count alongside hashes.
    #[serde(default = "crate::serde::default_false")]
    pub counter_enabled: bool,
    /// Number of bits to use to track counter. This will limit the max value.
    #[serde(default = "default_cuckoo_counter_bits")]
    pub counter_bits: NonZeroUsize,
}

const fn default_cuckoo_fingerprint_bits() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(8) }
}

const fn default_cuckoo_bucket_size() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(4) }
}

const fn default_cuckoo_ttl_bits() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(8) }
}

const fn default_cuckoo_counter_bits() -> NonZeroUsize {
    unsafe { NonZeroUsize::new_unchecked(8) }
}

const fn default_cuckoo_max_kicks() -> usize {
    500
}

impl CuckooMemoryTable {
    /// Creates a new [CuckooMemoryTable] based on the provided config.
    pub(super) fn new(
        config: MemoryConfig,
        cuckoo_config: CuckooMemoryConfig,
    ) -> crate::Result<Self> {
        let ttl_val = config.ttl / config.scan_interval.get();
        let mut builder = CuckooConfiguration::builder(cuckoo_config.max_entries)
            .fingerprint_bits(cuckoo_config.fingerprint_bits.get().try_into()?)
            .bucket_size(cuckoo_config.bucket_size)
            .max_kicks(cuckoo_config.max_kicks);

        if cuckoo_config.lru_enabled {
            builder = builder.with_lru(LruConfig::default());
        }

        if cuckoo_config.ttl_enabled {
            builder = builder.with_ttl(TtlConfig {
                ttl: u32::try_from(ttl_val)?.try_into()?,
                ttl_bits: cuckoo_config.ttl_bits.get().try_into()?,
            });
        }

        if cuckoo_config.counter_enabled {
            builder = builder.with_counter(CounterConfig {
                counter_bits: cuckoo_config.counter_bits.get().try_into()?,
                ..Default::default()
            });
        }

        let built_config = builder.build()?;
        Ok(Self {
            config,
            filter: CuckooFilter::new_random(built_config),
        })
    }

    fn handle_value(&self, value: ObjectMap) {
        for (k, _) in value.iter() {
            let _ = self.filter.insert_if_not_present(k);
        }
    }
}

impl Table for CuckooMemoryTable {
    fn find_table_row<'a>(
        &self,
        case: Case,
        condition: &'a [Condition<'a>],
        select: Option<&'a [String]>,
        wildcard: Option<&Value>,
        index: Option<IndexHandle>,
    ) -> Result<ObjectMap, String> {
        let mut rows = self.find_table_rows(case, condition, select, wildcard, index)?;

        match rows.pop() {
            Some(row) if rows.is_empty() => Ok(row),
            Some(_) => Err("More than 1 row found".to_string()),
            None => Err("Key not found".to_string()),
        }
    }

    fn find_table_rows<'a>(
        &self,
        _case: Case,
        condition: &'a [Condition<'a>],
        _select: Option<&'a [String]>,
        _wildcard: Option<&Value>,
        _index: Option<IndexHandle>,
    ) -> Result<Vec<ObjectMap>, String> {
        match condition.first() {
            Some(_) if condition.len() > 1 => Err("Only one condition is allowed".to_string()),
            Some(Condition::Equals { value, .. }) => {
                let key = value.to_string_lossy();
                if let Some(associated_data) = self.filter.get_associated_data(&key) {
                    emit!(MemoryEnrichmentTableRead {
                        key: &key,
                        include_key_metric_tag: self.config.internal_metrics.include_key_tag
                    });
                    let mut result = ObjectMap::from([
                        (
                            KeyString::from("key"),
                            Value::Bytes(Bytes::copy_from_slice(key.as_bytes())),
                        ),
                        (
                            KeyString::from("fingerprint"),
                            Value::Bytes(Bytes::from(format!(
                                "{:X}",
                                associated_data.get_fingerprint()
                            ))),
                        ),
                    ]);
                    if let Ok(ttl) = associated_data.get_stored_ttl_value()
                        && let Ok(ttl) = (ttl as u64 * self.config.scan_interval.get()).try_into()
                    {
                        result.insert(KeyString::from("ttl"), Value::Integer(ttl));
                    }
                    if let Ok(counter) = associated_data.get_counter() {
                        result.insert(KeyString::from("counter"), Value::Integer(counter.into()));
                    }
                    Ok(vec![result])
                } else {
                    emit!(MemoryEnrichmentTableReadFailed {
                        key: &key,
                        include_key_metric_tag: self.config.internal_metrics.include_key_tag
                    });
                    Ok(Default::default())
                }
            }
            Some(_) => Err("Only equality condition is allowed".to_string()),
            None => Err("Key condition must be specified".to_string()),
        }
    }

    fn add_index(&mut self, _case: Case, fields: &[&str]) -> Result<IndexHandle, String> {
        match fields.len() {
            0 => Err("Key field is required".to_string()),
            1 => Ok(IndexHandle(0)),
            _ => Err("Only one field is allowed".to_string()),
        }
    }

    /// Returns a list of the field names that are in each index
    fn index_fields(&self) -> Vec<(Case, Vec<String>)> {
        Vec::new()
    }

    /// Doesn't need reload, data is written directly
    fn needs_reload(&self) -> bool {
        false
    }
}

impl std::fmt::Debug for CuckooMemoryTable {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "CuckooMemoryTable {:?}", self.config)
    }
}

#[async_trait]
impl StreamSink<Event> for CuckooMemoryTable {
    async fn run(mut self: Box<Self>, mut input: BoxStream<'_, Event>) -> Result<(), ()> {
        let events_sent = register!(EventsSent::from(Output(None)));
        let bytes_sent = register!(BytesSent::from(Protocol("memory_enrichment_table".into(),)));
        let mut scan_interval = IntervalStream::new(interval(Duration::from_secs(
            self.config.scan_interval.into(),
        )));

        loop {
            tokio::select! {
                event = input.next() => {
                    let mut event = if let Some(event) = event {
                        event
                    } else {
                        break;
                    };
                    let event_byte_size = event.estimated_json_encoded_size_of();

                    let finalizers = event.take_finalizers();

                    // Panic: This sink only accepts Logs, so this should never panic
                    let log = event.into_log();

                    if let (Value::Object(map), _) = log.into_parts() {
                        self.handle_value(map)
                    };

                    finalizers.update_status(EventStatus::Delivered);
                    events_sent.emit(CountByteSize(1, event_byte_size));
                    bytes_sent.emit(ByteSize(event_byte_size.get()));
                },

                Some(_) = scan_interval.next() => {
                    self.filter.scan_and_update_full();
                }
            }
        }
        Ok(())
    }
}

// #[cfg(test)]
// mod tests {
//     use std::{num::NonZeroU64, slice::from_ref, time::Duration};
//
//     use futures::{StreamExt, future::ready};
//     use futures_util::stream;
//     use tokio::time;
//
//     use vector_lib::{
//         event::{EventContainer, MetricValue},
//         lookup::lookup_v2::OptionalValuePath,
//         metrics::Controller,
//         sink::VectorSink,
//     };
//
//     use super::*;
//     use crate::{
//         enrichment_tables::memory::{
//             config::MemorySourceConfig, internal_events::InternalMetricsConfig,
//         },
//         event::{Event, LogEvent},
//         test_util::components::{
//             SINK_TAGS, SOURCE_TAGS, run_and_assert_sink_compliance,
//             run_and_assert_source_compliance,
//         },
//     };
//
//     fn build_memory_config(modfn: impl Fn(&mut MemoryConfig)) -> MemoryConfig {
//         let mut config = MemoryConfig::default();
//         modfn(&mut config);
//         config
//     }
//
//     #[test]
//     fn finds_row() {
//         let memory = Memory::new(Default::default());
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(memory.config.ttl)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(Case::Sensitive, &[condition], None, None, None)
//         );
//     }
//
//     #[test]
//     fn calculates_ttl() {
//         let ttl = 100;
//         let secs_to_subtract = 10;
//         let memory = Memory::new(build_memory_config(|c| c.ttl = ttl));
//         {
//             let mut handle = memory.write_handle.lock().unwrap();
//             handle.write_handle.update(
//                 "test_key".to_string(),
//                 MemoryEntry {
//                     value: "5".to_string(),
//                     update_time: (Instant::now() - Duration::from_secs(secs_to_subtract)).into(),
//                     ttl,
//                 },
//             );
//             handle.write_handle.refresh();
//         }
//
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(ttl - secs_to_subtract)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(Case::Sensitive, &[condition], None, None, None)
//         );
//     }
//
//     #[test]
//     fn calculates_ttl_override() {
//         let global_ttl = 100;
//         let ttl_override = 10;
//         let memory = Memory::new(build_memory_config(|c| {
//             c.ttl = global_ttl;
//             c.ttl_field = OptionalValuePath::new("ttl");
//         }));
//         memory.handle_value(ObjectMap::from([
//             (
//                 "ttl_override".into(),
//                 Value::from(ObjectMap::from([
//                     ("val".into(), Value::from(5)),
//                     ("ttl".into(), Value::from(ttl_override)),
//                 ])),
//             ),
//             (
//                 "default_ttl".into(),
//                 Value::from(ObjectMap::from([("val".into(), Value::from(5))])),
//             ),
//         ]));
//
//         let default_condition = Condition::Equals {
//             field: "key",
//             value: Value::from("default_ttl"),
//         };
//         let override_condition = Condition::Equals {
//             field: "key",
//             value: Value::from("ttl_override"),
//         };
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("default_ttl")),
//                 ("ttl".into(), Value::from(global_ttl)),
//                 (
//                     "value".into(),
//                     Value::from(ObjectMap::from([("val".into(), Value::from(5))]))
//                 ),
//             ])),
//             memory.find_table_row(Case::Sensitive, &[default_condition], None, None, None)
//         );
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("ttl_override")),
//                 ("ttl".into(), Value::from(ttl_override)),
//                 (
//                     "value".into(),
//                     Value::from(ObjectMap::from([
//                         ("val".into(), Value::from(5)),
//                         ("ttl".into(), Value::from(ttl_override))
//                     ]))
//                 ),
//             ])),
//             memory.find_table_row(Case::Sensitive, &[override_condition], None, None, None)
//         );
//     }
//
//     #[test]
//     fn removes_expired_records_on_scan_interval() {
//         let ttl = 100;
//         let memory = Memory::new(build_memory_config(|c| {
//             c.ttl = ttl;
//         }));
//         {
//             let mut handle = memory.write_handle.lock().unwrap();
//             handle.write_handle.update(
//                 "test_key".to_string(),
//                 MemoryEntry {
//                     value: "5".to_string(),
//                     update_time: (Instant::now() - Duration::from_secs(ttl + 10)).into(),
//                     ttl,
//                 },
//             );
//             handle.write_handle.refresh();
//         }
//
//         // Finds the value before scan
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(0)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(Case::Sensitive, from_ref(&condition), None, None, None)
//         );
//
//         // Force scan
//         let writer = memory.write_handle.lock().unwrap();
//         memory.scan(writer);
//
//         // The value is not present anymore
//         assert!(
//             memory
//                 .find_table_rows(Case::Sensitive, &[condition], None, None, None)
//                 .unwrap()
//                 .pop()
//                 .is_none()
//         );
//     }
//
//     #[test]
//     fn does_not_show_values_before_flush_interval() {
//         let ttl = 100;
//         let memory = Memory::new(build_memory_config(|c| {
//             c.ttl = ttl;
//             c.flush_interval = Some(10);
//         }));
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert!(
//             memory
//                 .find_table_rows(Case::Sensitive, &[condition], None, None, None)
//                 .unwrap()
//                 .pop()
//                 .is_none()
//         );
//     }
//
//     #[test]
//     fn updates_ttl_on_value_replacement() {
//         let ttl = 100;
//         let memory = Memory::new(build_memory_config(|c| c.ttl = ttl));
//         {
//             let mut handle = memory.write_handle.lock().unwrap();
//             handle.write_handle.update(
//                 "test_key".to_string(),
//                 MemoryEntry {
//                     value: "5".to_string(),
//                     update_time: (Instant::now() - Duration::from_secs(ttl / 2)).into(),
//                     ttl,
//                 },
//             );
//             handle.write_handle.refresh();
//         }
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(ttl / 2)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(Case::Sensitive, from_ref(&condition), None, None, None)
//         );
//
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(ttl)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(Case::Sensitive, &[condition], None, None, None)
//         );
//     }
//
//     #[test]
//     fn ignores_all_values_over_byte_size_limit() {
//         let memory = Memory::new(build_memory_config(|c| {
//             c.max_byte_size = Some(1);
//         }));
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert!(
//             memory
//                 .find_table_rows(Case::Sensitive, &[condition], None, None, None)
//                 .unwrap()
//                 .pop()
//                 .is_none()
//         );
//     }
//
//     #[test]
//     fn ignores_values_when_byte_size_limit_is_reached() {
//         let ttl = 100;
//         let memory = Memory::new(build_memory_config(|c| {
//             c.ttl = ttl;
//             c.max_byte_size = Some(150);
//         }));
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//         memory.handle_value(ObjectMap::from([("rejected_key".into(), Value::from(5))]));
//
//         assert_eq!(
//             Ok(ObjectMap::from([
//                 ("key".into(), Value::from("test_key")),
//                 ("ttl".into(), Value::from(ttl)),
//                 ("value".into(), Value::from(5)),
//             ])),
//             memory.find_table_row(
//                 Case::Sensitive,
//                 &[Condition::Equals {
//                     field: "key",
//                     value: Value::from("test_key")
//                 }],
//                 None,
//                 None,
//                 None
//             )
//         );
//
//         assert!(
//             memory
//                 .find_table_rows(
//                     Case::Sensitive,
//                     &[Condition::Equals {
//                         field: "key",
//                         value: Value::from("rejected_key")
//                     }],
//                     None,
//                     None,
//                     None
//                 )
//                 .unwrap()
//                 .pop()
//                 .is_none()
//         );
//     }
//
//     #[test]
//     fn missing_key() {
//         let memory = Memory::new(Default::default());
//
//         let condition = Condition::Equals {
//             field: "key",
//             value: Value::from("test_key"),
//         };
//
//         assert!(
//             memory
//                 .find_table_rows(Case::Sensitive, &[condition], None, None, None)
//                 .unwrap()
//                 .pop()
//                 .is_none()
//         );
//     }
//
//     #[tokio::test]
//     async fn sink_spec_compliance() {
//         let event = Event::Log(LogEvent::from(ObjectMap::from([(
//             "test_key".into(),
//             Value::from(5),
//         )])));
//
//         let memory = Memory::new(Default::default());
//
//         run_and_assert_sink_compliance(
//             VectorSink::from_event_streamsink(memory),
//             stream::once(ready(event)),
//             &SINK_TAGS,
//         )
//         .await;
//     }
//
//     #[tokio::test]
//     async fn flush_metrics_without_interval() {
//         let event = Event::Log(LogEvent::from(ObjectMap::from([(
//             "test_key".into(),
//             Value::from(5),
//         )])));
//
//         let memory = Memory::new(Default::default());
//
//         run_and_assert_sink_compliance(
//             VectorSink::from_event_streamsink(memory),
//             stream::once(ready(event)),
//             &SINK_TAGS,
//         )
//         .await;
//
//         let metrics = Controller::get().unwrap().capture_metrics();
//         let insertions_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_insertions_total"
//             })
//             .expect("Insertions metric is missing!");
//         let MetricValue::Counter {
//             value: insertions_count,
//         } = insertions_counter.value()
//         else {
//             unreachable!();
//         };
//         let flushes_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_flushes_total"
//             })
//             .expect("Flushes metric is missing!");
//         let MetricValue::Counter {
//             value: flushes_count,
//         } = flushes_counter.value()
//         else {
//             unreachable!();
//         };
//         let object_count_gauge = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Gauge { .. })
//                     && m.name() == "memory_enrichment_table_objects_count"
//             })
//             .expect("Object count metric is missing!");
//         let MetricValue::Gauge {
//             value: object_count,
//         } = object_count_gauge.value()
//         else {
//             unreachable!();
//         };
//         let byte_size_gauge = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Gauge { .. })
//                     && m.name() == "memory_enrichment_table_byte_size"
//             })
//             .expect("Byte size metric is missing!");
//         assert_eq!(*insertions_count, 1.0);
//         assert_eq!(*flushes_count, 1.0);
//         assert_eq!(*object_count, 1.0);
//         assert!(!byte_size_gauge.is_empty());
//     }
//
//     #[tokio::test]
//     async fn flush_metrics_with_interval() {
//         let event = Event::Log(LogEvent::from(ObjectMap::from([(
//             "test_key".into(),
//             Value::from(5),
//         )])));
//
//         let memory = Memory::new(build_memory_config(|c| {
//             c.flush_interval = Some(1);
//         }));
//
//         run_and_assert_sink_compliance(
//             VectorSink::from_event_streamsink(memory),
//             stream::iter(vec![event.clone(), event]).flat_map(|e| {
//                 stream::once(async move {
//                     tokio::time::sleep(Duration::from_millis(600)).await;
//                     e
//                 })
//             }),
//             &SINK_TAGS,
//         )
//         .await;
//
//         let metrics = Controller::get().unwrap().capture_metrics();
//         let insertions_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_insertions_total"
//             })
//             .expect("Insertions metric is missing!");
//         let MetricValue::Counter {
//             value: insertions_count,
//         } = insertions_counter.value()
//         else {
//             unreachable!();
//         };
//         let flushes_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_flushes_total"
//             })
//             .expect("Flushes metric is missing!");
//         let MetricValue::Counter {
//             value: flushes_count,
//         } = flushes_counter.value()
//         else {
//             unreachable!();
//         };
//         let object_count_gauge = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Gauge { .. })
//                     && m.name() == "memory_enrichment_table_objects_count"
//             })
//             .expect("Object count metric is missing!");
//         let MetricValue::Gauge {
//             value: object_count,
//         } = object_count_gauge.value()
//         else {
//             unreachable!();
//         };
//         let byte_size_gauge = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Gauge { .. })
//                     && m.name() == "memory_enrichment_table_byte_size"
//             })
//             .expect("Byte size metric is missing!");
//
//         assert_eq!(*insertions_count, 2.0);
//         // One is done right away and the next one after the interval
//         assert_eq!(*flushes_count, 2.0);
//         assert_eq!(*object_count, 1.0);
//         assert!(!byte_size_gauge.is_empty());
//     }
//
//     #[tokio::test]
//     async fn flush_metrics_with_key() {
//         let event = Event::Log(LogEvent::from(ObjectMap::from([(
//             "test_key".into(),
//             Value::from(5),
//         )])));
//
//         let memory = Memory::new(build_memory_config(|c| {
//             c.internal_metrics = InternalMetricsConfig {
//                 include_key_tag: true,
//             };
//         }));
//
//         run_and_assert_sink_compliance(
//             VectorSink::from_event_streamsink(memory),
//             stream::once(ready(event)),
//             &SINK_TAGS,
//         )
//         .await;
//
//         let metrics = Controller::get().unwrap().capture_metrics();
//         let insertions_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_insertions_total"
//             })
//             .expect("Insertions metric is missing!");
//
//         assert!(insertions_counter.tag_matches("key", "test_key"));
//     }
//
//     #[tokio::test]
//     async fn flush_metrics_without_key() {
//         let event = Event::Log(LogEvent::from(ObjectMap::from([(
//             "test_key".into(),
//             Value::from(5),
//         )])));
//
//         let memory = Memory::new(Default::default());
//
//         run_and_assert_sink_compliance(
//             VectorSink::from_event_streamsink(memory),
//             stream::once(ready(event)),
//             &SINK_TAGS,
//         )
//         .await;
//
//         let metrics = Controller::get().unwrap().capture_metrics();
//         let insertions_counter = metrics
//             .iter()
//             .find(|m| {
//                 matches!(m.value(), MetricValue::Counter { .. })
//                     && m.name() == "memory_enrichment_table_insertions_total"
//             })
//             .expect("Insertions metric is missing!");
//
//         assert!(insertions_counter.tag_value("key").is_none());
//     }
//
//     #[tokio::test]
//     async fn source_spec_compliance() {
//         let mut memory_config = MemoryConfig::default();
//         memory_config.source_config = Some(MemorySourceConfig {
//             export_interval: Some(NonZeroU64::try_from(1).unwrap()),
//             export_batch_size: None,
//             remove_after_export: false,
//             export_expired_items: false,
//             source_key: "test".to_string(),
//         });
//         let memory = memory_config.get_or_build_memory().await;
//         memory.handle_value(ObjectMap::from([("test_key".into(), Value::from(5))]));
//
//         let mut events: Vec<Event> = run_and_assert_source_compliance(
//             memory_config,
//             time::Duration::from_secs(5),
//             &SOURCE_TAGS,
//         )
//         .await;
//
//         assert!(!events.is_empty());
//         let event = events.remove(0);
//         let log = event.as_log();
//
//         assert!(!log.value().is_empty());
//     }
// }
