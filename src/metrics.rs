use std::convert::TryInto;

use usiem::prelude::{
    metrics::{SiemMetric, SiemMetricDefinition},
    LogParser,
};

use usiem::prelude::counter::CounterVec;
#[cfg(feature="metrics")]
pub fn generate_parser_metrics(
    parsers: &[Box<dyn LogParser>],
) -> (Vec<SiemMetricDefinition>, ParserMetrics) {
    let mut labels = Vec::with_capacity(32);
    let empty = vec![];
    labels.push(&empty[..]);
    let mut parser_names = Vec::with_capacity(32);
    for parser in parsers {
        parser_names.push(vec![("parser", parser.name())]);
    }
    for i in 0..parsers.len() {
        labels.push(parser_names.get(i).unwrap());
    }

    let parser_unimplemented = SiemMetricDefinition::new(
        "parser_unimplemented",
        "Number of logs which the parser was not implemented for",
        SiemMetric::Counter(CounterVec::new(&labels[..])),
    )
    .unwrap();
    let parser_format_error = SiemMetricDefinition::new(
        "parser_format_error",
        "Number of logs for which the parser needs to be updated",
        SiemMetric::Counter(CounterVec::new(&labels[..])),
    )
    .unwrap();
    let parser_bug_error = SiemMetricDefinition::new(
        "parser_bug_error",
        "Number of logs for which the parser has a bug",
        SiemMetric::Counter(CounterVec::new(&labels[..])),
    )
    .unwrap();
    let parser_discarded = SiemMetricDefinition::new(
        "parser_discarded",
        "Number of logs discarded by parsers",
        SiemMetric::Counter(CounterVec::new(&labels[..])),
    )
    .unwrap();
    let metrics = ParserMetrics {
        parser_unimplemented: get_metric_counter(&parser_unimplemented),
        parser_format_error: get_metric_counter(&parser_format_error),
        parser_bug_error: get_metric_counter(&parser_bug_error),
        parser_discarded: get_metric_counter(&parser_discarded),
    };
    (
        vec![
            parser_unimplemented,
            parser_format_error,
            parser_bug_error,
            parser_discarded,
        ],
        metrics,
    )
}

#[cfg(not(feature="metrics"))]
pub fn generate_parser_metrics(
    _parsers: &[Box<dyn LogParser>],
) -> (Vec<SiemMetricDefinition>, ParserMetrics) {
    (vec![], ParserMetrics::empty())
}

fn get_metric_counter(definition: &SiemMetricDefinition) -> CounterVec {
    definition.metric().try_into().unwrap()
}

#[derive(Clone)]
pub struct ParserMetrics {
    pub parser_unimplemented: CounterVec,
    pub parser_format_error: CounterVec,
    pub parser_bug_error: CounterVec,
    pub parser_discarded: CounterVec,
}
#[cfg(not(feature="metrics"))]
impl ParserMetrics {
    pub fn empty() -> Self {
        Self {
            parser_unimplemented: CounterVec::new(&[]),
            parser_format_error: CounterVec::new(&[]),
            parser_bug_error: CounterVec::new(&[]),
            parser_discarded: CounterVec::new(&[]),
        }
    }
}