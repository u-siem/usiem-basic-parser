# µSIEM Parser
[![Documentation](https://docs.rs/usiem-basic-parser/badge.svg)](https://docs.rs/u-siem) [![crates.io](https://img.shields.io/crates/v/usiem-basic-parser.svg)](https://crates.io/crates/usiem-basic-parser)


Basic Parser component that supports multiple different sources and log formats

### Usage

```rust
// Create component and register parsers
let mut parser_component = BasicParserComponent::new();
parser_component.add_parser(Box::from(parser1));
parser_component.add_parser(Box::from(parser2));

// Send the component to the kernel to be managed
kernel.add_component(parser_component);
```

### How to build parsers

There are some examples in the [µSIEM library](https://github.com/u-siem/u-siem-core/blob/main/src/testing/parsers.rs) used for testing.

```rust
#[derive(Clone)]
pub struct DummyParserText {
    schema : FieldSchema
}
impl DummyParserText {
    pub fn new() -> Self {
        Self {
            schema : FieldSchema::new()
        }
    }
}

impl LogParser for DummyParserText {
    fn parse_log(
        &self,
        mut log: SiemLog,
        _datasets: &DatasetHolder,
    ) -> Result<SiemLog, LogParsingError> {
        if !log.message().contains("DUMMY") {
            return Err(LogParsingError::NoValidParser(log));
        }
        log.add_field("parser", SiemField::from_str("DummyParserText"));
        Ok(log)
    }
    fn name(&self) -> &'static str {
        "DummyParserText"
    }
    fn description(&self) -> &'static str {
        "This is a dummy that parsers if contains DUMMY in text"
    }
    fn schema(&self) -> & FieldSchema {
        &self.schema
    }

    fn generator(&self) -> Box<dyn LogGenerator> {
        return Box::new(DummyLogGenerator {});
    }
}

let parser1 = DummyParserText::new();
parser_component.add_parser(Box::from(parser1));

```