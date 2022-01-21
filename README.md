# usiem-basic-parser
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

```rust
use usiem::components::common::{LogParser, LogParsingError};
use usiem::events::SiemLog;
use usiem::components::SiemComponent;

struct DummyParserTextDUMMY {}

impl LogParser for DummyParserTextDUMMY {
    fn parse_log(&self, mut log: SiemLog) -> Result<SiemLog, LogParsingError> {
        log.add_field("parser", SiemField::from_str("DummyParserTextDUMMY"));
        Ok(log)
    }
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("DummyParserTextDUMMY")
    }
}

let parser1 = DummyParserTextDUMMY{};
parser_component.add_parser(Box::from(parser1));

```