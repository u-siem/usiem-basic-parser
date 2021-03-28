use crossbeam_channel::TryRecvError;
use crossbeam_channel::{Receiver, Sender};
use std::borrow::Cow;
use usiem::components::common::{
    CommandDefinition, SiemComponentCapabilities, SiemComponentStateStorage, SiemFunctionCall,
    SiemFunctionType, SiemMessage, UserRole,
};
use usiem::components::common::{LogParser, LogParsingError};
use usiem::components::SiemComponent;
use usiem::events::SiemLog;

pub struct BasicParserComponent {
    /// Send actions to the kernel
    kernel_sender: Sender<SiemMessage>,
    /// Receive actions from other components or the kernel
    local_chnl_rcv: Receiver<SiemMessage>,
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    /// Receive logs
    log_receiver: Receiver<SiemLog>,
    log_sender: Sender<SiemLog>,
    conn: Option<Box<dyn SiemComponentStateStorage>>,
    parsers: Vec<Box<dyn LogParser>>,
    cache: Vec<SiemLog>,
    kernel_errors : usize,
    send_errors : usize,
}

impl BasicParserComponent {
    pub fn new() -> BasicParserComponent {
        let (kernel_sender, _receiver) = crossbeam_channel::bounded(1000);
        let (local_chnl_snd, local_chnl_rcv) = crossbeam_channel::unbounded();
        let (log_sender, log_receiver) = crossbeam_channel::unbounded();
        return BasicParserComponent {
            kernel_sender,
            local_chnl_rcv,
            local_chnl_snd,
            log_receiver,
            log_sender,
            parsers: Vec::new(),
            conn: None,
            cache : Vec::with_capacity(128),
            kernel_errors : 0,
            send_errors : 0
        };
    }
    pub fn add_parser(&mut self, parser: Box<dyn LogParser>) {
        self.parsers.push(parser);
    }
    fn parse_log(&mut self, log: SiemLog) {
        let mut selected_parser = None;
        for parser in &self.parsers {
            if parser.device_match(&log) {
                selected_parser = Some(parser);
                break;
            }
        }
        let res = match selected_parser {
            Some(parser) => match parser.parse_log(log) {
                Ok(lg) => {
                    self.log_sender.send(lg)
                }
                Err(e) => match e {
                    LogParsingError::NoValidParser(lg) => {
                        self.log_sender.send(lg)
                    }
                    LogParsingError::ParserError(lg) => {
                        let r = self.kernel_sender
                            .send(SiemMessage::Notification(Cow::Borrowed(
                                "Error parsing log...",
                        )));
                        match r {
                            Err(_e) => {
                                self.kernel_errors += 1;
                            },
                            _ => {}
                        }
                        self.log_sender.send(lg)
                    }
                }
            },
            None => {
                self.log_sender.send(log)
            }
        };
        match res {
            Err(e) => {
                let log = e.into_inner();
                self.cache.push(log);
                self.send_errors += 1;
            },
            _ => {}
        }
        
    }
}

impl SiemComponent for BasicParserComponent {
    fn name(&self) -> Cow<'static, str> {
        Cow::Borrowed("BasicParser")
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn set_log_channel(&mut self, log_sender: Sender<SiemLog>, receiver: Receiver<SiemLog>) {
        self.log_receiver = receiver;
        self.log_sender = log_sender;
    }
    fn set_kernel_sender(&mut self, sender: Sender<SiemMessage>) {
        self.kernel_sender = sender;
    }

    /// Execute the logic of this component in an infinite loop. Must be stopped using Commands sent using the channel.
    fn run(&mut self) {
        loop {
            let rcv_action = (&self.local_chnl_rcv).try_recv();
            match rcv_action {
                Ok(msg) => match msg {
                    SiemMessage::Command(cmd) => match cmd {
                        SiemFunctionCall::STOP_COMPONENT(_n) => return,
                        _ => {}
                    },
                    SiemMessage::Log(msg) => {
                        self.parse_log(msg);
                    }
                    _ => {}
                },
                Err(e) => match e {
                    TryRecvError::Empty => {
                        std::thread::sleep(std::time::Duration::from_millis(10));
                    }
                    TryRecvError::Disconnected => return,
                },
            }

            let rcv_log = (&self.log_receiver).try_recv();
            match rcv_log {
                Ok(log) => {
                    self.parse_log(log);
                }
                Err(e) => match e {
                    TryRecvError::Empty => {
                        continue;
                    }
                    TryRecvError::Disconnected => return,
                },
            }
        }
    }

    /// Allow to store information about this component like the state or conigurations.
    fn set_storage(&mut self, conn: Box<dyn SiemComponentStateStorage>) {
        self.conn = Some(conn);
    }

    /// Capabilities and actions that can be performed on this component
    fn capabilities(&self) -> SiemComponentCapabilities {
        let datasets = Vec::new();
        let mut commands = Vec::new();

        let stop_component = CommandDefinition::new(SiemFunctionType::STOP_COMPONENT,Cow::Borrowed("Stop BasicParser") ,Cow::Borrowed("This allows stopping all Basic Parser components.\nUse only when really needed, like when there is a bug in the parsing process.") , UserRole::Administrator);
        commands.push(stop_component);
        let start_component = CommandDefinition::new(
            SiemFunctionType::START_COMPONENT, // Must be added by default by the KERNEL and only used by him
            Cow::Borrowed("Start Basic Parser"),
            Cow::Borrowed("This allows processing logs."),
            UserRole::Administrator,
        );
        commands.push(start_component);
        SiemComponentCapabilities::new(
            Cow::Borrowed("BasicParser"),
            Cow::Borrowed("Parse logs using multiple diferent parsers"),
            Cow::Borrowed(""), // No HTML
            datasets,
            commands,
        )
    }
}

#[cfg(test)]
mod parser_test {
    use usiem::components::common::{LogParser, LogParsingError, SiemFunctionCall, SiemMessage};
    use usiem::events::SiemLog;
    use usiem::events::field::{SiemField,SiemIp};
    use std::borrow::Cow;
    use super::{BasicParserComponent};
    use usiem::components::SiemComponent;


    struct DummyParserTextDUMMY {}

    impl LogParser for DummyParserTextDUMMY {
        fn parse_log(&self, mut log: SiemLog) -> Result<SiemLog, LogParsingError> {
            log.add_field("parser", SiemField::from_str("DummyParserTextDUMMY"));
            Ok(log)
        }
        fn device_match(&self, log: &SiemLog) -> bool {
            log.message().contains("DUMMY")
        }
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("This is a dummy that parsers if contains DUMMY in text")
        }
    }

    struct DummyParserALL {}

    impl LogParser for DummyParserALL {
        fn parse_log(&self, mut log: SiemLog) -> Result<SiemLog, LogParsingError> {
            log.add_field("parser", SiemField::from_str("DummyParserALL"));
            Ok(log)
        }
        fn device_match(&self, _log: &SiemLog) -> bool {
            true
        }
        fn name(&self) -> Cow<'static, str> {
            Cow::Borrowed("This is a dummy parser that always parses logs")
        }
    }

    #[test]
    fn test_parser() {
        let (log_to_component, log_receiver) = crossbeam_channel::unbounded();
        let (log_sender, next_log_receiver) = crossbeam_channel::unbounded();

        let parser1 = DummyParserTextDUMMY{};
        let parser2 = DummyParserALL{};

        let mut parser = BasicParserComponent::new();
        let component_channel = parser.local_channel();
        parser.add_parser(Box::from(parser1));
        parser.add_parser(Box::from(parser2));
        parser.set_log_channel(log_sender, log_receiver);

        let log1 = SiemLog::new(String::from("This is a DUMMY log for DummyParserTextDUMMY"), 0, SiemIp::V4(0));
        let log2 = SiemLog::new(String::from("This is a text log for DummyParserALL"), 0, SiemIp::V4(1));
        let _r = log_to_component.send(log1);
        let _r = log_to_component.send(log2);

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            // STOP parser component to finish testing
            let _r = component_channel.send(SiemMessage::Command(SiemFunctionCall::STOP_COMPONENT(Cow::Borrowed("BasicParserComponent"))));
        });
        parser.run();
        
        let log1=next_log_receiver.recv();
        match log1 {
            Ok(log) => {
                assert_eq!(log.field("parser"), Some(&SiemField::from_str("DummyParserTextDUMMY")));
            },
            _ => {panic!("Must be received")}
        }
        let log2 = next_log_receiver.recv();
        match log2 {
            Ok(log) => {
                assert_eq!(log.field("parser"), Some(&SiemField::from_str("DummyParserALL")));
            },
            _ => {panic!("Must be received")}
        }
    }
}
