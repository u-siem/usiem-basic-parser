use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use usiem::components::command::{
    CommandDefinition, SiemCommandCall, SiemCommandHeader, SiemCommandResponse, SiemFunctionType,
};
use usiem::components::command_types::ParserDefinition;
use usiem::components::parsing::{LogParser, LogParsingError};
use usiem::components::common::{
    SiemComponentCapabilities, SiemComponentStateStorage, SiemMessage, UserRole,
};
use usiem::components::dataset::SiemDataset;
use usiem::components::SiemComponent;
use usiem::crossbeam_channel::TryRecvError;
use usiem::crossbeam_channel::{Receiver, Sender};
use usiem::events::SiemLog;

#[derive(Clone)]
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
    id: u64,
}

impl BasicParserComponent {
    pub fn new() -> BasicParserComponent {
        let (kernel_sender, _receiver) = usiem::crossbeam_channel::bounded(1000);
        let (local_chnl_snd, local_chnl_rcv) = usiem::crossbeam_channel::unbounded();
        let (log_sender, log_receiver) = usiem::crossbeam_channel::unbounded();
        return BasicParserComponent {
            kernel_sender,
            local_chnl_rcv,
            local_chnl_snd,
            log_receiver,
            log_sender,
            parsers: Vec::new(),
            conn: None,
            id: 0,
        };
    }
    pub fn add_parser(&mut self, parser: Box<dyn LogParser>) {
        self.parsers.push(parser);
    }

    fn list_parsers(&self, header: &SiemCommandHeader) -> SiemMessage {
        let content2 = self
            .parsers
            .iter()
            .map(|x| ParserDefinition {
                name: x.name().to_string(),
                description: x.description().to_string(),
            })
            .collect::<Vec<ParserDefinition>>();
        SiemMessage::Response(
            SiemCommandHeader {
                comm_id: header.comm_id,
                comp_id: header.comp_id,
                user: header.user.clone(),
            },
            SiemCommandResponse::LIST_PARSERS(Ok(content2)),
        )
    }

    fn parse_log<'a>(
        &'a self,
        origin_parser_map: &mut BTreeMap<String, Vec<&'a Box<dyn LogParser>>>,
        log: SiemLog,
    ) {
        let mut empty = vec![];
        let origin : String = log.origin().to_string();
        let selected_parsers = match origin_parser_map.get_mut(&origin) {
            Some(vc) => vc,
            None => &mut empty,
        };
        let mut tried_parsers = BTreeSet::new();
        let mut log = log;
        // Direct search
        for parser in &(*selected_parsers) {
            log = match parser.parse_log(log) {
                Ok(lg) => {
                    let _ = self.log_sender.send(lg);
                    return;
                }
                Err(e) => match e {
                    LogParsingError::NoValidParser(lg) => lg,
                    LogParsingError::ParserError(lg, error) => {
                        let r = self
                            .kernel_sender
                            .send(SiemMessage::Notification(self.id, Cow::Owned(error)));
                        match r {
                            Err(_e) => {}
                            _ => {}
                        };
                        let _ = self.log_sender.send(lg);
                        return;
                    }
                    LogParsingError::NotImplemented(lg) => {
                        let _ = self.log_sender.send(lg);
                        return;
                    }
                    LogParsingError::FormatError(lg, _) => {
                        let _ = self.log_sender.send(lg);
                        return;
                    }
                },
            };
            tried_parsers.insert(parser.name());
        }
        // No direct parser found, try indirect
        for parser in &self.parsers {
            if !tried_parsers.contains(parser.name()) {
                log = match parser.parse_log(log) {
                    Ok(lg) => {

                        if !origin_parser_map.contains_key(&origin) {
                            origin_parser_map.insert(origin.clone(), vec![&parser]);
                        }else{
                            let _ : Option<String> = origin_parser_map.get_mut(&origin).and_then(|x| {x.push(&parser); None});
                        }
                        let _ = self.log_sender.send(lg);
                        return;
                    }
                    Err(e) => match e {
                        LogParsingError::NoValidParser(lg) => lg,
                        LogParsingError::ParserError(lg, error) => {
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            }else{
                                let _ : Option<String> = origin_parser_map.get_mut(&origin).and_then(|x| {x.push(&parser); None});
                            }
                            let r = self
                                .kernel_sender
                                .send(SiemMessage::Notification(self.id, Cow::Owned(error)));
                            match r {
                                Err(_e) => {}
                                _ => {}
                            };
                            let _ = self.log_sender.send(lg);
                            return;
                        }
                        LogParsingError::NotImplemented(lg) => {
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            }else{
                                let _ : Option<String> = origin_parser_map.get_mut(&origin).and_then(|x| {x.push(&parser); None});
                            }
                            let _ = self.log_sender.send(lg);
                            return;
                        }
                        LogParsingError::FormatError(lg, _error) => {
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            }else{
                                let _ : Option<String> = origin_parser_map.get_mut(&origin).and_then(|x| {x.push(&parser); None});
                            }
                            let _ = self.log_sender.send(lg);
                            return;
                        }
                    },
                };
            }
        }
        let _ = self.log_sender.send(log);
    }
}

impl SiemComponent for BasicParserComponent {
    fn id(&self) -> u64 {
        return self.id;
    }
    fn set_id(&mut self, id: u64) {
        self.id = id;
    }
    fn name(&self) -> &str {
        "BasicParserExecutor"
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
    fn duplicate(&self) -> Box<dyn SiemComponent> {
        return Box::new(self.clone());
    }
    fn set_datasets(&mut self, _datasets: Vec<SiemDataset>) {}

    /// Execute the logic of this component in an infinite loop. Must be stopped using Commands sent using the channel.
    fn run(&mut self) {
        let kernel_channel = self.kernel_sender.clone();
        let local_chnl_rcv = self.local_chnl_rcv.clone();
        let mut origin_parser_map: BTreeMap<String, Vec<&Box<dyn LogParser>>> = BTreeMap::new();
        loop {
            let rcv_action = local_chnl_rcv.try_recv();
            match rcv_action {
                Ok(msg) => match msg {
                    SiemMessage::Command(hdr, cmd) => match cmd {
                        SiemCommandCall::STOP_COMPONENT(_name) => return,
                        SiemCommandCall::LIST_PARSERS(_pagination) => {
                            let _ = kernel_channel.send(self.list_parsers(&hdr));
                        }
                        _ => {}
                    },
                    SiemMessage::Log(msg) => {
                        self.parse_log(&mut origin_parser_map, msg);
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
                    self.parse_log(&mut origin_parser_map, log);
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

        let list_parsers = CommandDefinition::new(
            SiemFunctionType::OTHER(
                Cow::Borrowed("LIST_PARSERS"),
                std::collections::BTreeMap::new(),
            ), // Must be added by default by the KERNEL and only used by him
            Cow::Borrowed("List log parsers"),
            Cow::Borrowed("List all parsers in this component."),
            UserRole::Administrator,
        );
        commands.push(list_parsers);

        SiemComponentCapabilities::new(
            Cow::Borrowed("BasicParser"),
            Cow::Borrowed("Parse logs using multiple diferent parsers"),
            Cow::Borrowed(""), // No HTML
            datasets,
            commands,
            vec![],
            vec![],
        )
    }
}

#[cfg(test)]
mod parser_test {
    use super::BasicParserComponent;
    use usiem::components::command::{SiemCommandHeader, SiemCommandCall, SiemCommandResponse, Pagination};
    use usiem::components::common::{ SiemMessage};
    use usiem::components::parsing::{LogGenerator, LogParser, LogParsingError};
    use usiem::components::SiemComponent;
    use usiem::events::field::{SiemField};
    use usiem::events::schema::FieldSchema;
    use usiem::events::SiemLog;

    use lazy_static::lazy_static;

    lazy_static! {
        static ref BASIC_SCHEMA: FieldSchema = FieldSchema::new();
    }

    struct DummyLogGenerator {}

    impl LogGenerator for DummyLogGenerator {
        fn log(&self) -> String {
            "This is a dummy log".to_string()
        }

        fn weight(&self) -> u8 {
            1
        }
    }

    #[derive(Clone)]
    struct DummyParserTextDUMMY {}

    impl LogParser for DummyParserTextDUMMY {
        fn parse_log(&self, mut log: SiemLog) -> Result<SiemLog, LogParsingError> {
            if !log.message().contains("DUMMY") {
                return Err(LogParsingError::NoValidParser(log))
            }
            log.add_field("parser", SiemField::from_str("DummyParserTextDUMMY"));
            Ok(log)
        }
        fn name(&self) -> &str {
            "DummyParserTextDUMMY"
        }
        fn description(&self) -> &str {
            "This is a dummy that parsers if contains DUMMY in text"
        }
        fn schema(&self) -> &'static FieldSchema {
            return &BASIC_SCHEMA;
        }

        fn generator(&self) -> Box<dyn LogGenerator> {
            return Box::new(DummyLogGenerator {});
        }
    }
    #[derive(Clone)]
    struct DummyParserALL {}

    impl LogParser for DummyParserALL {
        fn parse_log(&self, mut log: SiemLog) -> Result<SiemLog, LogParsingError> {
            log.add_field("parser", SiemField::from_str("DummyParserALL"));
            Ok(log)
        }
        fn name(&self) -> &str {
            "DummyParserALL"
        }
        fn description(&self) -> &str {
            "This is a dummy parser that always parses logs"
        }
        fn schema(&self) -> &'static FieldSchema {
            return &BASIC_SCHEMA;
        }

        fn generator(&self) -> Box<dyn LogGenerator> {
            return Box::new(DummyLogGenerator {});
        }
    }

    #[test]
    fn test_parser() {
        let (log_to_component, log_receiver) = usiem::crossbeam_channel::unbounded();
        let (log_sender, next_log_receiver) = usiem::crossbeam_channel::unbounded();

        let parser1 = DummyParserTextDUMMY {};
        let parser2 = DummyParserALL {};

        let mut parser = BasicParserComponent::new();
        let component_channel = parser.local_channel();
        parser.add_parser(Box::from(parser1));
        parser.add_parser(Box::from(parser2));
        parser.set_log_channel(log_sender, log_receiver);

        let log1 = SiemLog::new(
            "This is a DUMY log for DummyParserTextDUMMY",
            0,
            "localhost1",
        );
        let log2 = SiemLog::new(
            "This is NOT a DUmmY log for DummyParserTextDUmmY",
            0,
            "localhost2",
        );
        let _r = log_to_component.send(log1);
        let _r = log_to_component.send(log2);

        std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(200));
            // STOP parser component to finish testing
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {comm_id : 0, comp_id : 0, user : "Superuser".to_string()},
                SiemCommandCall::STOP_COMPONENT("BasicParserComponent".to_string()),
            ));
        });
        parser.run();

        if let Ok(log) =  next_log_receiver.recv() {
            assert_eq!(
                log.field("parser"),
                Some(&SiemField::from_str("DummyParserTextDUMMY"))
            );
        }else{
            panic!("Must be received")
        }
        if let Ok(log) =  next_log_receiver.recv() {
            assert_eq!(
                log.field("parser"),
                Some(&SiemField::from_str("DummyParserALL"))
            );
        }else{
            panic!("Must be received")
        }
    }

    #[test]
    fn test_list_parsers() {
        let (kernel_sender, kernel_receiver) = usiem::crossbeam_channel::unbounded();

        let parser1 = DummyParserTextDUMMY {};
        let parser2 = DummyParserALL {};

        let mut parser = BasicParserComponent::new();
        let component_channel = parser.local_channel();
        parser.add_parser(Box::from(parser1));
        parser.add_parser(Box::from(parser2));
        parser.set_kernel_sender(kernel_sender);

        std::thread::spawn(move || {
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {comm_id : 0, comp_id : 0, user : "Superuser".to_string()},
                SiemCommandCall::LIST_PARSERS(Pagination {
                    offset : 0,
                    limit : 1000
                })
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
            // STOP parser component to finish testing
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {comm_id : 0, comp_id : 0, user : "Superuser".to_string()},
                SiemCommandCall::STOP_COMPONENT("BasicParserComponent".to_string()),
            ));
        });
        parser.run();

        let response = kernel_receiver.recv();
        if let Ok(SiemMessage::Response(_,SiemCommandResponse::LIST_PARSERS(Ok(res)))) = response {
            assert_eq!(2, res.len());
            assert_eq!("DummyParserTextDUMMY", res.get(0).unwrap().name);
            assert_eq!("DummyParserALL", res.get(1).unwrap().name);
        }else{
            panic!("Must not be error")
        }
    }
}
