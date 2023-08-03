use metrics::{generate_parser_metrics, ParserMetrics};
use std::borrow::Cow;
use std::collections::{BTreeMap, BTreeSet};
use usiem::components::command::{
    CommandDefinition, SiemCommandCall, SiemCommandHeader, SiemCommandResponse, SiemFunctionType,
};
use usiem::components::command_types::ParserDefinition;
use usiem::components::common::{SiemComponentCapabilities, SiemMessage, UserRole};
use usiem::components::dataset::holder::DatasetHolder;
use usiem::components::parsing::{LogParser, LogParsingError};
use usiem::components::SiemComponent;
use usiem::crossbeam_channel::TryRecvError;
use usiem::crossbeam_channel::{Receiver, Sender};
use usiem::events::SiemLog;

use usiem::prelude::storage::SiemComponentStateStorage;
use usiem::prelude::{CommandResult, SiemError, SiemMetricDefinition};
use usiem::send_message;

mod metrics;

/// Basic component parser
/// 
/// # Example
/// ```ignore
/// let mut parser_component = BasicParserComponent::new();
/// parser_component.add_parser(Box::from(parser1));
/// parser_component.add_parser(Box::from(parser2));
/// 
/// kernel.add_component(parser_component);
/// ```
#[derive(Clone)]
pub struct BasicParserComponent {
    /// Receive actions from other components or the kernel
    local_chnl_rcv: Receiver<SiemMessage>,
    /// Send actions to this components
    local_chnl_snd: Sender<SiemMessage>,
    /// Receive logs
    log_receiver: Receiver<SiemLog>,
    log_sender: Sender<SiemLog>,
    conn: Option<Box<dyn SiemComponentStateStorage>>,
    parsers: Vec<Box<dyn LogParser>>,
    datasets: DatasetHolder,
    metrics: (Vec<SiemMetricDefinition>, ParserMetrics),
}

impl BasicParserComponent {
    pub fn new() -> BasicParserComponent {
        let (local_chnl_snd, local_chnl_rcv) = usiem::crossbeam_channel::bounded(10_000);
        let (log_sender, log_receiver) = usiem::crossbeam_channel::unbounded();
        return BasicParserComponent {
            local_chnl_rcv,
            local_chnl_snd,
            log_receiver,
            log_sender,
            parsers: Vec::new(),
            conn: None,
            datasets: DatasetHolder::from_datasets(vec![]),
            metrics: generate_parser_metrics(&[]),
        };
    }
    /// Adds a new parser in the component
    pub fn add_parser(&mut self, parser: Box<dyn LogParser>) {
        self.parsers.push(parser);
        self.metrics = generate_parser_metrics(&self.parsers);
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
            SiemCommandResponse::LIST_PARSERS(CommandResult::Ok(content2)),
        )
    }

    #[cfg(feature="metrics")]
    fn update_parser_error_metric(&self, parser: &str) {
        self.metrics
            .1
            .parser_bug_error
            .with_labels(&[("parser", parser)])
            .and_then(|metric| {
                metric.inc();
                Some(metric)
            });
    }
    #[cfg(feature="metrics")]
    fn update_parser_not_implemented(&self, parser: &str) {
        self.metrics
            .1
            .parser_unimplemented
            .with_labels(&[("parser", parser)])
            .and_then(|metric| {
                metric.inc();
                Some(metric)
            });
    }
    #[cfg(feature="metrics")]
    fn update_parser_format_error(&self, parser: &str) {
        self.metrics
            .1
            .parser_format_error
            .with_labels(&[("parser", parser)])
            .and_then(|metric| {
                metric.inc();
                Some(metric)
            });
    }
    #[cfg(feature="metrics")]
    fn update_discard_metric(&self, parser: &str) {
        self.metrics
            .1
            .parser_discarded
            .with_labels(&[("parser", parser)])
            .and_then(|metric| {
                metric.inc();
                Some(metric)
            });
    }

    fn parse_log<'a>(
        &'a self,
        origin_parser_map: &mut BTreeMap<String, Vec<&'a Box<dyn LogParser>>>,
        log: SiemLog,
    ) -> Option<SiemLog> {
        let mut empty = Vec::with_capacity(128);
        let origin: String = log.origin().to_string();
        let selected_parsers = match origin_parser_map.get_mut(&origin) {
            Some(vc) => vc,
            None => &mut empty,
        };
        let mut tried_parsers = BTreeSet::new();
        let mut log = log;
        // Direct search
        for parser in &(*selected_parsers) {
            match parser.parse_log(log, &self.datasets) {
                Ok(lg) => {
                    return Some(lg);
                }
                Err(e) => match e {
                    LogParsingError::NoValidParser(lg) => {
                        log = lg;
                    }
                    LogParsingError::ParserError(lg, error) => {
                        usiem::warn!("Cannot parse log {:?}. Error={}", lg, error);
                        #[cfg(feature="metrics")]
                        self.update_parser_error_metric(parser.name());
                        return Some(lg);
                    }
                    LogParsingError::NotImplemented(lg) => {
                        #[cfg(feature="metrics")]
                        self.update_parser_not_implemented(parser.name());
                        return Some(lg);
                    }
                    LogParsingError::FormatError(lg, _) => {
                        #[cfg(feature="metrics")]
                        self.update_parser_format_error(parser.name());
                        return Some(lg);
                    }
                    LogParsingError::Discard => {
                        #[cfg(feature="metrics")]
                        self.update_discard_metric(parser.name());
                        return None;
                    }
                },
            };
            tried_parsers.insert(parser.name());
        }
        // No direct parser found, try indirect
        for parser in &self.parsers {
            if !tried_parsers.contains(parser.name()) {
                log = match parser.parse_log(log, &self.datasets) {
                    Ok(lg) => {
                        if !origin_parser_map.contains_key(&origin) {
                            origin_parser_map.insert(origin.clone(), vec![&parser]);
                        } else {
                            let _: Option<String> =
                                origin_parser_map.get_mut(&origin).and_then(|x| {
                                    x.push(&parser);
                                    None
                                });
                        }
                        return Some(lg);
                    }
                    Err(e) => match e {
                        LogParsingError::NoValidParser(lg) => lg,
                        LogParsingError::ParserError(lg, error) => {
                            #[cfg(feature="metrics")]
                            self.update_parser_error_metric(parser.name());
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            } else {
                                let _: Option<String> =
                                    origin_parser_map.get_mut(&origin).and_then(|x| {
                                        x.push(&parser);
                                        None
                                    });
                            }
                            usiem::warn!("Cannot parse log {:?}. Error={}", lg, error);
                            return Some(lg);
                        }
                        LogParsingError::NotImplemented(lg) => {
                            #[cfg(feature="metrics")]
                            self.update_parser_not_implemented(parser.name());
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            } else {
                                let _: Option<String> =
                                    origin_parser_map.get_mut(&origin).and_then(|x| {
                                        x.push(&parser);
                                        None
                                    });
                            }
                            return Some(lg);
                        }
                        LogParsingError::FormatError(lg, error) => {
                            #[cfg(feature="metrics")]
                            self.update_parser_format_error(parser.name());
                            usiem::warn!("Cannot process log. Format error: {}", error);
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            } else {
                                let _: Option<String> =
                                    origin_parser_map.get_mut(&origin).and_then(|x| {
                                        x.push(&parser);
                                        None
                                    });
                            }
                            return Some(lg);
                        }
                        LogParsingError::Discard => {
                            #[cfg(feature="metrics")]
                            self.update_discard_metric(parser.name());
                            if !origin_parser_map.contains_key(&origin) {
                                origin_parser_map.insert(origin.clone(), vec![&parser]);
                            } else {
                                let _: Option<String> =
                                    origin_parser_map.get_mut(&origin).and_then(|x| {
                                        x.push(&parser);
                                        None
                                    });
                            }
                            return None;
                        }
                    },
                };
            }
        }
        Some(log)
    }
}

impl SiemComponent for BasicParserComponent {
    fn name(&self) -> &'static str {
        "BasicParser"
    }
    fn local_channel(&self) -> Sender<SiemMessage> {
        self.local_chnl_snd.clone()
    }
    fn set_log_channel(&mut self, log_sender: Sender<SiemLog>, receiver: Receiver<SiemLog>) {
        self.log_receiver = receiver;
        self.log_sender = log_sender;
    }
    fn duplicate(&self) -> Box<dyn SiemComponent> {
        return Box::new(self.clone());
    }
    fn set_datasets(&mut self, datasets: DatasetHolder) {
        self.datasets = datasets;
    }

    /// Execute the logic of this component in an infinite loop. Must be stopped using Commands sent using the channel.
    fn run(&mut self) -> Result<(), SiemError> {
        let local_chnl_rcv = self.local_chnl_rcv.clone();
        let mut origin_parser_map: BTreeMap<String, Vec<&Box<dyn LogParser>>> = BTreeMap::new();
        loop {
            let rcv_action = local_chnl_rcv.try_recv();
            match rcv_action {
                Ok(msg) => match msg {
                    SiemMessage::Command(hdr, cmd) => match cmd {
                        SiemCommandCall::STOP_COMPONENT(_name) => return Ok(()),
                        SiemCommandCall::LIST_PARSERS(_pagination) => {
                            send_message!(self.list_parsers(&hdr)).unwrap();
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
                    TryRecvError::Disconnected => return Ok(()),
                },
            }

            let rcv_log = (&self.log_receiver).try_recv();
            match rcv_log {
                Ok(log) => {
                    let log = match self.parse_log(&mut origin_parser_map, log) {
                        Some(log) => log,
                        None => continue,
                    };
                    match self.log_sender.send(log) {
                        Ok(v) => v,
                        Err(err) => usiem::warn!("Cannot send log: {:?}", err.0),
                    };
                }
                Err(e) => match e {
                    TryRecvError::Empty => {
                        continue;
                    }
                    TryRecvError::Disconnected => return Ok(()),
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
            SiemFunctionType::LIST_PARSERS,
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
            self.metrics.0.clone(),
        )
    }
}

#[cfg(test)]
mod parser_test {
    use std::convert::TryInto;

    use super::BasicParserComponent;
    use usiem::components::command::{
        Pagination, SiemCommandCall, SiemCommandHeader, SiemCommandResponse,
    };
    use usiem::components::common::SiemMessage;
    use usiem::components::SiemComponent;
    use usiem::events::field::SiemField;
    use usiem::events::SiemLog;
    use usiem::prelude::counter::CounterVec;
    use usiem::prelude::kernel_message::KernelMessager;
    use usiem::prelude::{CommandResult, NotificationLevel};
    use usiem::testing::parsers::{DummyParserAll, DummyParserError, DummyParserText};

    #[test]
    fn test_parser() {
        let (log_to_component, log_receiver) = usiem::crossbeam_channel::unbounded();
        let (log_sender, next_log_receiver) = usiem::crossbeam_channel::unbounded();

        let parser1 = DummyParserText::new();
        let parser2 = DummyParserAll::new();

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
                SiemCommandHeader {
                    comm_id: 0,
                    comp_id: 0,
                    user: "Superuser".to_string(),
                },
                SiemCommandCall::STOP_COMPONENT("BasicParserComponent".to_string()),
            ));
        });
        parser.run().expect("Should not end with errors");
        let log = next_log_receiver.recv().expect("Log must be received");
        assert_eq!(
            log.field("parser"),
            Some(&SiemField::from_str("DummyParserText"))
        );
        let log = next_log_receiver.recv().expect("Log must be received");
        assert_eq!(
            log.field("parser"),
            Some(&SiemField::from_str("DummyParserAll"))
        );
    }

    #[test]
    fn should_list_all_parsers() {
        let (kernel_sender, kernel_receiver) = usiem::crossbeam_channel::unbounded();

        let msngr = KernelMessager::new(1, String::new(), kernel_sender);

        let parser1 = DummyParserText::new();
        let parser2 = DummyParserAll::new();

        let mut parser = BasicParserComponent::new();
        let component_channel = parser.local_channel();
        parser.add_parser(Box::from(parser1));
        parser.add_parser(Box::from(parser2));
        let parser_thd = std::thread::spawn(move || {
            usiem::logging::initialize_component_logger(msngr);
            parser.run().expect("Should end without errors");
        });
        std::thread::spawn(move || {
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {
                    comm_id: 0,
                    comp_id: 0,
                    user: "Superuser".to_string(),
                },
                SiemCommandCall::LIST_PARSERS(Pagination {
                    offset: 0,
                    limit: 1000,
                }),
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
            // STOP parser component to finish testing
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {
                    comm_id: 0,
                    comp_id: 0,
                    user: "Superuser".to_string(),
                },
                SiemCommandCall::STOP_COMPONENT("BasicParserComponent".to_string()),
            ));
        });
        parser_thd.join().unwrap();

        let response = kernel_receiver.recv();
        let msg: SiemMessage = response.unwrap();
        if let SiemMessage::Response(_, SiemCommandResponse::LIST_PARSERS(CommandResult::Ok(res))) =
            msg
        {
            assert_eq!(2, res.len());
            assert_eq!("DummyParserText", res.get(0).unwrap().name);
            assert_eq!("DummyParserAll", res.get(1).unwrap().name);
        } else {
            panic!("Must not be error")
        }
    }

    #[test]
    #[cfg(feature="metrics")]
    fn should_update_metrics() {
        let (kernel_sender, kernel_receiver) = usiem::crossbeam_channel::unbounded();

        let msngr = KernelMessager::new(1, String::new(), kernel_sender);
        let parser1 = DummyParserText::new();
        let parser2 = DummyParserError::new();

        let mut parser = BasicParserComponent::new();

        let component_channel = parser.local_channel();
        parser.add_parser(Box::from(parser2));
        parser.add_parser(Box::from(parser1));

        let capa = parser.capabilities();
        let mut metrics = std::collections::BTreeMap::new();
        capa.metrics().iter().for_each(|v| {
            metrics.insert(v.name().to_string(), v.metric().clone());
        });

        let parser_unimplemented: CounterVec = metrics
            .get("parser_unimplemented")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(0, parser_unimplemented.with_labels(&[]).unwrap().get());
        let parser_format_error: CounterVec = metrics
            .get("parser_format_error")
            .unwrap()
            .try_into()
            .unwrap();
        assert_eq!(0, parser_format_error.with_labels(&[]).unwrap().get());
        let parser_bug_error: CounterVec =
            metrics.get("parser_bug_error").unwrap().try_into().unwrap();
        assert_eq!(0, parser_bug_error.with_labels(&[]).unwrap().get());
        assert_eq!(
            0,
            parser_bug_error
                .with_labels(&[("parser", "DummyParserError")])
                .unwrap()
                .get()
        );
        assert_eq!(
            0,
            parser_bug_error
                .with_labels(&[("parser", "DummyParserText")])
                .unwrap()
                .get()
        );

        let parser_thd = std::thread::spawn(move || {
            usiem::logging::set_max_level(NotificationLevel::Debug);
            usiem::logging::initialize_component_logger(msngr);
            parser.run().expect("Should end without errors");
        });

        std::thread::spawn(move || loop {
            let msg = match kernel_receiver.recv() {
                Ok(v) => v,
                Err(_) => return,
            };
            if let SiemMessage::Notification(msg) = msg {
                println!("{}", msg.log);
            }
        });

        let log1 = SiemLog::new("This is a DUMMY log for DummyParserText", 0, "localhost1");
        let _r = component_channel.send(SiemMessage::Log(log1));

        std::thread::spawn(move || {
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {
                    comm_id: 0,
                    comp_id: 0,
                    user: "Superuser".to_string(),
                },
                SiemCommandCall::LIST_PARSERS(Pagination {
                    offset: 0,
                    limit: 1000,
                }),
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
            // STOP parser component to finish testing
            let _r = component_channel.send(SiemMessage::Command(
                SiemCommandHeader {
                    comm_id: 0,
                    comp_id: 0,
                    user: "Superuser".to_string(),
                },
                SiemCommandCall::STOP_COMPONENT("BasicParserComponent".to_string()),
            ));
            std::thread::sleep(std::time::Duration::from_millis(200));
        });

        parser_thd.join().unwrap();
        println!("{:?}", parser_bug_error);
        assert_eq!(
            1,
            parser_bug_error
                .with_labels(&[("parser", "DummyParserError")])
                .unwrap()
                .get()
        );
        assert_eq!(
            0,
            parser_bug_error
                .with_labels(&[("parser", "DummyParserText")])
                .unwrap()
                .get()
        );
    }
}
