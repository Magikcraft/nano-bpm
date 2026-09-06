use std::collections::HashMap;

use nanobpmn_engine_core::{
    bpmn::parse_bpmn, AgentDefinition, AgentType, Command, ElementKind, Engine, EngineError, Event,
    Value,
};

const MODEL: &str = include_str!("fixtures/external-agent-job-type.bpmn");

#[test]
fn parser_preserves_agent_job_type_independently_of_extension_order() {
    for job_type in [Some("senior:rebase"), Some("= localRoute"), None] {
        let declaration = job_type
            .map(|value| format!("<zeebe:taskDefinition type=\"{value}\"/>"))
            .unwrap_or_default();
        let xml = MODEL.replace("<zeebe:taskDefinition type=\"senior:rebase\"/>", "");
        for marker in [
            format!("{declaration}<zeebe:agentDefinition agentType=\"external\"/>"),
            format!("<zeebe:agentDefinition agentType=\"external\"/>{declaration}"),
        ] {
            let xml = xml.replace("<zeebe:agentDefinition agentType=\"external\"/>", &marker);
            let def = parse_bpmn(&xml).unwrap().remove(0);
            assert!(matches!(
                &def.elements["agent"].kind,
                ElementKind::AgentTask { agent_type: AgentType::External, job_type: actual, .. }
                    if actual.as_deref() == job_type
            ));
        }
    }
}

#[test]
fn native_agent_with_task_definition_still_creates_no_job() {
    let xml = MODEL.replace("agentType=\"external\"", "agentType=\"aiAgentTask\"");
    let def = parse_bpmn(&xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "external-agent-routing",
            HashMap::from([("route".into(), Value::Str("senior:rebase".into()))]),
        ))
        .unwrap();
    assert!(!events.iter().any(|e| matches!(e, Event::JobCreated { .. })));
    assert!(events
        .iter()
        .any(|e| matches!(e, Event::AgentInstanceCreated { .. })));
}

fn assert_external_job_lifecycle(xml: &str, expected_type: &str) {
    let def = parse_bpmn(xml).unwrap().remove(0);
    let mut engine = Engine::new();
    engine.apply_command(Command::DeployProcess(def)).unwrap();
    let events = engine
        .apply_command(Command::create_instance_with(
            "external-agent-routing",
            HashMap::from([("route".into(), Value::Str("senior:rebase".into()))]),
        ))
        .unwrap();
    let (job_key, element_instance_key) = events
        .iter()
        .find_map(|event| match event {
            Event::JobCreated {
                job_key,
                element_instance_key,
                job_type,
                ..
            } => {
                assert_eq!(job_type, expected_type);
                Some((*job_key, *element_instance_key))
            }
            _ => None,
        })
        .expect("external agent creates a normal job");
    assert!(!events
        .iter()
        .any(|event| matches!(event, Event::AgentInstanceCreated { .. })));

    let create_agent = |job_lease| Command::CreateAgentInstance {
        element_instance_key,
        job_key,
        job_lease,
        definition: AgentDefinition::default(),
        limits: None,
        history: vec![],
    };
    assert!(matches!(
        engine.apply_command(create_agent(0)).unwrap_err(),
        EngineError::AgentInstanceJobNotActive { .. }
    ));
    if expected_type != "agent" {
        assert!(engine.activate_jobs("agent", "W", 1, 1_000, 0).is_empty());
    }
    let job = engine
        .activate_jobs(expected_type, "W", 1, 1_000, 0)
        .pop()
        .expect("worker subscribed by job type activates the agent");
    assert_eq!(job.key, job_key);
    let lease = job.lease_token.expect("external job is lease-gated");
    assert!(matches!(
        engine.apply_command(create_agent(lease + 1)).unwrap_err(),
        EngineError::AgentInstanceJobLeaseMismatch { .. }
    ));
    let events = engine.apply_command(create_agent(lease)).unwrap();
    assert!(events.iter().any(|event| matches!(
        event,
        Event::AgentInstanceCreated { agent_instance, .. }
            if agent_instance.agent_type == AgentType::External
                && agent_instance.job_key == job_key
                && agent_instance.job_lease == lease
    )));
    let events = engine
        .apply_command(Command::complete_job(job_key))
        .unwrap();
    assert!(events
        .iter()
        .any(|event| matches!(event, Event::ProcessInstanceCompleted { .. })));
}

#[test]
fn external_agent_uses_declared_job_type() {
    assert_external_job_lifecycle(MODEL, "senior:rebase");
}

#[test]
fn external_agent_without_task_definition_uses_element_id() {
    let xml = MODEL.replace("<zeebe:taskDefinition type=\"senior:rebase\"/>", "");
    assert_external_job_lifecycle(&xml, "agent");
}

#[test]
fn external_agent_job_type_expression_uses_input_mapping_scope() {
    let xml = MODEL.replace("type=\"senior:rebase\"", "type=\"= localRoute\"");
    assert_external_job_lifecycle(&xml, "senior:rebase");
}

#[test]
fn job_type_expressions_see_task_inputs_with_and_without_start_listeners() {
    for external in [false, true] {
        for listener in [false, true] {
            let mut xml = MODEL.replace("type=\"senior:rebase\"", "type=\"= localRoute\"");
            if !external {
                xml = xml.replace("<zeebe:agentDefinition agentType=\"external\"/>", "");
            }
            if listener {
                xml = xml.replace(
                    "<zeebe:ioMapping>",
                    r#"<zeebe:executionListeners>
                         <zeebe:executionListener eventType="start" type="before"/>
                       </zeebe:executionListeners><zeebe:ioMapping>"#,
                );
            }
            let def = parse_bpmn(&xml).unwrap().remove(0);
            let mut engine = Engine::new();
            engine.apply_command(Command::DeployProcess(def)).unwrap();
            engine
                .apply_command(Command::create_instance_with(
                    "external-agent-routing",
                    HashMap::from([
                        ("route".into(), Value::Str("senior:rebase".into())),
                        ("localRoute".into(), Value::Str("wrong-parent-route".into())),
                    ]),
                ))
                .unwrap();
            if listener {
                let job = engine
                    .activate_jobs("before", "W", 1, 1_000, 0)
                    .pop()
                    .unwrap();
                engine
                    .apply_command(Command::complete_job(job.key))
                    .unwrap();
            }
            assert_eq!(
                engine
                    .activate_jobs("senior:rebase", "W", 1, 1_000, 0)
                    .len(),
                1,
                "external={external}, start_listener={listener}"
            );
        }
    }
}
