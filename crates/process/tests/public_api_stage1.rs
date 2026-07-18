use std::collections::HashMap;
use std::time::Duration;

use swactor_process::{
    ExitStatus, ProcessCommand, ProcessOutput, ProcessSpec, send_process_command,
};

#[test]
fn stage1_public_api_exposes_only_target_process_shapes() {
    let spec = ProcessSpec {
        command: "echo".to_string(),
        args: vec!["ok".to_string()],
        env: HashMap::new(),
        working_dir: None,
        label: Some("echo_ok".to_string()),
    };
    assert_eq!(spec.label.as_deref(), Some("echo_ok"));

    let stop = ProcessCommand::Stop {
        kill_after: Some(Duration::from_millis(10)),
    };
    assert!(matches!(
        stop,
        ProcessCommand::Stop {
            kill_after: Some(_)
        }
    ));

    let outputs = [
        ProcessOutput::Started { pid: 1 },
        ProcessOutput::SpawnFailed {
            error: "spawn failed".to_string(),
        },
        ProcessOutput::Exited {
            status: ExitStatus::Code(0),
        },
        ProcessOutput::Error {
            error: "supervisor failed".to_string(),
        },
    ];
    assert!(matches!(outputs[0], ProcessOutput::Started { pid: 1 }));
    assert!(matches!(
        outputs[2],
        ProcessOutput::Exited {
            status: ExitStatus::Code(0)
        }
    ));

    let _send: fn(
        &swactor::runtime::ExternalSender,
        swactor::actor::ActorAddress,
        ProcessCommand,
    ) -> Result<(), swactor::Error> = send_process_command;
}
