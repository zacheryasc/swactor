//! StdExtension beta-surface tests.
//!
//! Tests only the std APIs used by production crates: runtime naming,
//! runtime groups, actor-side watch, actor-side group join, and extension install.

mod common;
use common::*;

#[derive(Clone)]
struct ReportExitTo {
    target: ActorAddress,
    report_to: ActorAddress,
}

impl ActorInterface for ReportExitTo {
    type Incoming = ();
    type Response = ();

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.watch(self.target);
    }

    fn handle(&mut self, _ctx: &Ctx, _msg: ()) {}

    fn on_actor_exit(&mut self, ctx: &Ctx, exited: ActorExited) {
        ctx.send(self.report_to, exited).unwrap();
    }
}

struct JoinOnStart {
    group: &'static str,
}

impl ActorInterface for JoinOnStart {
    type Incoming = Ping;
    type Response = Pong;

    fn on_start(&mut self, ctx: &Ctx) {
        ctx.join_group(self.group);
    }

    fn handle(&mut self, ctx: &Ctx, msg: Ping) {
        ctx.send(msg.reply_to, Pong).unwrap();
    }
}

#[test]
fn std_extension_installs() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let actor = rt.spawn(PingPongActor).unwrap();

    rt.send_to(
        actor,
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    tick_n(&rt, 2);

    assert_eq!(inbox.try_recv(), Some(Pong));
}

#[test]
fn runtime_naming_lifecycle() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let actor = rt.spawn(PingPongActor).unwrap();

    rt.register_name("worker", actor).unwrap();
    assert_eq!(rt.where_is("worker"), Some(actor));
    assert_eq!(rt.where_is("missing"), None);

    rt.send_to(
        rt.where_is("worker").unwrap(),
        Ping {
            reply_to: *inbox.addr(),
        },
    )
    .unwrap();
    tick_n(&rt, 2);
    assert_eq!(inbox.try_recv(), Some(Pong));

    assert!(
        rt.register_name("worker", rt.spawn(PingPongActor).unwrap())
            .is_err()
    );

    let mut names = rt.registered_names();
    names.sort();
    assert_eq!(names, vec!["worker".to_string()]);

    assert_eq!(rt.unregister("worker"), Some(actor));
    assert_eq!(rt.where_is("worker"), None);

    rt.register_name("worker", actor).unwrap();
    rt.stop_actor(actor).unwrap();
    tick_n(&rt, 3);
    assert_eq!(rt.where_is("worker"), None, "dead actors are unregistered");
}

#[test]
fn runtime_groups_lifecycle() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    let a = rt.spawn(PingPongActor).unwrap();
    let b = rt.spawn(PingPongActor).unwrap();

    rt.join_group(a, "workers");
    rt.join_group(b, "workers");

    let mut members = rt.group_members("workers");
    members.sort_by_key(|addr| addr.0);
    assert_eq!(members.len(), 2);
    assert!(members.contains(&a));
    assert!(members.contains(&b));
    assert_eq!(rt.groups(), vec!["workers".to_string()]);

    assert_eq!(
        rt.publish_to(
            "workers",
            Ping {
                reply_to: *inbox.addr(),
            },
        ),
        2
    );
    tick_n(&rt, 2);
    assert_eq!(tick_and_drain(&rt, &inbox, 0), vec![Pong, Pong]);

    rt.leave_group(a, "workers");
    assert_eq!(rt.group_members("workers"), vec![b]);

    rt.stop_actor(b).unwrap();
    tick_n(&rt, 3);
    assert!(rt.group_members("workers").is_empty());
    assert!(rt.groups().is_empty());
}

#[test]
fn ctx_watch_delivers_actor_exited() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<ActorExited>().unwrap();
    let target = rt.spawn(PanicActor).unwrap();
    rt.spawn(ReportExitTo {
        target,
        report_to: *inbox.addr(),
    })
    .unwrap();
    tick_n(&rt, 2);

    rt.send_to(target, PanicMsg).unwrap();
    tick_n(&rt, 4);

    let exited = inbox.try_recv().expect("watch notification");
    assert_eq!(exited.addr, target);
    assert_eq!(exited.reason, ExitReason::Panicked);
}

#[test]
fn ctx_join_group_receives_runtime_publish() {
    let rt = std_runtime(RuntimeConfig::default());
    let inbox = rt.new_inbox::<Pong>().unwrap();
    rt.spawn(JoinOnStart { group: "joined" }).unwrap();
    tick_n(&rt, 2);

    assert_eq!(rt.group_members("joined").len(), 1);
    assert_eq!(
        rt.publish_to(
            "joined",
            Ping {
                reply_to: *inbox.addr(),
            },
        ),
        1
    );
    tick_n(&rt, 2);

    assert_eq!(inbox.try_recv(), Some(Pong));
}
