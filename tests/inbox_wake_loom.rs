//! Loom model for the external-inbox pop/register/pop wake protocol.
//!
//! The two mutexes model the linearization points supplied by the lock-free
//! queue and `AtomicWaker`; they are not the production implementation.

use loom::sync::atomic::{AtomicBool, Ordering};
use loom::sync::{Arc, Mutex};
use loom::thread;

#[test]
fn send_cannot_be_lost_while_receiver_registers() {
    loom::model(|| {
        let queued = Arc::new(Mutex::new(false));
        let registered = Arc::new(Mutex::new(false));
        let ready = Arc::new(AtomicBool::new(false));
        let woken = Arc::new(AtomicBool::new(false));

        let sender_queued = queued.clone();
        let sender_registered = registered.clone();
        let sender_woken = woken.clone();
        let sender = thread::spawn(move || {
            *sender_queued.lock().unwrap() = true;
            if *sender_registered.lock().unwrap() {
                sender_woken.store(true, Ordering::Release);
            }
        });

        let receiver_queued = queued.clone();
        let receiver_registered = registered.clone();
        let receiver_ready = ready.clone();
        let receiver = thread::spawn(move || {
            {
                let mut queued = receiver_queued.lock().unwrap();
                if *queued {
                    *queued = false;
                    receiver_ready.store(true, Ordering::Release);
                    return;
                }
            }

            *receiver_registered.lock().unwrap() = true;

            let mut queued = receiver_queued.lock().unwrap();
            if *queued {
                *queued = false;
                receiver_ready.store(true, Ordering::Release);
            }
        });

        sender.join().unwrap();
        receiver.join().unwrap();

        let ready = ready.load(Ordering::Acquire);
        let woken = woken.load(Ordering::Acquire);
        let queued = *queued.lock().unwrap();
        let registered = *registered.lock().unwrap();
        assert!(
            ready || woken,
            "ready={ready}, woken={woken}, queued={queued}, registered={registered}"
        );
    });
}
