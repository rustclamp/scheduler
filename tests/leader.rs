//! Tests for leader-only scheduling across instances sharing one lock.

use std::future::Future;
use std::pin::pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, UNIX_EPOCH};

use rustclamp_core::{ContributionTarget, ManualClock, ModuleId};
use rustclamp_scheduler::{
    JobDeclaration, LeaderFuture, LeaderLock, MisfirePolicy, RunNow, Scheduler, SchedulerTarget,
};

const OWNER: ModuleId = ModuleId::new("test.module.leader");
const INTERVAL: Duration = Duration::from_secs(10);

// ponytail: jobs and locks here complete on first poll, so a noop-waker loop is a full executor.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}

/// One lock shared by several instances: the first to ask holds it until released.
#[derive(Default)]
struct Shared {
    holder: Mutex<Option<usize>>,
    asked: AtomicUsize,
}

struct Lock(Arc<Shared>, usize);

impl LeaderLock for Lock {
    fn hold(&self) -> LeaderFuture<'_> {
        self.0.asked.fetch_add(1, Ordering::SeqCst);
        let mut holder = self.0.holder.lock().unwrap();
        let leader = *holder.get_or_insert(self.1) == self.1;
        Box::pin(async move { leader })
    }
}

fn instance(shared: &Arc<Shared>, id: usize, runs: &Arc<AtomicUsize>) -> Scheduler {
    let runs = Arc::clone(runs);
    SchedulerTarget
        .build(&[(
            OWNER,
            JobDeclaration::new("job", INTERVAL, MisfirePolicy::Skip, move || {
                runs.fetch_add(1, Ordering::SeqCst);
                async { Ok(()) }
            }),
        )])
        .unwrap()
        .leader_only(Lock(Arc::clone(shared), id))
}

#[test]
fn only_the_leader_runs_and_a_standby_takes_over_on_its_next_due_tick() {
    let shared = Arc::new(Shared::default());
    let runs = Arc::new(AtomicUsize::new(0));
    let a = instance(&shared, 1, &runs);
    let b = instance(&shared, 2, &runs);
    let clock = ManualClock::new(UNIX_EPOCH);

    let first = block_on(a.tick(&clock));
    let second = block_on(b.tick(&clock));
    assert_eq!((first.invoked, first.not_leader), (1, 0));
    assert_eq!((second.invoked, second.not_leader), (0, 1));
    assert_eq!(runs.load(Ordering::SeqCst), 1);

    // Not due: the lock is not asked.
    let asked = shared.asked.load(Ordering::SeqCst);
    block_on(b.tick(&clock));
    assert_eq!(shared.asked.load(Ordering::SeqCst), asked);

    // The leader loses its lock; the standby runs the next slot, not a backlog.
    *shared.holder.lock().unwrap() = None;
    clock.advance(INTERVAL);
    assert_eq!(block_on(b.tick(&clock)).invoked, 1);
    assert_eq!(block_on(a.tick(&clock)).not_leader, 1);
    assert_eq!(runs.load(Ordering::SeqCst), 2);

    // run_now is an admin action on this instance and ignores the lock.
    assert!(matches!(block_on(a.run_now("job")), RunNow::Ran(Ok(()))));
}
