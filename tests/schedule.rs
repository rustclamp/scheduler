//! Tests for the schedule grid, run-now and surfaced job errors.

use std::future::Future;
use std::pin::pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::task::{Context, Poll, Waker};
use std::time::{Duration, UNIX_EPOCH};

use rustclamp_core::{ContributionTarget, ManualClock, ModuleId};
use rustclamp_scheduler::{JobDeclaration, JobError, MisfirePolicy, RunNow, SchedulerTarget};

const OWNER: ModuleId = ModuleId::new("test.module.jobs");
const TEN: Duration = Duration::from_secs(10);

// ponytail: jobs here complete on first poll, so a noop-waker loop is a full executor.
fn block_on<F: Future>(future: F) -> F::Output {
    let mut future = pin!(future);
    let mut context = Context::from_waker(Waker::noop());
    loop {
        if let Poll::Ready(output) = future.as_mut().poll(&mut context) {
            return output;
        }
    }
}

fn counting(misfire: MisfirePolicy, runs: &Arc<AtomicUsize>) -> JobDeclaration {
    let runs = Arc::clone(runs);
    JobDeclaration::new("count", TEN, misfire, move || {
        runs.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    })
}

fn at(clock: &ManualClock, millis: u64) {
    clock.set(UNIX_EPOCH + Duration::from_millis(millis));
}

#[test]
fn late_ticks_do_not_push_the_schedule_back() {
    let runs = Arc::new(AtomicUsize::new(0));
    let scheduler = SchedulerTarget
        .build(&[(OWNER, counting(MisfirePolicy::RunOnce, &runs))])
        .unwrap();
    let clock = ManualClock::new(UNIX_EPOCH);
    let invoked = |millis| {
        at(&clock, millis);
        block_on(scheduler.tick(&clock)).invoked
    };
    assert_eq!(invoked(0), 1);
    assert_eq!(invoked(10_500), 1, "due at 10 s, ticked late");
    assert_eq!(invoked(19_900), 0);
    assert_eq!(invoked(20_000), 1, "still on the 10 s grid, not 20.5 s");
    assert_eq!(
        invoked(35_000),
        1,
        "RunOnce: one run for the missed 30 s slot"
    );
    assert_eq!(invoked(39_999), 0);
    assert_eq!(invoked(40_000), 1);
}

#[test]
fn skip_moves_to_the_next_slot_after_now() {
    let runs = Arc::new(AtomicUsize::new(0));
    let scheduler = SchedulerTarget
        .build(&[(OWNER, counting(MisfirePolicy::Skip, &runs))])
        .unwrap();
    let clock = ManualClock::new(UNIX_EPOCH);
    block_on(scheduler.tick(&clock));
    at(&clock, 35_000);
    assert_eq!(block_on(scheduler.tick(&clock)).misfires_skipped, 1);
    at(&clock, 40_000);
    assert_eq!(block_on(scheduler.tick(&clock)).invoked, 1);
}

#[test]
fn run_now_ignores_the_schedule_but_not_the_gates() {
    let runs = Arc::new(AtomicUsize::new(0));
    let scheduler = SchedulerTarget
        .build(&[(OWNER, counting(MisfirePolicy::Skip, &runs))])
        .unwrap();
    assert!(matches!(
        block_on(scheduler.run_now("count")),
        RunNow::Ran(Ok(()))
    ));
    assert!(matches!(
        block_on(scheduler.run_now("count")),
        RunNow::Ran(Ok(()))
    ));
    assert_eq!(runs.load(Ordering::SeqCst), 2);
    let clock = ManualClock::new(UNIX_EPOCH);
    assert_eq!(
        block_on(scheduler.tick(&clock)).invoked,
        1,
        "schedule untouched"
    );
    assert!(matches!(
        block_on(scheduler.run_now("missing")),
        RunNow::NoSuchJob
    ));
    scheduler.stop_admission();
    assert!(matches!(
        block_on(scheduler.run_now("count")),
        RunNow::Stopped
    ));
}

#[test]
fn failed_jobs_are_reported_with_their_errors() {
    let failing = JobDeclaration::new("sync", TEN, MisfirePolicy::Skip, || async {
        Err::<(), JobError>("upstream timed out".into())
    });
    let scheduler = SchedulerTarget.build(&[(OWNER, failing)]).unwrap();
    let report = block_on(scheduler.tick(&ManualClock::new(UNIX_EPOCH)));
    assert_eq!(report.failed, 1);
    let (name, error) = &report.failures[0];
    assert_eq!(
        (name.as_str(), error.to_string()),
        ("sync", "upstream timed out".into())
    );
    assert!(matches!(
        block_on(scheduler.run_now("sync")),
        RunNow::Ran(Err(error)) if error.to_string() == "upstream timed out"
    ));
}
