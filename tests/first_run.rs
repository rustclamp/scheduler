//! Tests for when a job's first invocation happens.

use std::future::Future;
use std::pin::pin;
use std::sync::Mutex;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use rustclamp_core::{Clock, ContributionTarget, ModuleId};
use rustclamp_scheduler::{JobDeclaration, MisfirePolicy, SchedulerTarget};

const OWNER: ModuleId = ModuleId::new("test.module.heartbeat");
const INTERVAL: Duration = Duration::from_secs(10);

// A Mutex, not a Cell: core's Clock is Send + Sync (core#5).
struct ManualClock(Mutex<SystemTime>);

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        *self.0.lock().unwrap()
    }
}

impl ManualClock {
    fn advance(&self, by: Duration) {
        *self.0.lock().unwrap() += by;
    }
}

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

fn job() -> JobDeclaration {
    JobDeclaration::new("heartbeat", INTERVAL, MisfirePolicy::Skip, || async {
        Ok(())
    })
}

fn invocations(declaration: JobDeclaration, clock: &ManualClock, ticks: &[Duration]) -> Vec<usize> {
    let scheduler = SchedulerTarget.build(&[(OWNER, declaration)]).unwrap();
    ticks
        .iter()
        .map(|advance| {
            clock.advance(*advance);
            block_on(scheduler.tick(clock)).invoked
        })
        .collect()
}

#[test]
fn a_job_runs_on_the_first_tick_by_default() {
    let clock = ManualClock(Mutex::new(SystemTime::UNIX_EPOCH));
    let ticks = [
        Duration::ZERO,
        Duration::from_secs(5),
        Duration::from_secs(5),
    ];
    assert_eq!(invocations(job(), &clock, &ticks), [1, 0, 1]);
}

#[test]
fn first_run_after_interval_waits_one_interval() {
    let clock = ManualClock(Mutex::new(SystemTime::UNIX_EPOCH));
    let ticks = [
        Duration::ZERO,
        Duration::from_secs(5),
        Duration::from_secs(5),
        Duration::from_secs(10),
    ];
    assert_eq!(
        invocations(job().first_run_after_interval(), &clock, &ticks),
        [0, 0, 1, 1]
    );
}

#[cfg(feature = "tokio")]
#[test]
fn run_until_ticks_until_stopped_then_closes_admission() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use rustclamp_core::SystemClock;

    let runs = Arc::new(AtomicUsize::new(0));
    let counter = runs.clone();
    let declaration = JobDeclaration::new(
        "beat",
        Duration::from_millis(20),
        MisfirePolicy::Skip,
        move || {
            counter.fetch_add(1, Ordering::SeqCst);
            async { Ok(()) }
        },
    )
    .first_run_after_interval();
    let scheduler = SchedulerTarget.build(&[(OWNER, declaration)]).unwrap();
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_time()
        .build()
        .unwrap();
    runtime.block_on(async {
        let stop = tokio::time::sleep(Duration::from_millis(110));
        scheduler
            .run_until(&SystemClock, Duration::from_millis(5), stop)
            .await;
    });
    let beats = runs.load(Ordering::SeqCst);
    assert!((3..=6).contains(&beats), "{beats} beats in 110 ms at 20 ms");
    let after = runtime.block_on(scheduler.tick(&SystemClock));
    assert!(after.admission_stopped || after.invoked == 0);
}
