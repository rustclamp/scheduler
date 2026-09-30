//! Tests for when a job's first invocation happens.

use std::cell::Cell;
use std::future::Future;
use std::pin::pin;
use std::task::{Context, Poll, Waker};
use std::time::{Duration, SystemTime};

use rustclamp_core::{Clock, ContributionTarget, ModuleId};
use rustclamp_scheduler::{JobDeclaration, MisfirePolicy, SchedulerTarget};

const OWNER: ModuleId = ModuleId::new("test.module.heartbeat");
const INTERVAL: Duration = Duration::from_secs(10);

struct ManualClock(Cell<SystemTime>);

impl Clock for ManualClock {
    fn now(&self) -> SystemTime {
        self.0.get()
    }
}

impl ManualClock {
    fn advance(&self, by: Duration) {
        self.0.set(self.0.get() + by);
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
    let clock = ManualClock(Cell::new(SystemTime::UNIX_EPOCH));
    let ticks = [
        Duration::ZERO,
        Duration::from_secs(5),
        Duration::from_secs(5),
    ];
    assert_eq!(invocations(job(), &clock, &ticks), [1, 0, 1]);
}

#[test]
fn first_run_after_interval_waits_one_interval() {
    let clock = ManualClock(Cell::new(SystemTime::UNIX_EPOCH));
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
