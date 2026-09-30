//! Tests for concurrent ticks and the run loop sleeping until the next due job.
#![cfg(feature = "tokio")]

use std::time::{Duration, Instant, UNIX_EPOCH};

use rustclamp_core::{ContributionTarget, ManualClock, ModuleId};
use rustclamp_scheduler::{JobDeclaration, JobError, MisfirePolicy, SchedulerTarget};

const OWNER: ModuleId = ModuleId::new("test.module.jobs");
const DELAY: Duration = Duration::from_millis(100);
const HOUR: Duration = Duration::from_secs(3600);

fn slow(name: &'static str, delay: Duration, fail: bool) -> JobDeclaration {
    JobDeclaration::new(name, HOUR, MisfirePolicy::Skip, move || async move {
        tokio::time::sleep(delay).await;
        if fail {
            Err(JobError::from(name))
        } else {
            Ok(())
        }
    })
}

#[tokio::test]
async fn slow_job_does_not_delay_the_others_and_every_error_is_reported() {
    let scheduler = SchedulerTarget
        .build(&[
            (OWNER, slow("a", DELAY, true)),
            (OWNER, slow("b", DELAY, false)),
            (OWNER, slow("c", DELAY, true)),
        ])
        .unwrap();
    let clock = ManualClock::new(UNIX_EPOCH);
    let start = Instant::now();
    let report = scheduler.tick(&clock).await;
    assert!(start.elapsed() < DELAY * 2, "ran sequentially");
    assert_eq!((report.invoked, report.failed), (3, 2));
    let names: Vec<_> = report.failures.iter().map(|(n, _)| n.as_str()).collect();
    assert_eq!(names, ["a", "c"]);
}

#[tokio::test]
async fn run_loop_sleeps_until_the_next_due_job() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    let runs = Arc::new(AtomicUsize::new(0));
    let counter = Arc::clone(&runs);
    let job = JobDeclaration::new("hourly", HOUR, MisfirePolicy::Skip, move || {
        counter.fetch_add(1, Ordering::SeqCst);
        async { Ok(()) }
    });
    let scheduler = SchedulerTarget.build(&[(OWNER, job)]).unwrap();
    let clock = ManualClock::new(UNIX_EPOCH);
    let mut ticks = 0;
    let stop = tokio::time::sleep(Duration::from_millis(300));
    scheduler
        .run_until_reporting(&clock, Duration::from_millis(5), stop, |_| ticks += 1)
        .await;
    // The frozen test clock never makes the job due again: one run, and
    // wake-ups at the idle cap rather than every 5 ms resolution.
    assert_eq!(runs.load(Ordering::SeqCst), 1);
    assert!(ticks == 1, "{ticks} wake-ups in 300 ms");
}
