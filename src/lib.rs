//! Contribution target for bounded, clock-driven application jobs.

#![forbid(unsafe_code)]
#![deny(missing_docs)]

use rustclamp_core::{
    Clock, Contribution, ContributionId, ContributionTarget, ContributionTargetId, ModuleId,
};
use std::collections::{BTreeMap, btree_map::Entry};
use std::error::Error;
use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime};

const MAX_JOBS: usize = 128;

/// A boxed future returned by a scheduled application operation.
pub type JobFuture = Pin<Box<dyn Future<Output = Result<(), JobError>> + Send + 'static>>;

/// Failure returned by a scheduled operation.
pub type JobError = Box<dyn Error + Send + Sync>;

type Handler = Arc<dyn Fn() -> JobFuture + Send + Sync + 'static>;

/// How the scheduler treats multiple missed intervals.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum MisfirePolicy {
    /// Skip the job if it is at least one full interval late.
    Skip,
    /// Run one invocation for any number of missed intervals.
    RunOnce,
}

/// A module's declaration of one scheduled application operation.
pub struct JobDeclaration {
    name: String,
    interval: Duration,
    misfire: MisfirePolicy,
    handler: Handler,
}

impl JobDeclaration {
    /// Creates a job declaration; the target validates its name and interval.
    pub fn new<F, Fut>(
        name: impl Into<String>,
        interval: Duration,
        misfire: MisfirePolicy,
        handler: F,
    ) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<(), JobError>> + Send + 'static,
    {
        Self {
            name: name.into(),
            interval,
            misfire,
            handler: Arc::new(move || Box::pin(handler())),
        }
    }
}

impl Contribution for JobDeclaration {
    const ID: ContributionId = ContributionId::new("rustclamp.scheduler.job");
}

/// Target that validates and compiles scheduled job declarations.
pub struct SchedulerTarget;

impl ContributionTarget for SchedulerTarget {
    type Contribution = JobDeclaration;
    type Runtime = Scheduler;
    type Error = ScheduleBuildError;

    const ID: ContributionTargetId = ContributionTargetId::new("rustclamp.scheduler.jobs");

    fn build(
        &self,
        contributions: &[(ModuleId, Self::Contribution)],
    ) -> Result<Self::Runtime, Self::Error> {
        if contributions.len() > MAX_JOBS {
            return Err(ScheduleBuildError::TooManyJobs {
                maximum: MAX_JOBS,
                actual: contributions.len(),
            });
        }
        let mut jobs = BTreeMap::new();
        for (owner, declaration) in contributions {
            if declaration.name.trim().is_empty() {
                return Err(ScheduleBuildError::EmptyName { owner: *owner });
            }
            if declaration.interval.is_zero() {
                return Err(ScheduleBuildError::ZeroInterval {
                    owner: *owner,
                    name: declaration.name.clone(),
                });
            }
            match jobs.entry(declaration.name.clone()) {
                Entry::Vacant(entry) => {
                    entry.insert(Job {
                        interval: declaration.interval,
                        misfire: declaration.misfire,
                        handler: Arc::clone(&declaration.handler),
                        next_run: Mutex::new(None),
                        running: AtomicBool::new(false),
                    });
                }
                Entry::Occupied(_) => {
                    return Err(ScheduleBuildError::DuplicateName {
                        owner: *owner,
                        name: declaration.name.clone(),
                    });
                }
            }
        }
        Ok(Scheduler { jobs })
    }
}

struct Job {
    interval: Duration,
    misfire: MisfirePolicy,
    handler: Handler,
    next_run: Mutex<Option<SystemTime>>,
    running: AtomicBool,
}

/// Compiled schedule. Invocations are sequential within one tick and never overlap per job.
pub struct Scheduler {
    jobs: BTreeMap<String, Job>,
}

impl Scheduler {
    /// Runs jobs due at the injected clock's current time.
    pub async fn tick(&self, clock: &dyn Clock) -> TickReport {
        let now = clock.now();
        let mut report = TickReport::default();
        for job in self.jobs.values() {
            match is_due(job, now) {
                Due::No => continue,
                Due::SkippedMisfire => {
                    report.misfires_skipped += 1;
                    continue;
                }
                Due::Yes => {}
            }
            if job
                .running
                .compare_exchange(false, true, Ordering::AcqRel, Ordering::Acquire)
                .is_err()
            {
                report.overlaps_skipped += 1;
                continue;
            }
            let _running = RunningGuard(&job.running);
            report.invoked += 1;
            if (job.handler)().await.is_err() {
                report.failed += 1;
            }
        }
        report
    }

    /// Returns how many job declarations were compiled.
    pub fn len(&self) -> usize {
        self.jobs.len()
    }

    /// Reports whether this target compiled no jobs.
    pub fn is_empty(&self) -> bool {
        self.jobs.is_empty()
    }
}

enum Due {
    No,
    Yes,
    SkippedMisfire,
}

fn is_due(job: &Job, now: SystemTime) -> Due {
    let Ok(mut next_run) = job.next_run.lock() else {
        return Due::No;
    };
    let scheduled = *next_run.get_or_insert(now);
    if now < scheduled {
        return Due::No;
    }
    let late_by = now.duration_since(scheduled).unwrap_or_default();
    let missed_intervals = late_by.as_nanos() / job.interval.as_nanos();
    if missed_intervals > 0 && job.misfire == MisfirePolicy::Skip {
        *next_run = now.checked_add(job.interval);
        return Due::SkippedMisfire;
    }
    *next_run = now.checked_add(job.interval);
    Due::Yes
}

struct RunningGuard<'a>(&'a AtomicBool);

impl Drop for RunningGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

/// Summary of one scheduler tick.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
pub struct TickReport {
    /// Jobs invoked during this tick.
    pub invoked: usize,
    /// Jobs skipped because their configured misfire policy required it.
    pub misfires_skipped: usize,
    /// Jobs skipped because the same job was already running.
    pub overlaps_skipped: usize,
    /// Invoked jobs that returned an error.
    pub failed: usize,
}

/// Invalid or conflicting schedule declarations detected before runtime.
#[derive(Clone, Debug, Eq, PartialEq)]
pub enum ScheduleBuildError {
    /// A module declared a blank job name.
    EmptyName {
        /// Module that contributed the invalid declaration.
        owner: ModuleId,
    },
    /// A module declared a zero-length interval.
    ZeroInterval {
        /// Module that contributed the invalid declaration.
        owner: ModuleId,
        /// Invalid job name.
        name: String,
    },
    /// Two modules declared the same job name.
    DuplicateName {
        /// Module that supplied the conflicting declaration.
        owner: ModuleId,
        /// Conflicting job name.
        name: String,
    },
    /// The target received more jobs than its declared capacity.
    TooManyJobs {
        /// Maximum accepted declaration count.
        maximum: usize,
        /// Number of declarations received.
        actual: usize,
    },
}

impl fmt::Display for ScheduleBuildError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::EmptyName { owner } => write!(f, "module {owner:?} declared an empty job name"),
            Self::ZeroInterval { owner, name } => {
                write!(f, "module {owner:?} declared zero interval for {name:?}")
            }
            Self::DuplicateName { owner, name } => {
                write!(f, "module {owner:?} conflicts on job name {name:?}")
            }
            Self::TooManyJobs { maximum, actual } => {
                write!(f, "scheduler job limit is {maximum}, received {actual}")
            }
        }
    }
}

impl Error for ScheduleBuildError {}
