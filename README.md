<img src="https://docs.rustclamp.com/assets/rustclamp-logo.png" alt="RustClamp logo" width="160">

# rustclamp-scheduler

Clock-driven jobs for [RustClamp](https://github.com/rustclamp/rustclamp),
separate from async task execution. Modules contribute `JobDeclaration`s;
`SchedulerTarget` validates unique names and positive intervals (at most 128 jobs)
and compiles a `Scheduler`. Ticks use Core's replaceable `Clock`, so tests drive
time by hand.

## Install

Rust 1.96.1+, edition 2024:

```toml
[dependencies]
rustclamp-scheduler = { version = "1", features = ["tokio"] }
```

## Example

```rust
use std::time::Duration;
use rustclamp_core::{ContributionTarget, ModuleId};
use rustclamp_scheduler::{JobDeclaration, MisfirePolicy, SchedulerTarget};

const REPORTS: ModuleId = ModuleId::new("app.reports");

let scheduler = SchedulerTarget.build(&[(
    REPORTS,
    JobDeclaration::new("nightly", Duration::from_secs(86_400), MisfirePolicy::Skip, || async {
        Ok(())
    }),
)])?;
// with the `tokio` feature: scheduler.run_until(&clock, Duration::from_secs(1), shutdown).await;
```

## Main API

- `JobDeclaration::new(name, interval, MisfirePolicy, handler)`; `first_run_after_interval()` delays the first run by one interval (default: first tick).
- `MisfirePolicy::Skip` drops a stale interval; `RunOnce` runs once for any number missed. Schedules stay on the original grid (`first + k * interval`), so late ticks do not drift.
- `Scheduler::tick(&clock)` returns a `TickReport` (with job `failures`). Due jobs run concurrently; each job has at most one active invocation.
- `Scheduler::run_now(name)` returns a `RunNow`; overlap and admission rules apply, the schedule is untouched.
- `Scheduler::stop_admission()` closes the gate before shutdown owners await active ticks. No queued job buffer exists.
- `SchedulerJobs`: ready-made qualifier for the job target.

- `Scheduler::leader_only(lock)` runs jobs only while a `LeaderLock` says this instance leads (see below).

The overlap lock is process-local. With several replicas, use `leader_only` so each job runs once:
`LeaderLock::hold` is asked once per tick that has a due job; a standby skips (counted in
`TickReport::not_leader`, its schedule still advances) and takes over on its next due tick after
the leader loses the lock. `rustclamp-postgres` implements it with an advisory lock (feature `scheduler`):

```rust
let scheduler = scheduler.leader_only(db.leader_lock("app.scheduler"));
```

## Features

| Feature | Adds |
| --- | --- |
| `tokio` | `Scheduler::run_until` / `run_until_reporting`: sleep until the next job is due (at most a minute, at least one resolution) and tick until `stop` completes |

Changes are tracked in [CHANGELOG.md](CHANGELOG.md).

Full documentation: <https://docs.rustclamp.com>

## License

Licensed under either of [Apache License, Version 2.0](LICENSE-APACHE) or
[MIT license](LICENSE-MIT) at your option. Unless you state otherwise, any
contribution you submit for inclusion is dual licensed as above, without
additional terms or conditions.
