# Changelog

## Unreleased

- Fix drift: after a late tick the next run is the next slot on the original
  grid (`first + k * interval`), not `now + interval` (#3).
- Add `Scheduler::run_now(name) -> RunNow`, for admin actions and tests;
  overlap and admission rules still apply, the schedule is untouched (#3).
- Add `SchedulerJobs`, a qualifier for the job target (#3).
- Breaking: `TickReport` gains `failures: Vec<(String, JobError)>` and is no
  longer `Copy`/`Eq`; `run_until_reporting` hands each report to a callback,
  where `run_until` drops them (#3).
- Add feature `tokio`: `Scheduler::run_until(clock, resolution, stop)` drives ticks
  until `stop` completes, then stops admission (ADR 0020).
- Add `JobDeclaration::first_run_after_interval`: opt-in delay of the first
  invocation by one interval (default still runs on the first tick).
- Add clock-driven job declarations, validation, misfire behavior, and overlap control.
