# rustclamp-scheduler

Clock-driven job declarations and scheduling policy, separate from async task
execution. The target validates unique job names and positive intervals before
runtime. Each tick uses Core's replaceable `Clock`, runs due jobs sequentially,
and limits each job to one active invocation. The target caps declarations at
128 jobs.

Misfires either skip stale intervals or run once and schedule from the current
time. A process-local lock prevents overlapping invocations in one scheduler;
it does not claim cross-process coordination. Applications that run multiple
scheduler processes need an explicit distributed lock capability. Job handlers
are existing application operations wrapped as declarations and receive no
scheduler-specific context.

Shutdown owners call `stop_admission()` and await the tick futures they own.
This closes the admission gate before active operations drain; no queued job
buffer exists inside the scheduler.
