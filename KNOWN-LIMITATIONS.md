# Limitations

Linux-only /proc and flock assumptions. One worker per state directory. Stop the service before mutating active task state. Pause prevents new starts and does not interrupt the current task. Logs/checkpoints can contain sensitive content despite best-effort redaction. Resource checks are admission checks, not guarantees. Mount checks compare configured source strings. Heavy tasks are refused pending separate capacity qualification.

The generic unit is unqualified with a real CLI. Sandbox flags, authentication renewal, reboot/logout/session lock and application side effects need their own tests. Selftest success is not real model success. Output and retry budgets are bounded but not billing guarantees. No exactly-once guarantee; reconcile files and external effects before authorizing replay. No production queue, prompts, sessions or authentication are included.
