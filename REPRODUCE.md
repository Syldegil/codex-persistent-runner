# Reproduction

Linux, Python 3.11+, SQLite supplied with Python, systemd user manager for deployment. No dependencies beyond the standard library for tests.

```sh
python3 -m unittest -v test_runner test_portability
```

Copy config.example.json privately; replace every synthetic /home/user path, select your installed CLI executable and available model explicitly. Create an empty work root and a project subdirectory. Set PERSISTENT_RUNNER_CONFIG to the absolute config path. `python3 runner.py enqueue --project synthetic --cwd /home/user/runner-work/example --prompt prompt.txt --held --model MODEL` stages a held task. Do not run this example against an existing production queue.

Review the unit paths, resource limits, mount requirements and sandbox compatibility before installing it manually. This mission did not install/start/enable a service or invoke a model. Authentication must be provisioned independently using supported CLI methods; no credentials are supplied or copied. The runner uses an isolated CODEX_HOME inside state_dir. A missing authentication session is a blocked condition, not a reason to weaken isolation. Model/CLI flags retain historical semantics and require validation with your selected CLI version. Unit limits are example policy, not machine capacity claims.

Before manual service installation, create state_dir and work_dir with mode 0700 and the config parent with mode 0700; save the config with mode 0600. Install sources at the ExecStart location or edit the unit accordingly. Every ReadWritePaths entry must already exist and agree with config. The checkpoint resume command uses this runner and its explicit configuration. No installer is run automatically.
