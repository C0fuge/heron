# Working in this repository (instructions for Claude sessions)

Trino connector (plugin) for our Arrow Flight SQL server (DuckDB over Parquet on HDFS/S3/lakeFS, Arrow results,
"partial" result mode = one endpoint per node, backup locations for failover). Modules: `arrow-flight-common`
(engine-neutral Flight client), `trino-arrow-flight` (the plugin), `starrocks-arrow-flight` (old skeleton, not
developed here — the native StarRocks connector lives in another repo). Current state, backlog and decisions:
`docs/STATUS.md`. Read it and `README.md` first.

## Session routine

A recurring cloud session develops this repo; it starts with zero context every time.

1. **Lock.** `git fetch origin`. If `docs/LOCK` holds a UTC time less than 110 minutes old, another session is
   working: stop without changes. Otherwise write `date -u +%FT%TZ` to `docs/LOCK`, commit `[skip ci] lock`,
   push. Empty the file in the last commit of the session.
2. **Branch.** Work on `main` (if pushing to `main` is rejected, use `claude/trino-dev` and keep using it). Never
   force-push `main`.
3. **CI.** Build and test in GitHub Actions; `gh` works in the session (`gh run list/view --log-failed`,
   `gh workflow run`). Unit tests may also run in the session if Maven can download dependencies.
4. **Work** on the first open item of "Next steps" in `docs/STATUS.md`; finish one item properly rather than
   starting several.
5. **Before ending** (≤ ~90 min): update `docs/STATUS.md` (dated Log line with CI run ids, Next steps, Open
   questions), release the lock, commit, push. Never leave work only in the session.

## Rules

- The repo is **private**. Release `e2e-assets` holds the maintainer's server build — never make the repo
  public, never copy those assets elsewhere, never attach them to a product release.
- Actions minutes are limited (~2000/month shared with the maintainer's other private repos): keep jobs short,
  cache Maven and Docker layers, run the end-to-end job only when plugin code changed or on request.
- No secrets, credentials or personal data in the repo; test tokens are generated in the job.
- Commit/push only to this repository; no issues/PRs elsewhere.
- Behaviour that the maintainer's production depends on (see STATUS "Invariants") must not regress; every
  behaviour change gets a unit test and, where it touches the server protocol, an end-to-end check.
- Anything that needs the maintainer's decision goes to "Open questions" in `docs/STATUS.md`.
- Write docs and commit messages in English.
