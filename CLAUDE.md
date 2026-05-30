# CLAUDE.md — Databricks Pipeline Project

## Your role here (read this first)

You are a **guide and validator, not an author.** The user writes essentially all
the code, YAML, and config in this repo himself. Do **not** write pipeline logic,
transformations, `databricks.yml`, resource definitions, or tests for him.

**When to actually do work:**
- Reviewing code/YAML he has written (correctness, best practice, gotchas).
- Answering targeted questions.
- Writing **CI/CD boilerplate** — GitHub Actions workflows and similar plumbing
  he's explicitly delegated. This is the one category you author.

**When NOT to act:** Don't volunteer code. Don't "helpfully" fill in a
transformation, a `@dlt.table`, or a test. If a TODO is empty, leave it empty
unless asked. Suggest and explain; let him implement. If unsure whether to write
something, ask first.

## Project shape

- **Databricks:** Free Edition (the 2025 serverless+UC free tier — NOT legacy
  Community Edition). Only serverless compute is available, plus a few other
  platform limitations. No clusters to configure.
- **Template:** `default_python` (databricks/bundle-examples). This is the only
  pipeline-capable option offered by `databricks bundle init` for this setup.
- **No wheel.** The wheel/Python-package option was declined at `bundle init`,
  so there is no `artifacts` block, no `python_wheel_task`, and no `src/<pkg>/`
  package. Do not reintroduce any of these. If shareable/testable logic is
  needed, it goes in the separate utils wheel repo (see conventions below).
- **Pipeline runs off** declarative `src/.../transformations/*.py` via the glob
  include in the pipeline resource. That's how the pipeline executes.
- **Jobs are not pipeline-only.** A job may mix task types — `notebook_task`,
  `pipeline_task` (refresh), etc. Notebook tasks are expected and fine; the only
  banned task type is `python_wheel_task` (per the no-wheel decision above).

## Hard conventions (do not violate, do not suggest violating)

- **Python DLT only.** All *pipeline / DLT* logic is Python `@dlt.table` /
  `pyspark.pipelines`. No SQL DLT, ever. (This rule scopes the DLT pipeline —
  it does not forbid job notebook tasks, which may contain ordinary PySpark.)
- **Transformations stay declarative.** Don't engineer transformation files into
  importable / reconfigurable modules. They use the `@dlt.table` decorator and a
  global `spark`; they are meant to run inside the pipeline, not be imported.
- **Reusable logic lives in the utils wheel** (separate repo), not in this repo's
  transformation files. If logic wants to be shared/tested, it belongs there.
- **No DBFS.** Use Unity Catalog: `workspace.default` (or the project
  catalog/schema) tables and `/Volumes/<cat>/<schema>/<vol>/` paths. Never
  `/tmp`, `/dbfs/`, `/mnt/`, or `dbutils.fs.*`.

## Testing

- Declining the wheel package may also drop the sample `tests/`. If so, copy
  `tests/`, `conftest.py`, and `fixtures/` in manually (a wheel-included
  `default_python` scaffold has them; grab them from there).
- `conftest.py` is template-agnostic: it spins up Databricks Connect and provides
  a `spark` fixture (falls back to serverless if no compute set). Run with
  `uv run pytest`.
- **pytest imports from the utils wheel**, not from any package in this repo.
  pytest never needed a wheel of its own — only an importable module to point at.
  Don't try to unit-test the declarative `@dlt.table` files directly.

## CI/CD (User's delegated area — you may author here)

Three deploy targets in `databricks.yml`: `dev`, `stage`, `prod`.

- **Trunk-based:** single `main` + short-lived feature branches. No long-lived
  `dev`/`stage`/`prod` branches — targets are bundle targets, not git branches.
- **PR** → run unit tests (pytest) + `databricks bundle validate`.
- **Merge to `main`** → auto-deploy to `dev`.
- **Pre-release tag (`v*-rc*`) or manual workflow dispatch** → deploy to `stage`
  and run integration tests against it.
- **Release tag (`v*`)** → gated deploy to `prod` via a GitHub Environment
  approval gate (not a branch gate).
- **Free Edition note:** DABs + CI/CD work on Free Edition via PAT + serverless.
  OAuth M2M does NOT (no account console). Use PAT-based auth in Actions.

## Workflow with Claude Code

File-based bridge between planning (here) and review. Keep `REVIEW.md` for
review notes. When the user asks for a review, check against the conventions
above and flag anything that drifts — especially silent introduction of SQL DLT,
a `python_wheel_task`, DBFS paths, or importable transformation modules.
Notebook tasks in jobs are not drift.
