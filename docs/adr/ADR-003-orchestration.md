# ADR-003: Orchestration

- Status: Accepted
- Date: 2026-11-25

## Context
The pipeline has two parts: a Databricks notebook (landing -> bronze -> silver)
and a dbt project (silver -> staging -> marts). Both must run weekly, in order,
after the Valuer General publishes new files.

## Decision
- Databricks job `nsw-property-weekly-sales`: Tuesday 06:00 Australia/Sydney,
  runs `notebooks/10_bronze_to_silver` from the `main` branch (Git source).
- dbt Cloud job `weekly-build` in the Production environment: Tuesday 07:00,
  runs `dbt build` into `analytics_staging` and `analytics_marts`.
- The two jobs are linked by time, not by an explicit dependency.

## Consequences
- Simple to set up, with no extra orchestration tool.
- If the Databricks job fails or runs longer than one hour, dbt builds marts
  from stale silver data. Mitigations, in order of effort:
  1. dbt source freshness on silver (`_updated_at`) as the first job step.
  2. Trigger the dbt job from the end of the Databricks job via the dbt Cloud API.
  3. Move both into Airflow with an explicit dependency.
- Production credentials use a personal access token; a real deployment
  would use a service principal.