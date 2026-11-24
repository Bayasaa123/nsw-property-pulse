# ADR-001: Environments and storage

- Status: Accepted
- Date: 2026-11-25

## Context
Production lakehouses keep data in the company's own object storage (S3/ADLS)
and use Databricks for compute and governance. This project must cost close to $0.
Databricks Free Edition is free but does not support custom storage locations.

## Decision
One codebase, two environments:

| | dev | prod |
|---|---|---|
| Platform | Databricks Free Edition | Databricks on AWS (trial) |
| Storage | Databricks default storage | Own S3 bucket via Unity Catalog external location |
| Catalog | `workspace` | `nsw_prod` |
| When | All development | Final 2 weeks: deploy, record, tear down |

Notebooks read `catalog` and `schema` from widgets and never hard-code paths,
so the same code runs in both environments.

## Consequences
- Development is free; prod cost is covered by trial credits.
- Raw files can be re-downloaded from the Valuer General, so losing an
  environment loses no information.
- Free Edition compute quotas limit backfill to the last 10 years.