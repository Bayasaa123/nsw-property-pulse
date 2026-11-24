# ADR-002: Silver merge key for late-arriving sales

- Status: Accepted
- Date: 2026-11-25

## Context
The weekly file for district 001 (23 Nov 2026) contains a sale with contract
date 2022-06-22 that was reported in November 2026 (likely off-the-plan).
An incremental load based on a contract-date window would miss such sales.

## Decision
Silver is MERGEd on the business key `property_id + dealing_number`, with no
date window. When the same sale arrives again, the row with the newest
download timestamp wins. `sale_counter` is not used for identity because it
is only a sequence number inside one file.

## Consequences
- Late sales of any age are handled, and re-running a file changes nothing.
- Each MERGE scans the whole silver table. This is fine at a few million rows;
  at larger scale, add liquid clustering on `property_id`.
- Sales without a dealing number cannot be identified and go to quarantine.