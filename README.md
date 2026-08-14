# FHIR Data Ingestion & Analytics — Documentation

## What this is

This is a Medallion-architecture pipeline in Microsoft Fabric that pulls Patient, Encounter, Observation, and Condition data from the public HAPI FHIR test server, versions it properly using SCD Type 2, and lands it in a set of Gold tables ready for reporting. Built as a Spark-notebook pipeline end to end — Raw and Bronze are pure notebooks, Silver does the actual versioning logic, Gold is a set of Delta tables consumed through the SQL analytics endpoint and a Power BI report on top.

I'm using the public sandbox at `https://hapi.fhir.org/baseR4`, so a couple of things below (data volume, some missing references) are a byproduct of that being shared, constantly-changing test data rather than a real clinical system. I've called those out where relevant instead of pretending the numbers are cleaner than they are.

## Architecture

```
FHIR API
   |
   v
Raw layer      -> Files/raw/{resource_type}/{extract_date}/page_NNN.json
   |               (untouched API response, wrapped with a small provenance envelope)
   v
Bronze layer   -> bronze_patient / bronze_encounter / bronze_observation / bronze_condition
   |               (one row per FHIR resource, append-only, full version history)
   v
Silver layer   -> silver_patient / silver_encounter / silver_observation / silver_condition
   |               (deduped, flattened, SCD2 versioned)
   v
Gold layer     -> Dim_Patient, Fact_Encounter, Fact_Observation, Fact_Condition
                   (current-state Delta tables, star schema, feeds Power BI)
```

Four notebooks do the work: `ingest_fhir_raw`, `bronze_load`, `silver_load`, `gold_load`. The first three are parameterized by `resource_type` and run once per resource — a Fabric Data Pipeline calls them in order (Patient, then Encounter, then Observation, then Condition, since the latter two reference the first two), and `gold_load` runs once at the end after all four Silver loads finish.

### Why one notebook per layer instead of one big script

Mostly so a failure in one resource doesn't take down the others. If `Condition` throws a schema error on some malformed test record, I don't want that killing the `Patient` load that already succeeded ten minutes earlier. It also means the pipeline orchestration is actually doing something — four notebook activities calling the same reusable notebook with different parameters, rather than one opaque script that loops internally.

## Raw layer

`ingest_fhir_raw` follows FHIR's `Bundle.link[relation=next]` pagination — not offset-based, you literally follow the URL the server gives you back until there isn't one. Each page gets written as its own JSON file, wrapped with `api_url_or_params` and `extraction_timestamp` so that metadata survives even at the Raw layer, before anything gets parsed.

The incremental window (`days_back`) is computed dynamically at run time rather than hardcoded to fixed calendar dates. Since this is a public, globally-shared test server, a fixed date range can return almost nothing depending on when other people last touched records in that window — a rolling "last N days from now" window is what actually gives repeatable, meaningful incremental behavior.

## Bronze layer

`bronze_load` reads the raw JSON pages for a resource + date, explodes `Bundle.entry[]` into one row per FHIR resource, and appends to a Delta table. It's genuinely append-only — every version of every resource that's ever been pulled stays in Bronze. That matters because Silver's SCD2 logic depends on being able to compare against history, not just the latest snapshot.

Two extra columns beyond the required `extraction_timestamp` / `api_url_or_params`: `raw_file_path` (which physical Raw file this row came from, for lineage) and `bronze_load_timestamp` (when this particular Bronze load ran, which isn't always the same moment as extraction if a load gets re-run).

## Silver layer — versioning

This is the part of the assignment that actually required some thought. `silver_load` does three things:

1. **Dedup** the Bronze batch down to one row per `resource_id`, keeping the highest `meta.versionId`.
2. **Flatten** the nested FHIR resource into real columns — patient name/gender/DOB, encounter status/class/period, observation values, condition status. Reference fields like `subject.reference` (`"Patient/abc123"`) get parsed down to a clean `patient_id` so Gold can actually join on them.
3. **Version** — compare the incoming `version_id` per `resource_id` against whatever's currently marked `IsCurrent = true` in Silver. No match or no existing row → straight insert. Version changed → the old row gets `IsCurrent = false` and `EffectiveEndDate` set, and a new row goes in as current.

That gives a proper SCD2 history: `IsCurrent`, `EffectiveStartDate`, `EffectiveEndDate` per row. Worth noting — if nothing changed between two runs, the merge step is a genuine no-op, nothing gets written, and Delta's own transaction log won't show a new entry either. That's expected, not a bug; `DESCRIBE HISTORY silver_patient` only logs actual writes.

**"When each API was called" vs. "when data was saved"** — I kept these as two separate timestamps on purpose (`extraction_timestamp` vs `silver_load_timestamp`), since they can genuinely differ if a load is re-run against already-landed data.

## Gold layer

Originally built these as SQL views (`CREATE OR REPLACE VIEW`), which is the more "live" pattern and matches the assignment's wording of "final warehouse views." Ran into a real limitation here worth documenting: Spark-created views in a Fabric Lakehouse don't reliably sync to the SQL analytics endpoint — they showed up fine on the Spark/notebook side but never appeared under Tables or Views when queried through the SQL endpoint, which is what Power BI's default semantic model actually reads from.

Switched to materializing Gold as real Delta tables instead (`CREATE OR REPLACE TABLE`). Tradeoff: Gold now needs `gold_load` to re-run to pick up new Silver data, instead of being always-live like a view would be. That's an acceptable cost here since Gold already sits at the end of the pipeline chain and gets refreshed on every pipeline run anyway.

### Tables

- **Dim_Patient** — one row per current patient (`PatientId`, name, gender, DOB)
- **Fact_Encounter** — `EncounterId`, `PatientId`, status, class, type, period
- **Fact_Observation** — `ObservationId`, `PatientId`, `EncounterId`, observation name/value/unit
- **Fact_Condition** — `ConditionId`, `PatientId`, `EncounterId`, condition name, clinical status, dates

### Relationships

Star schema, Dim_Patient at the center, with a secondary link from Encounter down to Observation/Condition:

- `Dim_Patient.PatientId` → `Fact_Encounter.PatientId` (1:many)
- `Dim_Patient.PatientId` → `Fact_Observation.PatientId` (1:many, kept **inactive** in the semantic model)
- `Dim_Patient.PatientId` → `Fact_Condition.PatientId` (1:many, kept **inactive**)
- `Fact_Encounter.EncounterId` → `Fact_Observation.EncounterId` (1:many)
- `Fact_Encounter.EncounterId` → `Fact_Condition.EncounterId` (1:many)

The direct Patient links to Observation/Condition are inactive rather than deleted, because there are two valid paths from Observation back to Patient (direct, or via Encounter) and Power BI won't allow both active at once — it flags it as an ambiguous path. Encounter is the active route; the direct relationships stay available for a specific DAX measure via `USERELATIONSHIP()` if ever needed, without interfering with default report behavior.

## A known data quality note: orphan records

After building Gold, I checked referential integrity between the fact tables and Dim_Patient and found 8 orphan Encounters and 59 orphan Observations — rows referencing a `PatientId` that doesn't exist in the current Dim_Patient table.

This isn't a join bug. Each resource type is ingested independently with its own incremental window, so it's entirely possible for an Encounter or Observation to fall inside this run's window while the Patient it references was last updated outside it — meaning that Patient was never pulled into Silver during this run, even though it exists on the server. This is a genuine, well-known characteristic of incremental pipelines (dimension lag), not something specific to this implementation.

I left it as-is rather than widening Patient's window to eliminate it, since documenting the tradeoff honestly seemed more useful here than hiding it behind a wider pull. A production fix would likely give Patient (a slowly-changing dimension) a much wider or unbounded window compared to the faster-moving fact resources.

## Reporting

The Gold tables feed a Power BI report through Fabric's default semantic model (Direct Lake mode — reads straight off the Delta tables, no import/refresh cycle). One quirk worth knowing if you're building on this: Direct Lake tables don't show a data preview when you're setting up relationships in Power BI, and won't validate cardinality for you the way Import mode does. I confirmed `PatientId` uniqueness manually with a `GROUP BY ... HAVING COUNT(*) > 1` check before trusting the "one to many" cardinality Power BI guessed at.

## Repo structure

```
/notebooks
    ingest_fhir_raw.py
    bronze_load.py
    silver_load.py
    gold_load.py
/pipelines
    fhir_ingestion_pipeline.json   (pipeline definition export)
/docs
    README.md                      (this file)
```

## Running it

1. Attach `FHIR_Lakehouse` as the default Lakehouse in each notebook.
2. Run `ingest_fhir_raw` → `bronze_load` → `silver_load` per resource (Patient, Encounter, Observation, Condition, in that order — Encounter/Observation/Condition reference Patient).
3. Run `gold_load` once, after all four Silver loads are done.
4. Or just trigger the pipeline, which does steps 2–3 for you.

## Optional extensions not attempted

XML ingestion — didn't build this out; the API defaults to JSON and adding XML parsing on top wasn't worth the added surface area given the time available. Would follow the same Raw/Bronze pattern, just with an XML parser in Bronze instead of `spark.read.json`.
