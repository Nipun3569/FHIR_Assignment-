from pyspark.sql.functions import lit, current_timestamp
from datetime import datetime, timezone


pipeline_run_id = "manual-test-run"

resource_types = ["Patient", "Encounter", "Observation", "Condition"]
log_table = "pipeline_run_log"

run_timestamp = datetime.now(timezone.utc).isoformat()
log_rows = []


for rt in resource_types:
    bronze_table = f"bronze_{rt.lower()}"
    silver_table = f"silver_{rt.lower()}"

    bronze_count = spark.table(bronze_table).count() if spark.catalog.tableExists(bronze_table) else 0
    silver_current_count = (
        spark.table(silver_table).filter("IsCurrent = true").count()
        if spark.catalog.tableExists(silver_table) else 0
    )
    silver_total_count = (
        spark.table(silver_table).count()
        if spark.catalog.tableExists(silver_table) else 0
    )
    versioned_count = silver_total_count - silver_current_count  # rows with real SCD2 history

    log_rows.append((
        pipeline_run_id, run_timestamp, rt, "bronze", bronze_count, None
    ))
    log_rows.append((
        pipeline_run_id, run_timestamp, rt, "silver", silver_current_count, versioned_count
    ))
    print(f"[{rt}] bronze={bronze_count}, silver_current={silver_current_count}, versioned_history_rows={versioned_count}")


gold_tables = ["Dim_Patient", "Fact_Encounter", "Fact_Observation", "Fact_Condition"]
for gt in gold_tables:
    if spark.catalog.tableExists(gt):
        count = spark.table(gt).count()
        log_rows.append((pipeline_run_id, run_timestamp, gt, "gold", count, None))
        print(f"[{gt}] gold_rows={count}")

if spark.catalog.tableExists("Fact_Encounter") and spark.catalog.tableExists("Dim_Patient"):
    orphan_encounters = spark.sql("""
        SELECT COUNT(*) AS c FROM Fact_Encounter e
        LEFT ANTI JOIN Dim_Patient p ON e.PatientId = p.PatientId
    """).collect()[0]["c"]
    log_rows.append((pipeline_run_id, run_timestamp, "Fact_Encounter", "orphan_check", orphan_encounters, None))
    print(f"orphan_encounters={orphan_encounters}")

if spark.catalog.tableExists("Fact_Observation") and spark.catalog.tableExists("Dim_Patient"):
    orphan_observations = spark.sql("""
        SELECT COUNT(*) AS c FROM Fact_Observation o
        LEFT ANTI JOIN Dim_Patient p ON o.PatientId = p.PatientId
    """).collect()[0]["c"]
    log_rows.append((pipeline_run_id, run_timestamp, "Fact_Observation", "orphan_check", orphan_observations, None))
    print(f"orphan_observations={orphan_observations}")


schema = ["pipeline_run_id", "run_timestamp", "resource_or_table", "layer", "row_count", "versioned_history_rows"]
log_df = spark.createDataFrame(log_rows, schema=schema).withColumn("logged_at", current_timestamp())

if spark.catalog.tableExists(log_table):
    log_df.write.format("delta").mode("append").saveAsTable(log_table)
else:
    log_df.write.format("delta").mode("overwrite").saveAsTable(log_table)

print(f"Logged {len(log_rows)} metric rows to '{log_table}' for run {pipeline_run_id}")

notebookutils.notebook.exit(str({
    "pipeline_run_id": pipeline_run_id,
    "run_timestamp": run_timestamp,
    "metrics_logged": len(log_rows),
}))
