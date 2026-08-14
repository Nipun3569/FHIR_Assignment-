
spark.sql("""
CREATE OR REPLACE TABLE Dim_Patient AS
SELECT
    resource_id     AS PatientId,
    family_name,
    given_name,
    gender,
    birth_date,
    fhir_last_updated
FROM silver_patient
WHERE IsCurrent = true
""")

# --------------------------------------------------------------
# Fact_Encounter
# --------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE Fact_Encounter AS
SELECT
    e.resource_id       AS EncounterId,
    e.patient_id         AS PatientId,
    e.status,
    e.encounter_class,
    e.encounter_type,
    e.period_start,
    e.period_end,
    e.fhir_last_updated
FROM silver_encounter e
WHERE e.IsCurrent = true
""")

# --------------------------------------------------------------
# Fact_Observation
# --------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE Fact_Observation AS
SELECT
    o.resource_id        AS ObservationId,
    o.patient_id          AS PatientId,
    o.encounter_id        AS EncounterId,
    o.observation_name,
    o.value,
    o.unit,
    o.status,
    o.effective_datetime,
    o.fhir_last_updated
FROM silver_observation o
WHERE o.IsCurrent = true
""")

# --------------------------------------------------------------
# Fact_Condition
# --------------------------------------------------------------
spark.sql("""
CREATE OR REPLACE TABLE Fact_Condition AS
SELECT
    c.resource_id         AS ConditionId,
    c.patient_id           AS PatientId,
    c.encounter_id         AS EncounterId,
    c.condition_name,
    c.clinical_status,
    c.onset_datetime,
    c.recorded_date,
    c.fhir_last_updated
FROM silver_condition c
WHERE c.IsCurrent = true
""")

print("Gold tables created: Dim_Patient, Fact_Encounter, Fact_Observation, Fact_Condition")


orphan_encounters = spark.sql("""
    SELECT COUNT(*) AS orphan_count
    FROM Fact_Encounter e
    LEFT ANTI JOIN Dim_Patient p ON e.PatientId = p.PatientId
""").collect()[0]["orphan_count"]

orphan_observations = spark.sql("""
    SELECT COUNT(*) AS orphan_count
    FROM Fact_Observation o
    LEFT ANTI JOIN Dim_Patient p ON o.PatientId = p.PatientId
""").collect()[0]["orphan_count"]

print(f"Orphan Encounters (no matching Patient): {orphan_encounters}")
print(f"Orphan Observations (no matching Patient): {orphan_observations}")

notebookutils.notebook.exit(str({
    "tables_created": ["Dim_Patient", "Fact_Encounter", "Fact_Observation", "Fact_Condition"],
    "orphan_encounters": orphan_encounters,
    "orphan_observations": orphan_observations,
}))
