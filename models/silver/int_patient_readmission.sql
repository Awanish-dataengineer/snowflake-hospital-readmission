-- ============================================
-- DBT Silver Model: Patient Readmission
-- Author: Awanish Kumar
-- Layer: Silver (Cleaned & Standardized)
-- ============================================

{{ config(
    materialized = 'incremental',
    schema = 'silver',
    unique_key = 'patient_id',
    cluster_by = ['admission_date']
) }}

WITH bronze_data AS (

    SELECT *
    FROM {{ ref('stg_patient_records') }}

),

cleaned AS (

    SELECT
        -- Patient Info
        UPPER(TRIM(patient_id))         AS patient_id,
        INITCAP(TRIM(patient_name))     AS patient_name,
        age,
        UPPER(TRIM(gender))             AS gender,

        -- Dates
        admission_date,
        discharge_date,

        -- Department & Doctor
        UPPER(TRIM(department_name))    AS department_name,
        INITCAP(TRIM(doctor_name))      AS doctor_name,

        -- Diagnosis
        UPPER(TRIM(diagnosis_code))     AS diagnosis_code,
        INITCAP(diagnosis_category)     AS diagnosis_category,

        -- Metrics
        length_of_stay,
        total_cost,
        readmission_flag,
        days_to_readmission,

        -- Derived Columns
        CASE
            WHEN days_to_readmission <= 30  THEN '30-Day Readmission'
            WHEN days_to_readmission <= 60  THEN '60-Day Readmission'
            WHEN days_to_readmission <= 90  THEN '90-Day Readmission'
            ELSE 'No Readmission'
        END                             AS readmission_category,

        CASE
            WHEN age < 18   THEN 'Pediatric'
            WHEN age <= 40  THEN 'Young Adult'
            WHEN age <= 60  THEN 'Middle Aged'
            ELSE 'Senior'
        END                             AS age_group,

        -- Audit Columns
        created_at,
        CURRENT_TIMESTAMP()             AS updated_at

    FROM bronze_data

),

deduplicated AS (

    SELECT *,
        ROW_NUMBER() OVER (
            PARTITION BY patient_id, admission_date
            ORDER BY created_at DESC
        ) AS row_num
    FROM cleaned

)

SELECT * EXCEPT (row_num)
FROM deduplicated
WHERE row_num = 1

{% if is_incremental() %}
    AND updated_at > (SELECT MAX(updated_at) FROM {{ this }})
{% endif %}