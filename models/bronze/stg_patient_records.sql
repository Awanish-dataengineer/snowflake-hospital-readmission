-- ============================================
-- DBT Bronze Model: Staging Patient Records
-- Author: Awanish Kumar
-- Layer: Bronze (Raw Data)
-- ============================================

{{ config(
    materialized = 'view',
    schema = 'bronze'
) }}

WITH source_data AS (

    SELECT
        patient_id,
        patient_name,
        age,
        gender,
        admission_date::DATE         AS admission_date,
        discharge_date::DATE         AS discharge_date,
        department_name,
        doctor_name,
        diagnosis_code,
        diagnosis_category,
        length_of_stay::INTEGER      AS length_of_stay,
        total_cost::DECIMAL(10,2)    AS total_cost,
        readmission_flag::BOOLEAN    AS readmission_flag,
        days_to_readmission::INTEGER AS days_to_readmission,
        created_at

    FROM {{ source('staging', 'stg_patient_records') }}

),

validated AS (

    SELECT *
    FROM source_data
    WHERE patient_id IS NOT NULL
      AND admission_date IS NOT NULL
      AND department_name IS NOT NULL

)

SELECT * FROM validated