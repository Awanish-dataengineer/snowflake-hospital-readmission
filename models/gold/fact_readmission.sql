-- ============================================
-- DBT Gold Model: Fact Readmission
-- Author: Awanish Kumar
-- Layer: Gold (Star Schema - Fact Table)
-- ============================================

{{ config(
    materialized = 'table',
    schema = 'gold',
    cluster_by = ['admission_date', 'department_name']
) }}

WITH silver_data AS (

    SELECT *
    FROM {{ ref('int_patient_readmission') }}

),

dim_date AS (

    SELECT *
    FROM {{ ref('dim_date') }}

),

dim_patient AS (

    SELECT
        patient_id,
        patient_name,
        age,
        gender,
        age_group,
        diagnosis_category
    FROM {{ ref('dim_patient') }}

),

dim_department AS (

    SELECT
        department_id,
        department_name
    FROM {{ ref('dim_department') }}

),

dim_doctor AS (

    SELECT
        doctor_id,
        doctor_name,
        specialization
    FROM {{ ref('dim_doctor') }}

),

final AS (

    SELECT
        -- Keys
        {{ dbt_utils.generate_surrogate_key([
            's.patient_id',
            's.admission_date'
        ]) }}                               AS readmission_id,

        s.patient_id,
        dd.department_id,
        dr.doctor_id,
        dt.date_id                          AS admission_date_id,

        -- Patient Details
        p.patient_name,
        p.age,
        p.gender,
        p.age_group,
        p.diagnosis_category,

        -- Department & Doctor
        s.department_name,
        dr.doctor_name,
        dr.specialization,

        -- Dates
        s.admission_date,
        s.discharge_date,
        dt.year,
        dt.quarter,
        dt.month_name,

        -- Metrics
        s.length_of_stay,
        s.total_cost,
        s.readmission_flag,
        s.days_to_readmission,
        s.readmission_category,

        -- Audit
        s.created_at,
        s.updated_at

    FROM silver_data s
    LEFT JOIN dim_patient p
        ON s.patient_id = p.patient_id
    LEFT JOIN dim_department dd
        ON s.department_name = dd.department_name
    LEFT JOIN dim_doctor dr
        ON s.doctor_name = dr.doctor_name
    LEFT JOIN dim_date dt
        ON s.admission_date = dt.full_date

)

SELECT * FROM final