-- ============================================
-- Hospital Readmission Analysis Queries
-- Author: Awanish Kumar
-- Tools: Snowflake, SQL
-- ============================================

-- 1. Total Readmissions by Department
SELECT 
    department_name,
    COUNT(patient_id) AS total_readmissions,
    ROUND(COUNT(patient_id) * 100.0 / SUM(COUNT(patient_id)) OVER(), 2) AS readmission_pct
FROM fact_readmission fr
JOIN dim_department dd ON fr.department_id = dd.department_id
WHERE readmission_flag = TRUE
GROUP BY department_name
ORDER BY total_readmissions DESC;

-- 2. 30-Day Readmission Rate by Doctor
SELECT 
    d.doctor_name,
    COUNT(DISTINCT f.patient_id) AS total_patients,
    SUM(CASE WHEN f.days_to_readmission <= 30 
        THEN 1 ELSE 0 END) AS readmissions_30_days,
    ROUND(SUM(CASE WHEN f.days_to_readmission <= 30 
        THEN 1 ELSE 0 END) * 100.0 / 
        COUNT(DISTINCT f.patient_id), 2) AS readmission_rate
FROM fact_readmission f
JOIN dim_doctor d ON f.doctor_id = d.doctor_id
GROUP BY d.doctor_name
ORDER BY readmission_rate DESC;

-- 3. Monthly Readmission Trend
SELECT 
    dt.year,
    dt.month_name,
    COUNT(patient_id) AS total_readmissions,
    LAG(COUNT(patient_id)) OVER (ORDER BY dt.year, dt.month) AS prev_month,
    COUNT(patient_id) - LAG(COUNT(patient_id)) 
        OVER (ORDER BY dt.year, dt.month) AS month_over_month_change
FROM fact_readmission fr
JOIN dim_date dt ON fr.admission_date_id = dt.date_id
GROUP BY dt.year, dt.month, dt.month_name
ORDER BY dt.year, dt.month;

-- 4. High Risk Patients (3+ Readmissions)
SELECT 
    p.patient_id,
    p.patient_name,
    p.age,
    p.diagnosis_category,
    COUNT(f.readmission_id) AS total_readmissions
FROM fact_readmission f
JOIN dim_patient p ON f.patient_id = p.patient_id
GROUP BY p.patient_id, p.patient_name, p.age, p.diagnosis_category
HAVING COUNT(f.readmission_id) >= 3
ORDER BY total_readmissions DESC;

-- 5. Snowflake Performance Optimized Query
-- Using clustering keys on admission_date
SELECT 
    dd.department_name,
    dt.quarter,
    dt.year,
    COUNT(DISTINCT fr.patient_id) AS unique_patients,
    AVG(fr.length_of_stay) AS avg_length_of_stay,
    SUM(fr.total_cost) AS total_cost
FROM fact_readmission fr
JOIN dim_department dd ON fr.department_id = dd.department_id
JOIN dim_date dt ON fr.admission_date_id = dt.date_id
WHERE dt.year >= 2023
GROUP BY dd.department_name, dt.quarter, dt.year
ORDER BY dt.year, dt.quarter;