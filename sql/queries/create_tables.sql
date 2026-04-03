-- ============================================
-- Snowflake Table Definitions
-- Author: Awanish Kumar
-- Project: Hospital Readmission Analysis
-- ============================================

-- Staging Table
CREATE OR REPLACE TABLE staging.stg_patient_records (
    patient_id          VARCHAR(20),
    patient_name        VARCHAR(100),
    age                 INTEGER,
    gender              VARCHAR(10),
    admission_date      DATE,
    discharge_date      DATE,
    department_name     VARCHAR(50),
    doctor_name         VARCHAR(100),
    diagnosis_code      VARCHAR(20),
    diagnosis_category  VARCHAR(50),
    length_of_stay      INTEGER,
    total_cost          DECIMAL(10,2),
    readmission_flag    BOOLEAN,
    days_to_readmission INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Patient
CREATE OR REPLACE TABLE dim_patient (
    patient_id          VARCHAR(20) PRIMARY KEY,
    patient_name        VARCHAR(100),
    age                 INTEGER,
    gender              VARCHAR(10),
    diagnosis_category  VARCHAR(50),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Doctor
CREATE OR REPLACE TABLE dim_doctor (
    doctor_id           VARCHAR(20) PRIMARY KEY,
    doctor_name         VARCHAR(100),
    specialization      VARCHAR(50),
    department_id       VARCHAR(20),
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Department
CREATE OR REPLACE TABLE dim_department (
    department_id       VARCHAR(20) PRIMARY KEY,
    department_name     VARCHAR(50),
    floor_number        INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimension: Date
CREATE OR REPLACE TABLE dim_date (
    date_id             INTEGER PRIMARY KEY,
    full_date           DATE,
    year                INTEGER,
    quarter             INTEGER,
    month               INTEGER,
    month_name          VARCHAR(20),
    week                INTEGER,
    day_of_week         VARCHAR(20)
);

-- Fact Table: Readmission
CREATE OR REPLACE TABLE fact_readmission (
    readmission_id      VARCHAR(30) PRIMARY KEY,
    patient_id          VARCHAR(20),
    doctor_id           VARCHAR(20),
    department_id       VARCHAR(20),
    admission_date_id   INTEGER,
    discharge_date_id   INTEGER,
    length_of_stay      INTEGER,
    total_cost          DECIMAL(10,2),
    readmission_flag    BOOLEAN,
    days_to_readmission INTEGER,
    created_at          TIMESTAMP DEFAULT CURRENT_TIMESTAMP(),
    -- Foreign Keys
    FOREIGN KEY (patient_id) REFERENCES dim_patient(patient_id),
    FOREIGN KEY (doctor_id) REFERENCES dim_doctor(doctor_id),
    FOREIGN KEY (department_id) REFERENCES dim_department(department_id),
    FOREIGN KEY (admission_date_id) REFERENCES dim_date(date_id)
);