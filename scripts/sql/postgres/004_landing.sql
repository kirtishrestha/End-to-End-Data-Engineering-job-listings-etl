\connect workforce_db;

CREATE TABLE IF NOT EXISTS landing.job_listings (
    id BIGINT PRIMARY KEY,
    publishedat DATE,
    salary VARCHAR(255),
    title VARCHAR(255) NOT NULL,
    companyname VARCHAR(255),
    location VARCHAR(255),
    applicationscount VARCHAR(255),
    description TEXT,
    contracttype VARCHAR(50),
    experiencelevel VARCHAR(50),
    worktype VARCHAR(50),
    sector VARCHAR(255),
    applytype VARCHAR(50),
    companyid BIGINT,
    ingested_at TIMESTAMP WITHOUT TIME ZONE DEFAULT CURRENT_TIMESTAMP
);