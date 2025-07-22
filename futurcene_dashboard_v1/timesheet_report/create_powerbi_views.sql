-- PowerBI Data Model Views for Futurcene Dashboard v1

-- 1. dim_Tasks: Extract Account Name (Client_ID, Client_Name) from custom_fields
CREATE OR REPLACE VIEW clickup.dim_Tasks AS
SELECT
    t.id AS task_id,
    t.name AS task_name,
    t.status,
    t.orderindex,
    t.priority,
    t.due_date,
    t.start_date,
    t.date_created,
    t.date_updated,
    t.date_closed,
    t.closed,
    t.archived,
    t.list_id,
    t.folder_id,
    t.space_id,
    t.custom_id,
    t.parent,
    t.team_id,
    t.points,
    t.time_estimate,
    t.time_spent,
    t.duration_hours,
    -- Extract Account Name (Client_ID, Client_Name) from custom_fields JSONB
    (
        SELECT cf->'value'->0->>'id'
        FROM jsonb_array_elements(t.custom_fields) cf
        WHERE cf->>'name' = 'Account Name'
        LIMIT 1
    ) AS client_id,
    (
        SELECT cf->'value'->0->>'name'
        FROM jsonb_array_elements(t.custom_fields) cf
        WHERE cf->>'name' = 'Account Name'
        LIMIT 1
    ) AS client_name
FROM clickup.tasks t;

-- 2. dim_Clients: All columns from tasks where list_id = 901509635413 (no JSON extraction)
CREATE OR REPLACE VIEW clickup.dim_Clients AS
SELECT *
FROM clickup.tasks t
WHERE t.list_id = '901509635413';

-- 3. dim_Lists
CREATE OR REPLACE VIEW clickup.dim_Lists AS
SELECT * FROM clickup.lists;

-- 4. dim_Spaces
CREATE OR REPLACE VIEW clickup.dim_Spaces AS
SELECT * FROM clickup.spaces;

-- 5. dim_Folders
CREATE OR REPLACE VIEW clickup.dim_Folders AS
SELECT * FROM clickup.folders;

-- 6. dim_Projects: Only tasks where list_id = 901510836048
CREATE OR REPLACE VIEW clickup.dim_Projects AS
SELECT * FROM clickup.tasks WHERE list_id = '901510836048';

-- 7. fact_Timesheet
CREATE OR REPLACE VIEW clickup.fact_Timesheet AS
SELECT * FROM clickup.time_entries;

-- 8. dim_WIP: Custom dimension (empty for now)
CREATE TABLE IF NOT EXISTS clickup.dim_WIP (
    wip_id SERIAL PRIMARY KEY,
    wip_name TEXT
);

-- 9. dim_Calendar: Date dimension for PowerBI
CREATE OR REPLACE VIEW clickup.dim_Calendar AS
SELECT
    d::date AS date_id,
    d::date AS date,
    EXTRACT(YEAR FROM d) AS year,
    EXTRACT(MONTH FROM d) AS month,
    TO_CHAR(d, 'Mon') AS month_short,
    TO_CHAR(d, 'Month') AS month_long,
    EXTRACT(DAY FROM d) AS day,
    EXTRACT(DOY FROM d) AS day_of_year,
    EXTRACT(DOW FROM d) AS day_of_week,
    TO_CHAR(d, 'Dy') AS day_short,
    TO_CHAR(d, 'Day') AS day_name,
    EXTRACT(WEEK FROM d) AS week_of_year,
    'Q' || EXTRACT(QUARTER FROM d) || '-' || EXTRACT(YEAR FROM d) AS quarter,
    CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END AS fiscal_year,
    'FQ' ||
      CASE 
        WHEN EXTRACT(MONTH FROM d) BETWEEN 7 AND 9 THEN 1
        WHEN EXTRACT(MONTH FROM d) BETWEEN 10 AND 12 THEN 2
        WHEN EXTRACT(MONTH FROM d) BETWEEN 1 AND 3 THEN 3
        ELSE 4
      END || '-' ||
      (CASE WHEN EXTRACT(MONTH FROM d) >= 7 THEN EXTRACT(YEAR FROM d) + 1 ELSE EXTRACT(YEAR FROM d) END) AS fiscal_quarter,
    (EXTRACT(DOW FROM d) IN (0,6)) AS is_weekend
FROM generate_series('2025-01-01'::date, '2030-12-31'::date, interval '1 day') d; 