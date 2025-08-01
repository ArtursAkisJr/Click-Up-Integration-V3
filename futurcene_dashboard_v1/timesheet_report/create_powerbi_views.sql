-- PowerBI Data Model Views for Futurcene Dashboard v1
-- Updated to create views in public schema with clickup_ prefix

-- 1. clickup_dim_Tasks: Extract Account Name (Client_ID, Client_Name) from custom_fields
CREATE OR REPLACE VIEW public.clickup_dim_Tasks AS
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

-- 2. clickup_dim_Clients: All columns from tasks where list_id = 901509635413 (no JSON extraction)
CREATE OR REPLACE VIEW public.clickup_dim_Clients AS
SELECT *
FROM clickup.tasks t
WHERE t.list_id = '901509635413';

-- 3. clickup_dim_Lists
CREATE OR REPLACE VIEW public.clickup_dim_Lists AS
SELECT * FROM clickup.lists;

-- 4. clickup_dim_Spaces
CREATE OR REPLACE VIEW public.clickup_dim_Spaces AS
SELECT * FROM clickup.spaces;

-- 5. clickup_dim_Folders
CREATE OR REPLACE VIEW public.clickup_dim_Folders AS
SELECT * FROM clickup.folders;

-- 6. clickup_dim_Projects: Only tasks where list_id = 901510836048, extract Account Name
CREATE OR REPLACE VIEW public.clickup_dim_Projects AS
SELECT
    t.*,
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
FROM clickup.tasks t
WHERE t.list_id = '901510836048';

-- 7. clickup_fact_Timesheet
CREATE OR REPLACE VIEW public.clickup_fact_Timesheet AS
SELECT * FROM clickup.time_entries;

-- 8. clickup_dim_WIP: Custom dimension (empty for now)
CREATE TABLE IF NOT EXISTS public.clickup_dim_WIP (
    wip_id SERIAL PRIMARY KEY,
    wip_name TEXT
);

-- 9. clickup_dim_Calendar: Date dimension for PowerBI
CREATE OR REPLACE VIEW public.clickup_dim_Calendar AS
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
    (EXTRACT(DOW FROM d) IN (0,6)) AS is_weekend,
    nh.name AS holiday_name,
    CASE WHEN nh.date IS NOT NULL OR EXTRACT(DOW FROM d) IN (0,6) THEN 0 ELSE 8 END AS standard_hours,
    CASE WHEN nh.date IS NOT NULL OR EXTRACT(DOW FROM d) IN (0,6) THEN 0 ELSE 8 END AS working_hours
FROM generate_series('2025-01-01'::date, '2025-12-31'::date, interval '1 day') d
LEFT JOIN clickup.national_holidays nh ON d::date = nh.date; 

-- 10. clickup_dim_Members: All columns from team_members for PowerBI
CREATE OR REPLACE VIEW public.clickup_dim_Members AS
SELECT * FROM clickup.team_members; 