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
FROM generate_series('2025-01-01'::date, '2026-12-31'::date, interval '1 day') d
LEFT JOIN clickup.national_holidays nh ON d::date = nh.date; 

-- 10. clickup_dim_Members: All columns from team_members for PowerBI
CREATE OR REPLACE VIEW public.clickup_dim_Members AS
SELECT * FROM clickup.team_members;

-- 11. clickup_fact_StandardHours: Standard hours calculation for each user-date combination
CREATE OR REPLACE VIEW public.clickup_fact_StandardHours AS
WITH user_join_dates AS (
    -- Determine each user's join date (earliest time entry)
    SELECT 
        user_id,
        DATE(MIN(start_datetime)) AS join_date
    FROM clickup.time_entries 
    WHERE user_id IS NOT NULL AND start_datetime IS NOT NULL
    GROUP BY user_id
),
calendar_with_join_dates AS (
    -- Generate calendar dates and join user join dates
    SELECT 
        c.date_id,
        c.date,
        c.year,
        c.month,
        c.day,
        c.is_weekend,
        c.holiday_name,
        c.standard_hours AS base_standard_hours,
        ujd.user_id,
        ujd.join_date,
        -- Calculate days in month for the user's join month
        CASE 
            WHEN EXTRACT(YEAR FROM c.date) = EXTRACT(YEAR FROM ujd.join_date) 
                 AND EXTRACT(MONTH FROM c.date) = EXTRACT(MONTH FROM ujd.join_date) THEN
                -- For the join month, calculate partial days
                CASE 
                    WHEN c.date >= ujd.join_date THEN
                        -- User was active this day in join month
                        c.base_standard_hours
                    ELSE 0
                END
            ELSE
                -- For other months, use full standard hours
                c.base_standard_hours
        END AS adjusted_standard_hours
    FROM public.clickup_dim_Calendar c
    CROSS JOIN user_join_dates ujd
    WHERE c.date >= ujd.join_date  -- Only include dates after user joined
),
monthly_working_days AS (
    -- Calculate working days per month for each user
    SELECT 
        user_id,
        year,
        month,
        COUNT(*) AS working_days_in_month,
        SUM(adjusted_standard_hours) AS total_standard_hours_in_month
    FROM calendar_with_join_dates
    WHERE adjusted_standard_hours > 0
    GROUP BY user_id, year, month
)
SELECT 
    cwd.date_id,
    cwd.date,
    cwd.user_id,
    cwd.join_date,
    cwd.adjusted_standard_hours,
    -- Calculate pro-rated standard hours for partial months
    CASE 
        WHEN EXTRACT(YEAR FROM cwd.date) = EXTRACT(YEAR FROM cwd.join_date) 
             AND EXTRACT(MONTH FROM cwd.date) = EXTRACT(MONTH FROM cwd.join_date) THEN
            -- For join month, calculate pro-rated hours
            CASE 
                WHEN mwd.working_days_in_month > 0 THEN
                    -- Pro-rate based on working days in the month
                    (cwd.adjusted_standard_hours * 21.75) / mwd.working_days_in_month
                ELSE 0
            END
        ELSE
            -- For other months, use standard calculation
            cwd.adjusted_standard_hours
    END AS final_standard_hours,
    mwd.working_days_in_month,
    mwd.total_standard_hours_in_month,
    -- Additional metadata
    EXTRACT(YEAR FROM cwd.date) AS year,
    EXTRACT(MONTH FROM cwd.date) AS month,
    EXTRACT(DAY FROM cwd.date) AS day,
    cwd.is_weekend,
    cwd.holiday_name,
    CASE 
        WHEN cwd.date >= cwd.join_date THEN 'Active'
        ELSE 'Not Joined'
    END AS user_status
FROM calendar_with_join_dates cwd
LEFT JOIN monthly_working_days mwd ON 
    cwd.user_id = mwd.user_id AND 
    EXTRACT(YEAR FROM cwd.date) = mwd.year AND 
    EXTRACT(MONTH FROM cwd.date) = mwd.month
WHERE cwd.user_id IS NOT NULL; 