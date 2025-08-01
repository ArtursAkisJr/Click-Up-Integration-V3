-- Latvia National Holidays 2025
CREATE TABLE IF NOT EXISTS clickup.national_holidays (
    date date PRIMARY KEY,
    name text
);

INSERT INTO clickup.national_holidays (date, name) VALUES
('2025-01-01', 'New Year''s Day'),
('2025-03-29', 'Good Friday'),
('2025-03-31', 'Easter Monday'),
('2025-05-01', 'Labour Day'),
('2025-05-04', 'Restoration of Independence'),
('2025-05-11', 'Mother''s Day'),
('2025-06-23', 'Līgo Day'),
('2025-06-24', 'Jāņi (Midsummer)'),
('2025-11-18', 'Proclamation Day of the Republic of Latvia'),
('2025-12-24', 'Christmas Eve'),
('2025-12-25', 'Christmas Day'),
('2025-12-26', 'Second Day of Christmas'),
('2025-12-31', 'New Year''s Eve')
ON CONFLICT (date) DO NOTHING; 