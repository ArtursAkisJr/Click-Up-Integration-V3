# ClickUp Backend V3

A comprehensive backend system for syncing ClickUp data to Supabase database with PowerBI views and automated workflows.

## Features

- **ClickUp API Integration**: Syncs spaces, team members, folders, lists, tasks, and time entries
- **Supabase Database**: PostgreSQL database with optimized schema and views
- **PowerBI Views**: Pre-configured views in public schema with `clickup_` prefix
- **Duplicate Prevention**: Advanced deduplication for time entries
- **Automated Workflows**: GitHub Actions for daily synchronization
- **National Holidays**: Latvia 2025 holidays integration for calendar views

## Prerequisites

- Python 3.11 or higher
- PostgreSQL database (Supabase recommended)
- ClickUp API access
- Git

## Installation

### 1. Clone the repository

```bash
git clone https://github.com/ArtursAkisJr/Click-Up-Integration-V3.git
cd Click-Up-Integration-V3
```

### 2. Install dependencies

```bash
# Install Python dependencies
pip install -r requirements.txt
```

**Note for Windows users**: If `psycopg2-binary` installation fails, try:
```bash
pip install psycopg2
```

### 3. Set up environment variables

Copy the template environment file and fill in your credentials:

```bash
cp env_template.env env_real.env
```

Edit `env_real.env` with your actual credentials:

```env
# ClickUp API Configuration
CLICKUP_API_KEY=your_clickup_api_key
CLICKUP_TEAM_ID=your_team_id

# Supabase Database Connection
SUPABASE_DB_USER=your_db_user
SUPABASE_DB_PASSWORD=your_db_password
SUPABASE_DB_HOST=your_db_host
SUPABASE_DB_PORT=6543
SUPABASE_DB_NAME=postgres

# API KEY Supabase
SUPABASE_ANON_KEY=your_supabase_anon_key
SUPABASE_SERVICE_ROLE_KEY=your_supabase_service_role_key
```

## Usage

### Initial Setup

Run the Supabase setup script to create all tables and views:

```bash
python supabase_upload/supabase_setup.py
```

### Manual Sync

Run the full sync process:

```bash
python sync_scripts/sync_all.py
```

### Individual Sync Scripts

You can also run individual sync scripts:

```bash
# Sync spaces
python sync_scripts/sync_spaces.py

# Sync team members
python sync_scripts/sync_team_members.py

# Sync folders
python sync_scripts/sync_folders.py

# Sync lists
python sync_scripts/sync_lists.py

# Sync tasks
python sync_scripts/sync_tasks.py

# Sync time entries
python sync_scripts/sync_ytd_time_entries_all_users.py
```

### Cleanup Duplicates

If you need to clean up existing duplicate time entries:

```bash
python supabase_upload/cleanup_duplicate_time_entries.py
```

## Database Schema

### Main Tables (clickup schema)
- `sync_status` - Sync operation tracking
- `spaces` - ClickUp spaces
- `team_members` - Team member information
- `folders` - ClickUp folders
- `lists` - ClickUp lists
- `tasks` - ClickUp tasks with custom fields
- `time_entries` - Time tracking entries
- `national_holidays` - Latvia 2025 holidays

### PowerBI Views (public schema)
- `clickup_dim_Tasks` - Tasks with extracted Account Name
- `clickup_dim_Clients` - Client information
- `clickup_dim_Lists` - All lists
- `clickup_dim_Spaces` - All spaces
- `clickup_dim_Folders` - All folders
- `clickup_dim_Projects` - Project tasks with Account Name
- `clickup_fact_Timesheet` - Time entries
- `clickup_dim_WIP` - Work in progress dimension
- `clickup_dim_Calendar` - Date dimension with holidays
- `clickup_dim_Members` - Team members
- `clickup_fact_StandardHours` - Standard hours calculation for each user-date combination

## Automated Workflows

The project includes GitHub Actions workflows for automated synchronization:

- **Daily Sync**: Runs every day at 2am UTC
- **Manual Trigger**: Can be triggered manually from GitHub Actions

## Testing

Test your connection and setup:

```bash
# Test Supabase database connection
python test_connection/test_supabase_db.py

# Test ClickUp API connection
python test_connection/test_clickup_api.py

# Query time entries
python test_connection/query_time_entries.py
```

## Troubleshooting

### Common Issues

1. **psycopg2 installation fails on Windows**
   - Install Microsoft Visual C++ Build Tools
   - Or use: `pip install psycopg2` instead of `psycopg2-binary`

2. **Database connection errors**
   - Verify your Supabase credentials in `env_real.env`
   - Check if your IP is whitelisted in Supabase

3. **ClickUp API errors**
   - Verify your ClickUp API key and team ID
   - Check API rate limits

4. **Duplicate time entries**
   - Run the cleanup script: `python supabase_upload/cleanup_duplicate_time_entries.py`

## Project Structure

```
ClickUp-Backend-V3/
├── clickup_api/              # ClickUp API scripts
├── supabase_upload/          # Database upload scripts
├── sync_scripts/             # Sync orchestration
├── test_connection/          # Connection testing
├── futurcene_dashboard_v1/   # PowerBI views and reports
├── .github/workflows/        # GitHub Actions
├── requirements.txt          # Python dependencies
├── env_template.env          # Environment template
└── README.md                # This file
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Test thoroughly
5. Submit a pull request

## License

This project is proprietary and confidential.

## Support

For support or questions, please contact the development team. 