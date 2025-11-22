# Public-Transportation-Analysis

A Node.js cron job that fetches vehicle data from the Tranzy API and stores it in a PostgreSQL database.

## Prerequisites

- Node.js (v18 or higher)
- PostgreSQL database
- Tranzy API credentials

## Setup

1. Install dependencies:
```bash
npm install
```

2. Copy the environment file and configure it:
```bash
cp .env.example .env
```

3. Edit `.env` with your actual configuration:
   - Database connection details
   - Tranzy API URL and credentials

4. Set up the database schema:
```bash
psql -U your_db_user -d your_database_name -f schema.sql
```

Or manually run the SQL commands from `schema.sql` in your PostgreSQL database.

## Running

### Development
```bash
npm run dev
```

### Production
```bash
npm start
```

Or build and run:
```bash
npm run build
node dist/main.js
```

## How It Works

- Fetches vehicle data from the Tranzy API every minute
- Upserts vehicle information into the `vehicle` table
- Inserts vehicle state (location, speed, accessibility) into the `vehicle_state` table
- Handles graceful shutdown on SIGTERM/SIGINT signals

## Environment Variables

| Variable | Required | Description |
|----------|----------|-------------|
| `DB_HOST` | Yes | PostgreSQL host |
| `DB_PORT` | Yes | PostgreSQL port |
| `DB_USER` | Yes | Database username |
| `DB_PASSWORD` | Yes | Database password |
| `DB_NAME` | Yes | Database name |
| `TRANZY_API_URL` | Yes | Base URL for Tranzy API |
| `TRANZY_API_KEY` | Yes | API key for authentication |
| `TRANZY_AGENCY_ID` | No | Agency ID (optional) |

