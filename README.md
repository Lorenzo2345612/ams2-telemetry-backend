# AMS2 Telemetry API

FastAPI-based REST API for processing and storing Automobilista 2 (AMS2) telemetry data with PostgreSQL, Redis, and S3-compatible storage.

## Architecture

### Components

1. **API Service** - FastAPI application handling HTTP requests
2. **Worker Service** - RQ workers processing race data in background
3. **PostgreSQL** - Database for race and lap metadata
4. **Redis** - Queue backend for RQ (background jobs)
5. **MinIO** - S3-compatible object storage for telemetry files

### Data Flow

1. Client uploads compressed race data → API receives and validates
2. API saves compressed file to S3 → Creates Race record (Processing status)
3. API enqueues background job → Returns immediately with race_id
4. Worker picks up job → Downloads, decompresses, parses data
5. Worker saves lap data to S3 → Creates Lap records with UUIDs
6. Worker updates Race status → Ready or Failed

## Quick Start

### Prerequisites

- Docker and Docker Compose
- (Optional) Python 3.11+ for local development

### Running with Docker Compose

1. **Clone and navigate to the API directory**:
   ```bash
   cd analytics/api
   ```

2. **Copy environment file**:
   ```bash
   cp .env.example .env
   # Edit .env if needed (defaults work for local development)
   ```

3. **Start all services**:
   ```bash
   docker-compose up -d
   ```

4. **Check service status**:
   ```bash
   docker-compose ps
   ```

5. **View logs**:
   ```bash
   # All services
   docker-compose logs -f

   # Specific service
   docker-compose logs -f api
   docker-compose logs -f worker
   ```

### Services & Ports

- **API**: http://localhost:8000
- **API Docs**: http://localhost:8000/docs
- **PostgreSQL**: localhost:5432
- **Redis**: localhost:6379
- **MinIO API**: http://localhost:9000
- **MinIO Console**: http://localhost:9001 (login: minioadmin/minioadmin)

## API Endpoints

### Upload Race Data
```bash
POST /race/upload
Content-Type: application/json

{
  "data": "<base64-encoded-compressed-data>"
}

Response:
{
  "race_id": "uuid",
  "status": "Processing",
  "job_id": "rq-job-id",
  "message": "Race data uploaded successfully. Processing in background."
}
```

### Check Race Status
```bash
GET /race/{race_id}/status

Response:
{
  "race_id": "uuid",
  "status": "Ready|Processing|Failed",
  "created_at": "2024-01-04T12:00:00",
  "updated_at": "2024-01-04T12:05:00",
  "raw_data_path": "s3://...",
  "laps_count": 15
}
```

### List All Races
```bash
GET /race/list_ids

Response: ["race_id_1", "race_id_2", ...]
```

## Database Schema

### Race Table
- `race_id` (UUID, PK) - Unique race identifier
- `created_at` (DateTime) - Upload timestamp
- `updated_at` (DateTime) - Last modification
- `status` (Enum) - Processing | Ready | Failed
- `raw_data_path` (String) - S3 path to compressed file

### Lap Table
- `id` (Integer, PK) - Auto-increment ID
- `lap_uuid` (UUID, Unique) - Unique lap identifier
- `race_id` (UUID, FK) - References Race
- `lap_number` (Integer) - Lap number in race
- `raw_data_path` (String, Optional) - S3 path to raw lap data
- `processed_data_path` (String) - S3 path to numpy array (.npy)

## S3 Storage Structure

```
s3://ams2-telemetry/
├── races/
│   └── {race_id}/
│       ├── raw_data.deflate
│       └── laps/
│           ├── {lap_uuid_1}.npy
│           ├── {lap_uuid_2}.npy
│           └── ...
```

## Development

### Local Development (without Docker)

1. **Install dependencies**:
   ```bash
   pip install -r requirements.txt
   ```

2. **Start required services**:
   ```bash
   # PostgreSQL, Redis, MinIO need to be running
   docker-compose up -d postgres redis minio
   ```

3. **Run database migrations**:
   ```bash
   # Tables are auto-created on first API startup
   ```

4. **Run API**:
   ```bash
   uvicorn main:app --reload
   ```

5. **Run worker** (in separate terminal):
   ```bash
   rq worker race_processing --with-scheduler
   ```

### Scaling Workers

To run multiple workers for parallel processing:

```bash
docker-compose up -d --scale worker=4
```

## Monitoring

### RQ Dashboard (Optional)

Install and run RQ Dashboard to monitor jobs:

```bash
pip install rq-dashboard
rq-dashboard --redis-url redis://localhost:6379
```

Access at: http://localhost:9181

### Database Access

```bash
# Connect to PostgreSQL
docker-compose exec postgres psql -U ams2_user -d ams2_telemetry

# View races
SELECT race_id, status, created_at FROM races;

# View laps
SELECT lap_uuid, race_id, lap_number FROM laps;
```

## Troubleshooting

### Worker not processing jobs

1. Check worker logs: `docker-compose logs -f worker`
2. Verify Redis connection: `docker-compose exec redis redis-cli ping`
3. Check queue: `docker-compose exec redis redis-cli LLEN rq:queue:race_processing`

### S3 connection errors

1. Check MinIO is running: `docker-compose ps minio`
2. Access MinIO console: http://localhost:9001
3. Verify bucket exists (will be auto-created on first upload)

### Database connection errors

1. Check PostgreSQL: `docker-compose ps postgres`
2. Verify credentials in .env match docker-compose.yml
3. Check database exists: `docker-compose exec postgres psql -U ams2_user -l`

## Stopping Services

```bash
# Stop all services
docker-compose down

# Stop and remove volumes (WARNING: deletes all data)
docker-compose down -v
```

## Environment Variables

See `.env.example` for all configuration options.

Key variables:
- `DATABASE_URL` - PostgreSQL connection string
- `REDIS_HOST` - Redis server hostname
- `S3_BUCKET_NAME` - S3 bucket name
- `S3_ENDPOINT_URL` - S3 endpoint (MinIO or AWS)
- `AWS_ACCESS_KEY_ID` - S3 access key
- `AWS_SECRET_ACCESS_KEY` - S3 secret key
