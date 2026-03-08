# Electricity Bill Uploader

Node.js + TypeScript + React web interface for uploading electricity bill files in PDF or CSV format.

## Features

- Client-side validation for file type (`.pdf`, `.csv`) and max size (10 MB)
- Upload progress indicator with percentage
- Success and error states with clear user messages
- Responsive layout for desktop and mobile
- Express API endpoint to validate and accept uploads

## Run locally

1. Install dependencies:

   ```bash
   npm install
   ```

2. Install Python dependencies for file extraction:

   ```bash
   pip install -r server/python-requirements.txt
   ```

3. Start both server and client:

   ```bash
   npm run dev
   ```

4. Open `http://localhost:5173`

## Scripts

- `npm run dev` - runs server and client in parallel
- `npm run build` - builds server and client
- `npm run typecheck` - runs TypeScript checks

## Generate pandas aggregates

After normalized records are stored in `server/data/bill_store.json`, run:

```bash
cd server
python scripts/aggregate_normalized_data.py --input data/bill_store.json --output data/bill_aggregates.json
```

This writes JSON aggregates for daily, weekly, and monthly usage, plus hourly averages, peak hours, and low-usage periods.

## API

- `POST /api/upload` with multipart form field `bill`
- Accepted formats: PDF and CSV
- Max size: 10 MB
- Response includes `extracted` normalized JSON:
  - `data.date` (ISO date if found)
  - `data.total_kwh_consumed` (number)
  - `data.cost` (number)
  - `data.appliance_usage` (array when available)
  - `data.issues` (warnings about missing/malformed data)
