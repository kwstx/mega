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

2. Start both server and client:

   ```bash
   npm run dev
   ```

3. Open `http://localhost:5173`

## Scripts

- `npm run dev` - runs server and client in parallel
- `npm run build` - builds server and client
- `npm run typecheck` - runs TypeScript checks

## API

- `POST /api/upload` with multipart form field `bill`
- Accepted formats: PDF and CSV
- Max size: 10 MB
