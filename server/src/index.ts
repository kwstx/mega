import cors from "cors";
import express, { Request, Response } from "express";
import multer from "multer";
import { mkdir, mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { spawn } from "node:child_process";
import { randomUUID } from "node:crypto";
import os from "node:os";
import path from "node:path";

const app = express();
const port = process.env.PORT ? Number(process.env.PORT) : 3001;

const maxFileSize = 10 * 1024 * 1024;
const allowedMimeTypes = new Set(["application/pdf", "text/csv", "application/vnd.ms-excel"]);
const allowedExtensions = new Set([".pdf", ".csv"]);

const storage = multer.memoryStorage();

const upload = multer({
  storage,
  limits: { fileSize: maxFileSize },
  fileFilter: (req, file, callback) => {
    const ext = path.extname(file.originalname).toLowerCase();
    const extensionAllowed = allowedExtensions.has(ext);
    const mimeAllowed = allowedMimeTypes.has(file.mimetype);

    if (extensionAllowed || mimeAllowed) {
      callback(null, true);
      return;
    }

    callback(new Error("Invalid file type. Only PDF or CSV is allowed."));
  }
});

type ExtractedBillData = {
  source_file: string;
  file_type: string;
  extracted_at: string;
  data: {
    date: string | null;
    total_kwh_consumed: number | null;
    cost: number | null;
    currency: string | null;
    appliance_usage: Array<{
      appliance: string;
      kwh: number | null;
      cost: number | null;
    }>;
    issues: string[];
  };
};

type NormalizedBillRecord = {
  id: string;
  schema_version: string;
  record_version: number;
  normalizer_version: string;
  ingested_at: string;
  source_file: string;
  file_type: string;
  extracted_at: string;
  data: {
    bill_date: string | null;
    total_kwh_consumed: number | null;
    total_wh_consumed: number | null;
    cost: number | null;
    currency: string | null;
    appliance_usage: Array<{
      appliance: string;
      kwh: number | null;
      wh: number | null;
      cost: number | null;
    }>;
    issues: string[];
  };
};

type AggregateBucket = {
  period_key: string;
  count: number;
  total_kwh_consumed: number;
  total_cost: number;
  currency_breakdown: Record<string, number>;
  last_updated_at: string;
};

type JsonDataStore = {
  metadata: {
    schema_version: string;
    normalizer_version: string;
    created_at: string;
    updated_at: string;
    total_records: number;
  };
  records: NormalizedBillRecord[];
  indexes: {
    daily: Record<string, AggregateBucket>;
    weekly: Record<string, AggregateBucket>;
    monthly: Record<string, AggregateBucket>;
  };
};

const SCHEMA_VERSION = "1.0.0";
const NORMALIZER_VERSION = "1.0.0";
const dataStorePath = path.resolve(process.cwd(), "data", "bill_store.json");

const nowIso = (): string => new Date().toISOString();

const round = (value: number, decimals: number): number => {
  const factor = 10 ** decimals;
  return Math.round(value * factor) / factor;
};

const normalizeNumber = (value: number | null | undefined, decimals: number): number | null => {
  if (typeof value !== "number" || Number.isNaN(value)) {
    return null;
  }
  return round(value, decimals);
};

const normalizeDateOnly = (value: string | null | undefined): string | null => {
  if (!value) {
    return null;
  }
  const trimmed = value.trim();
  if (!trimmed) {
    return null;
  }
  if (/^\d{4}-\d{2}-\d{2}$/.test(trimmed)) {
    return trimmed;
  }
  const parsed = new Date(trimmed);
  if (Number.isNaN(parsed.getTime())) {
    return null;
  }
  return parsed.toISOString().slice(0, 10);
};

const normalizeTimestamp = (value: string | null | undefined): string => {
  if (!value) {
    return nowIso();
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.getTime())) {
    return nowIso();
  }
  return parsed.toISOString();
};

const normalizeCurrency = (value: string | null | undefined): string | null => {
  if (!value) {
    return null;
  }
  const clean = value.trim().toUpperCase();
  return clean.length === 3 ? clean : null;
};

const isoWeekKey = (dateOnly: string): string => {
  const date = new Date(`${dateOnly}T00:00:00.000Z`);
  const weekday = (date.getUTCDay() + 6) % 7;
  date.setUTCDate(date.getUTCDate() - weekday + 3);
  const firstThursday = new Date(Date.UTC(date.getUTCFullYear(), 0, 4));
  const firstWeekday = (firstThursday.getUTCDay() + 6) % 7;
  firstThursday.setUTCDate(firstThursday.getUTCDate() - firstWeekday + 3);
  const diffMs = date.getTime() - firstThursday.getTime();
  const week = 1 + Math.round(diffMs / (7 * 24 * 60 * 60 * 1000));
  return `${date.getUTCFullYear()}-W${String(week).padStart(2, "0")}`;
};

const normalizeExtractedData = (extracted: ExtractedBillData): NormalizedBillRecord => {
  const billDate = normalizeDateOnly(extracted.data.date);
  const normalizedAppliances = extracted.data.appliance_usage.map((entry) => {
    const kwh = normalizeNumber(entry.kwh, 3);
    return {
      appliance: entry.appliance.trim(),
      kwh,
      wh: kwh === null ? null : round(kwh * 1000, 1),
      cost: normalizeNumber(entry.cost, 2)
    };
  });

  const totalKwh = normalizeNumber(extracted.data.total_kwh_consumed, 3);
  return {
    id: randomUUID(),
    schema_version: SCHEMA_VERSION,
    record_version: 1,
    normalizer_version: NORMALIZER_VERSION,
    ingested_at: nowIso(),
    source_file: extracted.source_file,
    file_type: extracted.file_type.toLowerCase(),
    extracted_at: normalizeTimestamp(extracted.extracted_at),
    data: {
      bill_date: billDate,
      total_kwh_consumed: totalKwh,
      total_wh_consumed: totalKwh === null ? null : round(totalKwh * 1000, 1),
      cost: normalizeNumber(extracted.data.cost, 2),
      currency: normalizeCurrency(extracted.data.currency),
      appliance_usage: normalizedAppliances,
      issues: [...new Set(extracted.data.issues.map((issue) => issue.trim()).filter(Boolean))]
    }
  };
};

const buildEmptyStore = (): JsonDataStore => {
  const timestamp = nowIso();
  return {
    metadata: {
      schema_version: SCHEMA_VERSION,
      normalizer_version: NORMALIZER_VERSION,
      created_at: timestamp,
      updated_at: timestamp,
      total_records: 0
    },
    records: [],
    indexes: {
      daily: {},
      weekly: {},
      monthly: {}
    }
  };
};

const loadStore = async (): Promise<JsonDataStore> => {
  try {
    const raw = await readFile(dataStorePath, "utf8");
    return JSON.parse(raw) as JsonDataStore;
  } catch {
    return buildEmptyStore();
  }
};

const updateAggregateBucket = (
  container: Record<string, AggregateBucket>,
  periodKey: string,
  record: NormalizedBillRecord
): void => {
  const existing = container[periodKey];
  const totalKwh = record.data.total_kwh_consumed ?? 0;
  const totalCost = record.data.cost ?? 0;
  const currency = record.data.currency ?? "UNKNOWN";

  if (!existing) {
    container[periodKey] = {
      period_key: periodKey,
      count: 1,
      total_kwh_consumed: round(totalKwh, 3),
      total_cost: round(totalCost, 2),
      currency_breakdown: { [currency]: round(totalCost, 2) },
      last_updated_at: nowIso()
    };
    return;
  }

  existing.count += 1;
  existing.total_kwh_consumed = round(existing.total_kwh_consumed + totalKwh, 3);
  existing.total_cost = round(existing.total_cost + totalCost, 2);
  existing.currency_breakdown[currency] = round((existing.currency_breakdown[currency] ?? 0) + totalCost, 2);
  existing.last_updated_at = nowIso();
};

const appendRecordToStore = (store: JsonDataStore, record: NormalizedBillRecord): JsonDataStore => {
  const billingDate = record.data.bill_date ?? record.ingested_at.slice(0, 10);
  const weeklyKey = isoWeekKey(billingDate);
  const monthlyKey = billingDate.slice(0, 7);

  store.records.push(record);
  updateAggregateBucket(store.indexes.daily, billingDate, record);
  updateAggregateBucket(store.indexes.weekly, weeklyKey, record);
  updateAggregateBucket(store.indexes.monthly, monthlyKey, record);

  store.metadata.schema_version = SCHEMA_VERSION;
  store.metadata.normalizer_version = NORMALIZER_VERSION;
  store.metadata.total_records = store.records.length;
  store.metadata.updated_at = nowIso();

  return store;
};

const persistNormalizedRecord = async (record: NormalizedBillRecord): Promise<void> => {
  const store = await loadStore();
  const updatedStore = appendRecordToStore(store, record);
  await mkdir(path.dirname(dataStorePath), { recursive: true });
  await writeFile(dataStorePath, JSON.stringify(updatedStore, null, 2), "utf8");
};

const pythonExecutable = process.env.PYTHON_EXECUTABLE ?? "python";
const extractorScriptPath = path.resolve(process.cwd(), "scripts", "extract_bill_data.py");

const runPythonExtractor = async (file: Express.Multer.File): Promise<ExtractedBillData> => {
  const tempDirectory = await mkdtemp(path.join(os.tmpdir(), "bill-upload-"));
  const tempFilePath = path.join(tempDirectory, file.originalname);

  await writeFile(tempFilePath, file.buffer);

  try {
    const result = await new Promise<ExtractedBillData>((resolve, reject) => {
      const child = spawn(pythonExecutable, [extractorScriptPath, tempFilePath], {
        cwd: process.cwd()
      });

      let stdout = "";
      let stderr = "";

      child.stdout.on("data", (chunk: Buffer) => {
        stdout += chunk.toString();
      });

      child.stderr.on("data", (chunk: Buffer) => {
        stderr += chunk.toString();
      });

      child.on("error", (error) => {
        reject(error);
      });

      child.on("close", (code) => {
        if (code !== 0) {
          reject(new Error(stderr.trim() || stdout.trim() || "Python extractor failed."));
          return;
        }

        try {
          const parsed = JSON.parse(stdout.trim()) as ExtractedBillData | { error: string };
          if ("error" in parsed) {
            reject(new Error(parsed.error));
            return;
          }
          resolve(parsed);
        } catch {
          reject(new Error("Extractor returned invalid JSON."));
        }
      });
    });

    return result;
  } finally {
    await rm(tempDirectory, { recursive: true, force: true });
  }
};

app.use(cors());
app.use(express.json());

app.get("/api/health", (_req: Request, res: Response) => {
  res.json({ ok: true });
});

app.get("/api/aggregations", async (req: Request, res: Response) => {
  const period = String(req.query.period ?? "daily").toLowerCase();
  if (!["daily", "weekly", "monthly"].includes(period)) {
    res.status(400).json({ message: "Invalid period. Use daily, weekly, or monthly." });
    return;
  }

  const key = typeof req.query.key === "string" ? req.query.key : null;

  try {
    const store = await loadStore();
    const buckets = store.indexes[period as "daily" | "weekly" | "monthly"];

    if (key) {
      res.status(200).json({
        metadata: store.metadata,
        period,
        key,
        aggregation: buckets[key] ?? null
      });
      return;
    }

    res.status(200).json({
      metadata: store.metadata,
      period,
      aggregations: Object.values(buckets)
    });
  } catch {
    res.status(500).json({ message: "Failed to load aggregations." });
  }
});

app.post("/api/upload", (req: Request, res: Response) => {
  upload.single("bill")(req, res, (error?: unknown) => {
    if (error instanceof multer.MulterError && error.code === "LIMIT_FILE_SIZE") {
      res.status(400).json({ message: "File is too large. Maximum size is 10 MB." });
      return;
    }

    if (error instanceof Error) {
      res.status(400).json({ message: error.message });
      return;
    }

    if (!req.file) {
      res.status(400).json({ message: "No file was uploaded." });
      return;
    }

    runPythonExtractor(req.file)
      .then(async (extracted) => {
        const normalizedRecord = normalizeExtractedData(extracted);
        await persistNormalizedRecord(normalizedRecord);

        res.status(200).json({
          message: `Uploaded ${req.file?.originalname} successfully.`,
          file: {
            name: req.file?.originalname,
            size: req.file?.size,
            type: req.file?.mimetype
          },
          extracted,
          normalized: normalizedRecord
        });
      })
      .catch((extractError: unknown) => {
        const message = extractError instanceof Error ? extractError.message : "Failed to extract bill data.";
        res.status(422).json({
          message,
          file: {
            name: req.file?.originalname,
            size: req.file?.size,
            type: req.file?.mimetype
          }
        });
      });
  });
});

app.use((_req: Request, res: Response) => {
  res.status(404).json({ message: "Not found" });
});

app.listen(port, () => {
  console.log(`Upload API listening on http://localhost:${port}`);
});
