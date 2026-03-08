import cors from "cors";
import express, { Request, Response } from "express";
import multer from "multer";
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

app.use(cors());
app.use(express.json());

app.get("/api/health", (_req: Request, res: Response) => {
  res.json({ ok: true });
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

    res.status(200).json({
      message: `Uploaded ${req.file.originalname} successfully.`,
      file: {
        name: req.file.originalname,
        size: req.file.size,
        type: req.file.mimetype
      }
    });
  });
});

app.use((_req: Request, res: Response) => {
  res.status(404).json({ message: "Not found" });
});

app.listen(port, () => {
  console.log(`Upload API listening on http://localhost:${port}`);
});
