import { useMemo, useState } from "react";

const MAX_FILE_SIZE = 10 * 1024 * 1024;
const ALLOWED_TYPES = ["application/pdf", "text/csv"];
const ALLOWED_EXTENSIONS = [".pdf", ".csv"];

type UploadState = "idle" | "uploading" | "success" | "error";

function hasAllowedExtension(filename: string): boolean {
  const lower = filename.toLowerCase();
  return ALLOWED_EXTENSIONS.some((ext) => lower.endsWith(ext));
}

function validateFile(file: File): string | null {
  const typeIsValid = ALLOWED_TYPES.includes(file.type) || hasAllowedExtension(file.name);
  if (!typeIsValid) {
    return "Only PDF or CSV files are allowed.";
  }

  if (file.size > MAX_FILE_SIZE) {
    return "File is too large. Maximum size is 10 MB.";
  }

  return null;
}

function uploadFile(file: File, onProgress: (value: number) => void): Promise<{ message: string }> {
  return new Promise((resolve, reject) => {
    const formData = new FormData();
    formData.append("bill", file);

    const xhr = new XMLHttpRequest();
    xhr.open("POST", "/api/upload");

    xhr.upload.onprogress = (event) => {
      if (event.lengthComputable) {
        onProgress(Math.round((event.loaded / event.total) * 100));
      }
    };

    xhr.onload = () => {
      if (xhr.status >= 200 && xhr.status < 300) {
        resolve(JSON.parse(xhr.responseText));
      } else {
        const payload = JSON.parse(xhr.responseText || "{}");
        reject(new Error(payload.message || "Upload failed."));
      }
    };

    xhr.onerror = () => reject(new Error("Network error while uploading."));
    xhr.send(formData);
  });
}

export default function App() {
  const [selectedFile, setSelectedFile] = useState<File | null>(null);
  const [errorMessage, setErrorMessage] = useState<string>("");
  const [successMessage, setSuccessMessage] = useState<string>("");
  const [uploadState, setUploadState] = useState<UploadState>("idle");
  const [progress, setProgress] = useState(0);

  const helperText = useMemo(() => {
    if (selectedFile) {
      return `${selectedFile.name} (${(selectedFile.size / (1024 * 1024)).toFixed(2)} MB)`;
    }
    return "Upload one electricity bill in PDF or CSV format (max 10 MB).";
  }, [selectedFile]);

  const onFileChange = (file: File | null) => {
    setErrorMessage("");
    setSuccessMessage("");
    setProgress(0);

    if (!file) {
      setSelectedFile(null);
      setUploadState("idle");
      return;
    }

    const validationError = validateFile(file);
    if (validationError) {
      setSelectedFile(null);
      setUploadState("error");
      setErrorMessage(validationError);
      return;
    }

    setSelectedFile(file);
    setUploadState("idle");
  };

  const handleSubmit = async (event: React.FormEvent) => {
    event.preventDefault();

    if (!selectedFile) {
      setUploadState("error");
      setErrorMessage("Please select a file before uploading.");
      return;
    }

    setUploadState("uploading");
    setErrorMessage("");
    setSuccessMessage("");
    setProgress(0);

    try {
      const result = await uploadFile(selectedFile, setProgress);
      setUploadState("success");
      setSuccessMessage(result.message || "Upload complete.");
    } catch (error) {
      setUploadState("error");
      setErrorMessage(error instanceof Error ? error.message : "Unknown upload error.");
    }
  };

  return (
    <main className="page">
      <section className="card">
        <h1>Electricity Bill Upload</h1>
        <p className="subtitle">Submit your latest bill for processing.</p>

        <form onSubmit={handleSubmit} className="form" noValidate>
          <label className="upload-input" htmlFor="bill-file">
            <span>Select file</span>
            <input
              id="bill-file"
              type="file"
              accept=".pdf,.csv,application/pdf,text/csv"
              onChange={(event) => onFileChange(event.target.files?.[0] ?? null)}
              disabled={uploadState === "uploading"}
            />
          </label>

          <p className="helper">{helperText}</p>

          <button type="submit" disabled={uploadState === "uploading" || !selectedFile}>
            {uploadState === "uploading" ? "Uploading..." : "Upload bill"}
          </button>
        </form>

        {uploadState === "uploading" && (
          <div className="progress-wrap" aria-live="polite">
            <div className="progress-track" role="progressbar" aria-valuenow={progress} aria-valuemin={0} aria-valuemax={100}>
              <div className="progress-fill" style={{ width: `${progress}%` }} />
            </div>
            <p className="progress-label">{progress}%</p>
          </div>
        )}

        {successMessage && <p className="message success">{successMessage}</p>}
        {errorMessage && <p className="message error">{errorMessage}</p>}
      </section>
    </main>
  );
}
