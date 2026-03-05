import express from "express";
import {
  S3Client,
  GetObjectCommand,
  PutObjectCommand,
  DeleteObjectCommand,
} from "@aws-sdk/client-s3";
import { spawn } from "child_process";
import { createWriteStream, createReadStream } from "fs";
import { mkdir, rm, readdir } from "fs/promises";
import { join, extname } from "path";
import { tmpdir } from "os";
import { randomUUID } from "crypto";
import { pipeline } from "stream/promises";

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

const {
  R2_BUCKET,
  R2_ENDPOINT,
  R2_ACCESS_KEY,
  R2_SECRET_KEY,
  R2_PUBLIC_BASE,
  PORT = "3000",
} = process.env;

const REQUIRED_ENV = [
  "R2_BUCKET",
  "R2_ENDPOINT",
  "R2_ACCESS_KEY",
  "R2_SECRET_KEY",
  "R2_PUBLIC_BASE",
];

for (const key of REQUIRED_ENV) {
  if (!process.env[key]) {
    console.error(`Missing required environment variable: ${key}`);
    process.exit(1);
  }
}

const s3 = new S3Client({
  region: "auto",
  endpoint: R2_ENDPOINT,
  credentials: {
    accessKeyId: R2_ACCESS_KEY,
    secretAccessKey: R2_SECRET_KEY,
  },
});

// ---------------------------------------------------------------------------
// App
// ---------------------------------------------------------------------------

const app = express();
app.use(express.json());

app.post("/encode", encode);

app.listen(Number(PORT), () => {
  console.log(`Encoding service listening on port ${PORT}`);
});

// ---------------------------------------------------------------------------
// Route handler
// ---------------------------------------------------------------------------

async function encode(req, res) {
  const { videoId } = req.body;

  if (!videoId || typeof videoId !== "string" || !videoId.trim()) {
    return res.status(400).json({ error: "videoId is required" });
  }

  const safeId = videoId.trim();

  // Reject path-traversal attempts
  if (safeId.includes("/") || safeId.includes("..")) {
    return res.status(400).json({ error: "Invalid videoId" });
  }

  const workDir = join(tmpdir(), `encode-${safeId}-${randomUUID()}`);

  try {
    await mkdir(workDir, { recursive: true });

    // 1. Download source MP4 from R2
    const srcKey = `uploads/${safeId}.mp4`;
    const localMp4 = join(workDir, "input.mp4");
    await downloadFromR2(srcKey, localMp4);

    // 2. Run FFmpeg: MP4 → HLS
    const outputDir = join(workDir, "hls");
    await mkdir(outputDir);
    await runFfmpeg(localMp4, outputDir);

    // 3. Upload HLS files to R2
    const hlsFiles = await readdir(outputDir);
    await Promise.all(
      hlsFiles.map((file) => {
        const localPath = join(outputDir, file);
        const r2Key = `streams/${safeId}/${file}`;
        return uploadToR2(localPath, r2Key, contentType(file));
      })
    );

    // 4. Delete source MP4 from R2
    await deleteFromR2(srcKey);

    // 5. Clean up local temp directory
    await rm(workDir, { recursive: true, force: true });

    return res.json({
      status: "complete",
      playlistUrl: `${R2_PUBLIC_BASE}/streams/${safeId}/master.m3u8`,
    });
  } catch (err) {
    // Best-effort cleanup on error
    await rm(workDir, { recursive: true, force: true }).catch(() => {});

    console.error(`[encode] Error for videoId=${safeId}:`, err);

    if (err.name === "NoSuchKey") {
      return res
        .status(404)
        .json({ error: `Source file not found: uploads/${safeId}.mp4` });
    }

    return res.status(500).json({ error: "Encoding failed" });
  }
}

// ---------------------------------------------------------------------------
// R2 helpers
// ---------------------------------------------------------------------------

async function downloadFromR2(key, destPath) {
  const cmd = new GetObjectCommand({ Bucket: R2_BUCKET, Key: key });
  const { Body } = await s3.send(cmd);
  await pipeline(Body, createWriteStream(destPath));
}

async function uploadToR2(localPath, key, mimeType) {
  const cmd = new PutObjectCommand({
    Bucket: R2_BUCKET,
    Key: key,
    Body: createReadStream(localPath),
    ContentType: mimeType,
  });
  await s3.send(cmd);
}

async function deleteFromR2(key) {
  const cmd = new DeleteObjectCommand({ Bucket: R2_BUCKET, Key: key });
  await s3.send(cmd);
}

// ---------------------------------------------------------------------------
// FFmpeg
// ---------------------------------------------------------------------------

function runFfmpeg(inputPath, outputDir) {
  return new Promise((resolve, reject) => {
    const playlistPath = join(outputDir, "master.m3u8");
    const segmentPattern = join(outputDir, "segment_%03d.ts");

    const args = [
      "-i", inputPath,
      // Video
      "-c:v", "libx264",
      "-preset", "veryfast",
      "-crf", "23",
      "-profile:v", "main",
      "-level", "3.1",
      // Audio
      "-c:a", "aac",
      "-b:a", "128k",
      "-ar", "44100",
      // HLS
      "-f", "hls",
      "-hls_time", "6",
      "-hls_playlist_type", "vod",
      "-hls_segment_filename", segmentPattern,
      playlistPath,
    ];

    const ff = spawn("ffmpeg", args, { stdio: ["ignore", "pipe", "pipe"] });

    let stderr = "";
    ff.stderr.on("data", (chunk) => (stderr += chunk.toString()));

    ff.on("close", (code) => {
      if (code === 0) {
        resolve();
      } else {
        const err = new Error(`FFmpeg exited with code ${code}`);
        err.ffmpegStderr = stderr;
        reject(err);
      }
    });

    ff.on("error", (err) => {
      err.message = `Failed to spawn FFmpeg: ${err.message}`;
      reject(err);
    });
  });
}

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

function contentType(filename) {
  const ext = extname(filename).toLowerCase();
  if (ext === ".m3u8") return "application/vnd.apple.mpegurl";
  if (ext === ".ts") return "video/mp2t";
  return "application/octet-stream";
}
