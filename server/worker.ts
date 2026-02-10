import { bundle } from "@remotion/bundler";
import { ensureBrowser, renderMedia, selectComposition } from "@remotion/renderer";
import dotenv from "dotenv";
import fs from "fs/promises";
import fsSync from "fs";
import { Pool } from "pg";
import path from "path";

dotenv.config();

/* ================= CONFIG ================= */

const MAX_PARALLEL = Number(process.env.MAX_PARALLEL || 4);
const SERVE_URL = process.env.REMOTION_SERVE_URL;
// const TEMP_DIR = process.env.TEMP_RENDER_DIR || "/tmp/renders";
const TEMP_DIR = path.resolve(process.env.TEMP_RENDER_DIR || "renders");
const POLL_INTERVAL = Number(process.env.POLL_INTERVAL_MS || 1500);

/* ================= DB ================= */

const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
});

/* ================= STATE ================= */

let activeRenders = 0;
let shuttingDown = false;

/* ================= UTILS ================= */

function sleep(ms: number) {
    return new Promise((res) => setTimeout(res, ms));
}

async function ensureTempDir() {
    if (!fsSync.existsSync(TEMP_DIR)) {
        await fs.mkdir(TEMP_DIR, { recursive: true });
    }
}

/* ================= JOB FETCH ================= */

async function getNextJob() {
    const client = await pool.connect();
    try {
        await client.query("BEGIN");

        const res = await client.query(`
      SELECT *
      FROM render_jobs
      WHERE status = 'queued'
      ORDER BY created_at ASC
      LIMIT 1
      FOR UPDATE SKIP LOCKED
    `);

        if (res.rows.length === 0) {
            await client.query("ROLLBACK");
            return null;
        }

        const job = res.rows[0];

        await client.query(
            `UPDATE render_jobs SET status = 'rendering', started_at = NOW() WHERE id = $1`,
            [job.id]
        );

        await client.query("COMMIT");
        return job;
    } catch (err) {
        await client.query("ROLLBACK");
        throw err;
    } finally {
        client.release();
    }
}

/* ================= RENDER ================= */

async function renderJob(job: any, serveUrl: string) {
    const inputProps = job.input_props;

    console.log('check inputProps', inputProps.customStyleConfigs)

    const composition = await selectComposition({
        serveUrl,
        id: "VideoRenderer",
        inputProps,
    });

    const outputPath = path.join(TEMP_DIR, `${job.id}.mp4`);

    await renderMedia({
        composition,
        serveUrl,
        codec: "h264",
        inputProps,
        outputLocation: outputPath,
        onProgress: async ({ progress }) => {
            await pool.query(
                `UPDATE render_jobs SET progress = $1 WHERE id = $2`,
                [Math.round(progress * 100) / 100, job.id]
            );
        },
    });

    return outputPath;
}

/* ================= STORAGE ================= */

async function uploadToStorage(localFile: string, jobId: string): Promise<string> {
    const { S3Client, PutObjectCommand } = await import("@aws-sdk/client-s3");

    const s3 = new S3Client({
        region: "auto",
        endpoint: process.env.R2_ENDPOINT!,
        credentials: {
            accessKeyId: process.env.R2_ACCESS_KEY_ID!,
            secretAccessKey: process.env.R2_SECRET_ACCESS_KEY!,
        },
    });

    const fileBuffer = await fs.readFile(localFile);
    const key = `renders/${jobId}.mp4`;

    await s3.send(
        new PutObjectCommand({
            Bucket: process.env.R2_BUCKET!,
            Key: key,
            Body: fileBuffer,
            ContentType: "video/mp4",
        })
    );

    // Construct public URL — set this to your R2 custom domain or public bucket URL
    const cdnBase = process.env.CDN_BASE_URL || `${process.env.R2_ENDPOINT}/${process.env.R2_BUCKET}`;
    return `${cdnBase}/${key}`;
}

/* ================= DB UPDATE ================= */

async function completeJob(jobId: string, url: string) {
    await pool.query(
        `UPDATE render_jobs
     SET status = 'completed', output_url = $1, progress = 1, completed_at = NOW()
     WHERE id = $2`,
        [url, jobId]
    );
}

async function failJob(jobId: string, error: any) {
    await pool.query(
        `UPDATE render_jobs SET status = 'failed', error = $1 WHERE id = $2`,
        [String(error), jobId]
    );
}

/* ================= CLEANUP ================= */

async function cleanupFile(filePath: string) {
    // try {
    //     await fs.unlink(filePath);
    // } catch { }

    console.log('clearning yoo')
}

/* ================= PROCESS JOB ================= */

async function processJob(job: any, serveUrl: string) {
    console.log(`🎬 Starting render ${job.id}`);
    let outputFile = "";

    try {
        outputFile = await renderJob(job, serveUrl);
        console.log(`📦 Uploading ${job.id}`);
        const url = await uploadToStorage(outputFile, job.id);
        await completeJob(job.id, url);
        console.log(`✅ Completed ${job.id}`);
    } catch (err) {
        console.error(`❌ Failed ${job.id}`, err);
        await failJob(job.id, err);
    } finally {
        if (outputFile) await cleanupFile(outputFile);
    }
}

/* ================= WORKER LOOP ================= */

async function workerLoop(serveUrl: string) {
    console.log(`🚀 Worker started — max parallel: ${MAX_PARALLEL}`);
    console.log(`🎥 Using serve URL: ${serveUrl}`);

    while (!shuttingDown) {
        if (activeRenders >= MAX_PARALLEL) {
            await sleep(500);
            continue;
        }

        const job = await getNextJob();

        if (!job) {
            await sleep(POLL_INTERVAL);
            continue;
        }

        activeRenders++;
        processJob(job, serveUrl)
            .catch(console.error)
            .finally(() => {
                activeRenders--;
            });
    }
}

/* ================= SHUTDOWN ================= */

function setupShutdown() {
    const shutdown = async () => {
        console.log("🛑 Shutting down...");
        shuttingDown = true;
        while (activeRenders > 0) {
            console.log(`Waiting for ${activeRenders} active renders...`);
            await sleep(1000);
        }
        await pool.end();
        process.exit(0);
    };
    process.on("SIGINT", shutdown);
    process.on("SIGTERM", shutdown);
}

/* ================= MAIN ================= */

async function main() {
    await ensureTempDir();
    setupShutdown();
    await ensureBrowser();

    const serveUrl = SERVE_URL
        ? SERVE_URL
        : await bundle({
            entryPoint: path.resolve("src/remotion/index.ts"),
            onProgress: (progress) => {
                console.log(`📦 Bundling Remotion project: ${progress}%`);
            },
        });

    await workerLoop(serveUrl);
}

main();