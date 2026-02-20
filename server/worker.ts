import { bundle } from "@remotion/bundler";
import { ensureBrowser, renderMedia, selectComposition } from "@remotion/renderer";
import dotenv from "dotenv";
import fs from "fs/promises";
import fsSync from "fs";
import { Pool, PoolClient } from "pg";
import path from "path";

dotenv.config();

/* ================= CONFIG ================= */
const MAX_PARALLEL = Number(process.env.MAX_PARALLEL || 4);
const SERVE_URL = process.env.REMOTION_SERVE_URL;
const TEMP_DIR = path.resolve(process.env.TEMP_RENDER_DIR || "renders");
const POLL_INTERVAL = Number(process.env.POLL_INTERVAL_MS || 1500);

// Retry configuration
const DB_RETRY_ATTEMPTS = 3;
const DB_RETRY_DELAY_MS = 1000;
const PROGRESS_UPDATE_INTERVAL_MS = 5000; // Throttle DB updates

/* ================= DB ================= */
const pool = new Pool({
    connectionString: process.env.DATABASE_URL,
    max: 20, // Max connections in pool
    idleTimeoutMillis: 30000, // Close idle connections after 30s
    connectionTimeoutMillis: 10000, // 10s timeout for new connections
    // Add these for resilience:
    keepAlive: true,
    keepAliveInitialDelayMillis: 10000,
});

// Handle pool-level errors to prevent crashes
pool.on('error', (err) => {
    console.error('Unexpected PostgreSQL pool error:', err);
    // Don't exit - let the retry logic handle it
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

/* ================= RETRY WRAPPER ================= */
async function withRetry<T>(
    operation: () => Promise<T>,
    context: string,
    maxAttempts: number = DB_RETRY_ATTEMPTS
): Promise<T> {
    let lastError: Error | undefined;
    for (let attempt = 1; attempt <= maxAttempts; attempt++) {
        try {
            return await operation();
        } catch (err) {
            lastError = err as Error;
            console.warn(`⚠️ ${context} failed (attempt ${attempt}/${maxAttempts}):`, (err as Error).message);
            if (attempt < maxAttempts) {
                await sleep(DB_RETRY_DELAY_MS * attempt); // Exponential backoff
            }
        }
    }
    throw new Error(`${context} failed after ${maxAttempts} attempts: ${lastError?.message}`);
}

/* ================= SAFE DB OPERATIONS ================= */
async function getClient(): Promise<PoolClient> {
    return withRetry(
        () => pool.connect(),
        'Database connection'
    );
}

async function safeQuery(text: string, params?: any[]) {
    return withRetry(
        async () => {
            const client = await getClient();
            try {
                return await client.query(text, params);
            } finally {
                client.release();
            }
        },
        'Database query'
    );
}

/* ================= JOB FETCH ================= */
async function getNextJob(): Promise<any> {
    const client = await getClient();
    try {
        // Set a statement timeout for this transaction
        await client.query('SET statement_timeout = 5000');
        await client.query("BEGIN");

        const res = await client.query(`
      SELECT * FROM render_jobs
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
        await client.query("ROLLBACK").catch(() => { }); // Ignore rollback errors
        throw err;
    } finally {
        client.release();
    }
}

/* ================= RENDER ================= */
async function renderJob(job: any, serveUrl: string): Promise<string> {
    const inputProps = job.input_props;

    // console.log('check inputProps', inputProps.customStyleConfigs);
    // console.log('check inputProps', inputProps);

    const composition = await selectComposition({
        serveUrl,
        id: "VideoRenderer",
        inputProps,
    });

    const outputPath = path.join(TEMP_DIR, `${job.id}.mp4`);

    // Throttle progress updates to avoid DB spam
    let lastProgressUpdate = 0;
    let lastProgressValue = 0;

    await renderMedia({
        composition,
        serveUrl,
        codec: "h264",
        inputProps,
        outputLocation: outputPath,
        onProgress: ({ progress }) => {
            const now = Date.now();
            const progressRounded = Math.round(progress * 100) / 100;

            // Only update if significant change or enough time passed
            if (now - lastProgressUpdate > PROGRESS_UPDATE_INTERVAL_MS && progressRounded !== lastProgressValue) {
                lastProgressUpdate = now;
                lastProgressValue = progressRounded;

                // Fire-and-forget with error handling - NEVER throw here
                safeQuery(
                    `UPDATE render_jobs SET progress = $1 WHERE id = $2`,
                    [progressRounded, job.id]
                ).catch(err => {
                    console.error(`Failed to update progress for job ${job.id}:`, err.message);
                    // Continue rendering - don't crash
                });
            }
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

    const cdnBase = process.env.CDN_BASE_URL || `${process.env.R2_ENDPOINT}/${process.env.R2_BUCKET}`;
    // return `${cdnBase}/${key}`;
    return `${key}`;
}

/* ================= CREDIT DEDUCTION ================= */
async function deductUserCredit(userId: number): Promise<void> {
    const client = await getClient();
    try {
        await client.query("BEGIN");

        // Check current credits
        const userResult = await client.query(
            `SELECT credits FROM users WHERE id = $1 FOR UPDATE`,
            [userId]
        );

        if (userResult.rows.length === 0) {
            throw new Error(`User ${userId} not found`);
        }

        const currentCredits = userResult.rows[0].credits;

        if (currentCredits < 1) {
            console.warn(`⚠️ User ${userId} has insufficient credits (${currentCredits}), but video was already rendered`);
            // Still complete the job, but log the issue
        }

        // Deduct 1 credit (prevent going below 0)
        await client.query(
            `UPDATE users 
       SET credits = GREATEST(credits - 1, 0),
           updated_at = NOW()
       WHERE id = $1`,
            [userId]
        );

        await client.query("COMMIT");
        console.log(`💳 Deducted 1 credit from user ${userId} (had ${currentCredits})`);
    } catch (err) {
        await client.query("ROLLBACK").catch(() => { });
        throw err;
    } finally {
        client.release();
    }
}

/* ================= DB UPDATE ================= */
async function completeJob(jobId: string, url: string, userId: number, creditsToDeduct: number) {
    const client = await getClient();
    try {
        await client.query("BEGIN");

        await client.query(
            `UPDATE render_jobs 
       SET status = 'completed', 
           output_url = $1, 
           progress = 1, 
           completed_at = NOW() 
       WHERE id = $2`,
            [url, jobId]
        );

        await client.query(
            `UPDATE users 
       SET credits = GREATEST(credits - $1, 0),
           updated_at = NOW()
       WHERE id = $2`,
            [creditsToDeduct, userId]  // <-- dynamic now
        );

        await client.query("COMMIT");
        console.log(`✅ Job ${jobId} completed and ${creditsToDeduct} credits deducted from user ${userId}`);
    } catch (err) {
        await client.query("ROLLBACK").catch(() => { });
        throw err;
    } finally {
        client.release();
    }
}

async function failJob(jobId: string, error: any) {
    const errorMessage = error instanceof Error ? error.message : String(error);
    // Truncate if too long for DB
    const truncated = errorMessage.slice(0, 1000);

    await safeQuery(
        `UPDATE render_jobs SET status = 'failed', error = $1 WHERE id = $2`,
        [truncated, jobId]
    ).catch(err => {
        console.error(`CRITICAL: Failed to mark job ${jobId} as failed:`, err);
    });
}

/* ================= CLEANUP ================= */
async function cleanupFile(filePath: string) {
    try {
        await fs.unlink(filePath);
        console.log(`🗑️ Cleaned up ${filePath}`);
    } catch (err) {
        // File might not exist, that's fine
        console.log(`⚠️ Could not clean up ${filePath}:`, (err as Error).message);
    }
}

/* ================= PROCESS JOB ================= */
async function processJob(job: any, serveUrl: string) {
    console.log(`🎬 Starting render ${job.id}`);
    let outputFile = "";

    try {
        // Render the video
        outputFile = await renderJob(job, serveUrl);
        console.log(`📦 Uploading ${job.id}`);

        // Upload to R2
        const url = await uploadToStorage(outputFile, job.id);

        // Calculate credits: total_video_length_in_minutes * 2, rounded up
        const { durationInFrames, fps } = job.input_props.videoInfo;
        const durationInMinutes = durationInFrames / fps / 60;
        const creditsToDeduct = Math.ceil(durationInMinutes * 2);

        console.log(`💳 Video duration: ${(durationInMinutes * 60).toFixed(1)}s → deducting ${creditsToDeduct} credits`);

        await completeJob(job.id, url, job.user_id, creditsToDeduct);

        console.log(`✅ Completed ${job.id}`);
    } catch (err) {
        console.error(`❌ Failed ${job.id}`, err);
        await failJob(job.id, err);
        // Note: We don't deduct credits on failure
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

        let job: any;
        try {
            job = await getNextJob();
        } catch (err) {
            console.error('Failed to fetch job:', err);
            await sleep(POLL_INTERVAL * 2); // Wait longer on error
            continue;
        }

        if (!job) {
            await sleep(POLL_INTERVAL);
            continue;
        }

        activeRenders++;

        // Run job in background with isolated error handling
        processJob(job, serveUrl)
            .catch(err => {
                console.error(`Unhandled error in processJob for ${job?.id}:`, err);
            })
            .finally(() => {
                activeRenders--;
            });
    }
}

/* ================= SHUTDOWN ================= */
function setupShutdown() {
    const shutdown = async (signal: string) => {
        console.log(`🛑 Received ${signal}, shutting down...`);
        shuttingDown = true; // Stop accepting new jobs immediately

        const gracefulShutdown = async () => {
            while (activeRenders > 0) {
                console.log(`Waiting for ${activeRenders} active renders...`);
                await sleep(1000);
            }

            try {
                await pool.end();
                console.log('Database pool closed');
            } catch (err) {
                console.error('Error closing pool:', err);
            }

            process.exit(0);
        };

        // Force exit after 30s
        const forceExit = setTimeout(() => {
            console.error('Force exiting after timeout');
            process.exit(1);
        }, 30000);

        await gracefulShutdown();
        clearTimeout(forceExit);
    };

    process.on("SIGINT", () => shutdown("SIGINT"));
    process.on("SIGTERM", () => shutdown("SIGTERM"));

    // Catch unhandled rejections
    process.on('unhandledRejection', (reason, promise) => {
        console.error('Unhandled Rejection at:', promise, 'reason:', reason);
        // Don't exit - log and continue
    });

    // Catch uncaught exceptions
    process.on('uncaughtException', (err) => {
        console.error('Uncaught Exception:', err);

        const isRecoverableConnectionError =
            err.message.includes('Connection terminated unexpectedly') ||
            err.message.includes('terminating connection') ||
            err.message.includes('ECONNRESET') ||
            err.message.includes('EPIPE');

        if (isRecoverableConnectionError) {
            console.warn('⚠️ Recoverable connection error, continuing...');
            return; // DO NOT shut down
        }

        // Only shut down on truly unexpected errors
        shutdown('uncaughtException').catch(() => process.exit(1));
    });
}

/* ================= HEALTH CHECK ================= */
async function healthCheck() {
    try {
        await pool.query('SELECT 1');
        return true;
    } catch {
        return false;
    }
}

/* ================= MAIN ================= */
async function main() {
    await ensureTempDir();
    setupShutdown();

    // Verify DB connection before starting
    console.log('Checking database connection...');
    if (!await healthCheck()) {
        console.error('Database not reachable, exiting');
        process.exit(1);
    }
    console.log('Database connected');

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

main().catch(err => {
    console.error('Fatal error in main:', err);
    process.exit(1);
});