import express from "express";
import { makeRenderQueue } from "./render-queue";
import { bundle } from "@remotion/bundler";
import path from "node:path";
import { ensureBrowser } from "@remotion/renderer";

const { PORT = 3002, REMOTION_SERVE_URL } = process.env;

function setupApp({ remotionBundleUrl }: { remotionBundleUrl: string }) {
  const app = express();

  const rendersDir = path.resolve("renders");

  const queue = makeRenderQueue({
    port: Number(PORT),
    serveUrl: remotionBundleUrl,
    rendersDir,
  });

  // Host renders on /renders
  app.use("/renders", express.static(rendersDir));
  app.use(express.json());

  // Endpoint to create a new job
  app.post("/renders", async (req, res) => {
    const {
      style = "basic",
      captionPadding = 540,
      customStyleConfigs = {},
      transcript = [],
      videoUrl = "",
      videoInfo = { width: 0, height: 0, durationInFrames: 0, fps: 30 },
    } = req.body || {};

    // Validation
    if (typeof style !== "string") {
      res.status(400).json({ message: "style must be a string" });
      return;
    }

    if (typeof captionPadding !== "number") {
      res.status(400).json({ message: "captionPadding must be a number" });
      return;
    }

    if (!Array.isArray(transcript)) {
      res.status(400).json({ message: "transcript must be an array" });
      return;
    }

    if (typeof videoUrl !== "string") {
      res.status(400).json({ message: "videoUrl must be a string" });
      return;
    }

    if (!videoInfo || typeof videoInfo !== "object") {
      res.status(400).json({ message: "videoInfo must be an object" });
      return;
    }

    if (typeof videoInfo.width !== "number" ||
      typeof videoInfo.height !== "number" ||
      typeof videoInfo.durationInFrames !== "number" ||
      typeof videoInfo.fps !== "number") {
      res.status(400).json({
        message: "videoInfo must contain width, height, durationInFrames, and fps as numbers"
      });
      return;
    }

    const jobId = queue.createJob({
      style,
      captionPadding,
      customStyleConfigs,
      transcript,
      videoUrl,
      videoInfo,
    });

    res.json({ jobId });
  });

  // Endpoint to get a job status
  app.get("/renders/:jobId", (req, res) => {
    const jobId = req.params.jobId;
    const job = queue.jobs.get(jobId);

    if (!job) {
      res.status(404).json({ message: "Job not found" });
      return;
    }

    res.json(job);
  });

  // Endpoint to cancel a job
  app.delete("/renders/:jobId", (req, res) => {
    const jobId = req.params.jobId;

    const job = queue.jobs.get(jobId);

    if (!job) {
      res.status(404).json({ message: "Job not found" });
      return;
    }

    if (job.status !== "queued" && job.status !== "in-progress") {
      res.status(400).json({ message: "Job is not cancellable" });
      return;
    }

    job.cancel();

    res.json({ message: "Job cancelled" });
  });

  return app;
}

async function main() {
  await ensureBrowser();

  const remotionBundleUrl = REMOTION_SERVE_URL
    ? REMOTION_SERVE_URL
    : await bundle({
      entryPoint: path.resolve("src/remotion/index.ts"),
      onProgress(progress) {
        console.info(`Bundling Remotion project: ${progress}%`);
      },
    });

  const app = setupApp({ remotionBundleUrl });

  app.listen(PORT, () => {
    console.info(`Server is running on port ${PORT}`);
  });
}

main();