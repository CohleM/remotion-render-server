import {
  makeCancelSignal,
  renderMedia,
  selectComposition,
} from "@remotion/renderer";
import { randomUUID } from "node:crypto";
import path from "node:path";
import { SubtitleGroup } from "../types/subtitles";
import { SubtitleStyleConfig } from "../types/style";


// import { loadFont as loadInter } from '@remotion/google-fonts/Inter';
// import { loadFont as loadBebas } from '@remotion/google-fonts/BebasNeue';
// import { loadFont as loadPoppins } from '@remotion/google-fonts/Poppins';
// import { loadFont as loadMontserrat } from '@remotion/google-fonts/Montserrat';
// import { loadFont as loadOswald } from '@remotion/google-fonts/Oswald';

// // Pre-load fonts with SPECIFIC weights you use to reduce network requests
// const inter = loadInter();
// const bebas = loadBebas();
// const poppins = loadPoppins();
// const montserrat = loadMontserrat();
// const oswald = loadOswald();


interface JobData {
  style: string;
  captionPadding: number;
  customStyleConfigs?: Record<string, SubtitleStyleConfig>;
  transcript: SubtitleGroup[];
  videoUrl: string;
  videoInfo: {
    width: number;
    height: number;
    durationInFrames: number;
    fps: number;
  };
}

type JobState =
  | {
    status: "queued";
    data: JobData;
    cancel: () => void;
  }
  | {
    status: "in-progress";
    progress: number;
    data: JobData;
    cancel: () => void;
  }
  | {
    status: "completed";
    videoUrl: string;
    data: JobData;
  }
  | {
    status: "failed";
    error: Error;
    data: JobData;
  };

const compositionId = "VideoRenderer";

export const makeRenderQueue = ({
  port,
  serveUrl,
  rendersDir,
}: {
  port: number;
  serveUrl: string;
  rendersDir: string;
}) => {
  const jobs = new Map<string, JobState>();
  let queue: Promise<unknown> = Promise.resolve();

  const processRender = async (jobId: string) => {
    const job = jobs.get(jobId);
    if (!job) {
      throw new Error(`Render job ${jobId} not found`);
    }

    const { cancel, cancelSignal } = makeCancelSignal();

    jobs.set(jobId, {
      progress: 0,
      status: "in-progress",
      cancel: cancel,
      data: job.data,
    });

    try {
      const inputProps = {
        style: job.data.style,
        captionPadding: job.data.captionPadding,
        customStyleConfigs: job.data.customStyleConfigs,
        transcript: job.data.transcript,
        videoUrl: job.data.videoUrl,
        videoInfo: job.data.videoInfo,
      };

      const composition = await selectComposition({
        serveUrl,
        id: compositionId,
        inputProps,
      });

      await renderMedia({
        cancelSignal,
        serveUrl,
        composition,
        inputProps,
        codec: "h264",
        onProgress: (progress) => {
          console.info(`${jobId} render progress:`, progress.progress);
          jobs.set(jobId, {
            progress: progress.progress,
            status: "in-progress",
            cancel: cancel,
            data: job.data,
          });
        },
        outputLocation: path.join(rendersDir, `${jobId}.mp4`),
      });

      jobs.set(jobId, {
        status: "completed",
        videoUrl: `http://localhost:${port}/renders/${jobId}.mp4`,
        data: job.data,
      });
    } catch (error) {
      console.error(error);
      jobs.set(jobId, {
        status: "failed",
        error: error as Error,
        data: job.data,
      });
    }
  };

  const queueRender = async ({
    jobId,
    data,
  }: {
    jobId: string;
    data: JobData;
  }) => {
    jobs.set(jobId, {
      status: "queued",
      data,
      cancel: () => {
        jobs.delete(jobId);
      },
    });

    queue = queue.then(() => processRender(jobId));
  };

  function createJob(data: JobData) {
    const jobId = randomUUID();

    queueRender({ jobId, data });

    return jobId;
  }

  return {
    createJob,
    jobs,
  };
};