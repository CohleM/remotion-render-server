import { Composition } from "remotion";
import { Main, calculateMetadata } from "./Main";

export const RemotionRoot: React.FC = () => {
  return (
    <>
      <Composition
        id="VideoRenderer"
        component={Main}
        durationInFrames={300}
        fps={30}
        width={1080}
        height={1920}
        defaultProps={{
          style: 'basic',
          captionPadding: 540,
          customStyleConfigs: {},
          transcript: [],
          videoUrl: "",
          videoInfo: { width: 0, height: 0, durationInFrames: 0, fps: 0 }
        }}
        calculateMetadata={calculateMetadata}
      />
    </>
  );
};

