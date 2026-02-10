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
          customStyleConfigs: {
            id: "",
            name: "",
            category: "",
            isNew: false,
            isPremium: false,
            fonts: {
              bold: {
                fontSize: 0,
                fontWeight: 0,
                fontFamily: ""
              },
              thin: {
                fontSize: 0,
                fontWeight: 0,
                fontFamily: ""
              },
              normal: {
                fontSize: 0,
                fontWeight: 0,
                fontFamily: ""
              },
              italic: {
                fontSize: 0,
                fontWeight: 0,
                fontFamily: ""
              }
            }
          },
          transcript: [],
          videoUrl: "",
          videoInfo: { width: 0, height: 0, durationInFrames: 0, fps: 0 }
        }}
        calculateMetadata={calculateMetadata}
      />
    </>
  );
};

