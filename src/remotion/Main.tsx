import { SubtitleStyleConfig } from "../../types/style";
import { SubtitleGroup } from "../../types/subtitles";
import { MyVideo } from "./MyVideo";
import { CalculateMetadataFunction } from "remotion";

type VideoInfo = {
    width: number;
    height: number;
    durationInFrames: number;
    fps: number;
}

type MainProps = {
    transcript: SubtitleGroup[];
    style?: string;
    captionPadding: number;
    customStyleConfigs?: Record<string, SubtitleStyleConfig>; // Add this
    videoUrl: string
    videoInfo: VideoInfo
};

export const Main: React.FC<MainProps> = ({
    transcript,
    style = 'basic',
    captionPadding,
    customStyleConfigs,
    videoUrl,
    videoInfo
}) => {
    return <MyVideo
        groups={transcript}
        style={style}
        captionPadding={captionPadding}
        customStyleConfigs={customStyleConfigs} // Pass through
        videoUrl={videoUrl}
    />;
};

export const calculateMetadata: CalculateMetadataFunction<MainProps> = async ({
    props,
}) => {
    const { videoInfo } = props;

    // Use video dimensions if available, otherwise fallback to defaults
    const width = videoInfo?.width || 1080;
    const height = videoInfo?.height || 1920;
    const durationInFrames = videoInfo?.durationInFrames || 300;
    const fps = videoInfo?.fps || 30;

    return {
        width,
        height,
        durationInFrames,
        fps,
    };
};