// src/remotion/MyVideo.tsx - IMPROVED VERSION
import { AbsoluteFill, Sequence, useVideoConfig, Html5Video, staticFile } from 'remotion';
import { SubtitleGroup } from '../../types/subtitles';
import { StyleRenderer } from './StyleRenderer';
import { SubtitleStyleConfig } from '../../types/style';
import { memo, useMemo } from 'react';
import { Video } from '@remotion/media';
type MainProps = {
    groups: SubtitleGroup[];
    style?: string;
    captionPadding?: number;
    customStyleConfigs: SubtitleStyleConfig;
    videoUrl: string
};

// ✅ Memoize the entire component to prevent unnecessary re-renders
export const MyVideo: React.FC<MainProps> = memo(({
    groups,
    style = 'basic',
    captionPadding = 540,
    customStyleConfigs,
    videoUrl
}) => {
    const { fps } = useVideoConfig();

    // ✅ Stable sequence calculations - only recalculate when groups actually change
    const sequences = useMemo(() => {
        return groups.map((group, index) => {
            const from = Math.max(0, Math.round(group.start * fps));
            const nextStart = groups[index + 1]?.start;
            const toSeconds = nextStart ?? group.end;
            const to = Math.max(from + 1, Math.round(toSeconds * fps));
            const durationInFrames = to - from;

            return {
                key: group.id, // Keep stable ID
                from,
                durationInFrames,
                group,
            };
        });
    }, [groups, fps]);

    return (
        <AbsoluteFill style={{ background: 'black' }}>
            {sequences.map(({ key, from, durationInFrames, group }) => (
                <Sequence
                    key={key} // Stable key prevents remounting
                    from={from}
                    durationInFrames={durationInFrames}
                    // ✅ Add layout="none" for better performance with changing content
                    layout="none"
                >
                    <StyleRenderer
                        group={group}
                        style={style}
                        captionPadding={captionPadding}
                        customConfigs={customStyleConfigs}
                    />
                </Sequence>
            ))}
            <Video src={videoUrl} />
        </AbsoluteFill>
    );
});