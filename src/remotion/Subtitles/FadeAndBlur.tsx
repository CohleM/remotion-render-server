// src/remotion/Subtitles/FadeAndBlur.tsx
import React, { useMemo } from 'react';
import {
    AbsoluteFill,
    interpolate,
    useCurrentFrame,
    useVideoConfig,
    Sequence,
} from 'remotion';
import { memo } from 'react';
import { SubtitleGroup, Line } from '../../../types/subtitles';
import { SubtitleStyleConfig, FontStyleDefinition, AnimationType } from '../../../types/style';
import {
    getFontStyle,
    getAnimationType,
    buildTextShadow,
    calculateLinePositions,
    scaleConfig,
    ANIMATION_ANTICIPATION_FRAMES,
} from './shared/subtitleUtils';
import { useFontsLoaded, WordText } from './shared/SubtitlePrimitives';

// ─── FadeAndBlur-specific constants ──────────────────────────────────────────

const LINE_SPACING = 0;
const LINE_OVERLAP_FRAMES = 15;
const FADE_OUT_DURATION_FRAMES = 15;

// ─── useLineFadeOut ───────────────────────────────────────────────────────────
// Fades out the entire line when the next line starts appearing.
// This is unique to FadeAndBlur — no other layout does line-level fade-out.

const useLineFadeOut = (
    frame: number,
    fps: number,
    lineStart: number,
    nextLineStart: number | null,
    groupStart: number
) => {
    return useMemo(() => {
        if (nextLineStart === null) {
            return { opacity: 1, blur: 0 };
        }

        const relativeNextLineStart = (nextLineStart - groupStart) * fps;
        const fadeOutStart = relativeNextLineStart + LINE_OVERLAP_FRAMES;
        const fadeOutEnd = fadeOutStart + FADE_OUT_DURATION_FRAMES;

        if (frame < fadeOutStart) {
            return { opacity: 1, blur: 0 };
        }

        if (frame >= fadeOutEnd) {
            return { opacity: 0, blur: 10 };
        }

        const progress = (frame - fadeOutStart) / FADE_OUT_DURATION_FRAMES;

        return {
            opacity: interpolate(progress, [0, 1], [1, 0]),
            blur: interpolate(progress, [0, 1], [0, 10]),
        };
    }, [frame, fps, lineStart, nextLineStart, groupStart]);
};

// ─── LineText ─────────────────────────────────────────────────────────────────
// Unique to FadeAndBlur: applies line-level opacity/blur fade-out via
// useLineFadeOut, and does NOT apply fontScale (no equal-width scaling here).

const LineText = memo(function LineText({
    line,
    lineIndex,
    translateYOffset,
    style,
    captionPadding,
    nextLineStart,
    groupStart,
    animationType,
}: {
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
    nextLineStart: number | null;
    groupStart: number;
    animationType: AnimationType;
}) {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const { opacity: fadeOutOpacity, blur: fadeOutBlur } = useLineFadeOut(
        frame,
        fps,
        line.start,
        nextLineStart,
        groupStart
    );

    const textShadow = useMemo(() => buildTextShadow(style), [style]);

    const textStroke = useMemo(() => {
        if (!style.strokeWeight || style.strokeWeight === 'none') return undefined;
        const width = style.strokeWeight === 'small' ? 1 : style.strokeWeight === 'medium' ? 2 : 3;
        return `${width}px ${style.strokeColor || '#000000'}`;
    }, [style.strokeWeight, style.strokeColor]);

    const containerStyle = useMemo(() => ({
        justifyContent: 'flex-start' as const,
        alignItems: 'center' as const,
        paddingTop: captionPadding,
        opacity: fadeOutOpacity,
        filter: `blur(${fadeOutBlur}px)`,
        transition: 'none',
    }), [captionPadding, fadeOutOpacity, fadeOutBlur]);

    const textStyle = useMemo(() => ({
        transform: `translateY(${translateYOffset}px)`,
        fontSize: style.fontSize,
        fontFamily: `"${style.fontFamily}", sans-serif`,
        fontWeight: style.fontWeight,
        fontStyle: style.fontStyle || 'normal',
        color: style.color || '#ffffff',
        textAlign: 'center' as const,
        textShadow,
        lineHeight: 1.0,
        display: 'flex',
        flexWrap: 'wrap' as const,
        justifyContent: 'center',
        alignItems: 'baseline',
        textTransform: style.uppercase ? 'uppercase' : 'none' as const,
        WebkitTextStroke: textStroke,
    }), [translateYOffset, style, textShadow, textStroke]);

    // const lineEndTime = nextLineStart !== null
    //     ? nextLineStart
    //     : groupStart + (line.words[line.words.length - 1]?.end || line.end);

    return (
        <AbsoluteFill style={containerStyle}>
            <div style={textStyle}>
                {line.words.map((word, wordIndex) => (
                    <WordText
                        key={`${word.id}-${wordIndex}`}
                        word={word.word}
                        wordIndex={wordIndex}
                        lineStart={line.start}
                        wordStart={word.start}
                        wordEnd={word.end}
                        animationType={animationType}
                    />
                ))}
            </div>
        </AbsoluteFill>
    );
});

// ─── FadeAndBlur ─────────────────────────────────────────────────────────────

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const FadeAndBlur: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);
    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('FadeAndBlur: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    // FadeAndBlur does not use fontScales — no equal-width scaling applied.
    // Lines render at their native font sizes from config.
    const lineOffsets = useMemo(
        () => fontsLoaded
            ? calculateLinePositions(group.lines, config, containerWidth, [], LINE_SPACING)
            : [],
        [group.lines, config, fontsLoaded, containerWidth]
    );

    if (!fontsLoaded || !lineOffsets.length) {
        return null;
    }

    return (
        <AbsoluteFill>
            {group.lines.map((line, lineIndex) => {
                const relativeStart = line.start - group.start;
                const from = Math.max(
                    0,
                    Math.round(relativeStart * fps) - ANIMATION_ANTICIPATION_FRAMES
                );

                const nextLine = group.lines[lineIndex + 1];
                const nextLineStart = nextLine ? nextLine.start : null;

                const durationInFrames = nextLineStart
                    ? Math.round((nextLineStart - line.start) * fps) + LINE_OVERLAP_FRAMES + FADE_OUT_DURATION_FRAMES
                    : undefined;

                return (
                    <Sequence
                        key={`line-${lineIndex}`}
                        from={from}
                        durationInFrames={durationInFrames}
                    >
                        <LineText
                            line={line}
                            lineIndex={lineIndex}
                            translateYOffset={lineOffsets[lineIndex]}
                            style={getFontStyle(config, line.font_type)}
                            captionPadding={captionPadding}
                            nextLineStart={nextLineStart}
                            groupStart={group.start}
                            animationType={getAnimationType(config, line.font_type)}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};