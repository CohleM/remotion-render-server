// src/remotion/Subtitles/NormalAndItalic.tsx
import React, { useMemo } from 'react';
import {
    AbsoluteFill,
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

// ─── NormalAndItalic-specific font preload ────────────────────────────────────
// CormorantGaramond is used for italic lines in this layout.

import { loadFont as loadCormorantGaramond } from '@remotion/google-fonts/CormorantGaramond';
loadCormorantGaramond();

const LINE_SPACING = 0;

// ─── LineText ─────────────────────────────────────────────────────────────────

const LineText = memo(function LineText({
    line,
    lineIndex,
    translateYOffset,
    style,
    captionPadding,
    animationType,
}: {
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
    animationType: AnimationType;
}) {
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
    }), [captionPadding]);

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

// ─── NormalAndBold ────────────────────────────────────────────────────────────

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const NormalAndBold: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);
    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('NormalAndBold: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    // No fontScales — lines render at their native font sizes from config.
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

                return (
                    <Sequence key={`line-${lineIndex}`} from={from}>
                        <LineText
                            line={line}
                            lineIndex={lineIndex}
                            translateYOffset={lineOffsets[lineIndex]}
                            style={getFontStyle(config, line.font_type)}
                            captionPadding={captionPadding}
                            animationType={getAnimationType(config, line.font_type)}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};