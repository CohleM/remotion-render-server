// src/remotion/Subtitles/EqualWidth.tsx
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
    calculateFontScales,
    calculateLinePositions,
    scaleConfig,
    ANIMATION_ANTICIPATION_FRAMES,
} from './shared/subtitleUtils';
import { useFontsLoaded, WordText } from './shared/SubtitlePrimitives';

const LINE_SPACING = 0;

// ─── LineText ────────────────────────────────────────────────────────────────

const LineText = memo(function LineText({
    line,
    lineIndex,
    translateYOffset,
    style,
    captionPadding,
    fontScale,
    animationType,
}: {
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
    fontScale: number;
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
        fontSize: style.fontSize * fontScale,
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
    }), [translateYOffset, style, textShadow, textStroke, fontScale]);

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

// ─── EqualWidth ──────────────────────────────────────────────────────────────

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const EqualWidth: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);
    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('EqualWidth: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    const fontScales = useMemo(
        () => fontsLoaded ? calculateFontScales(group.lines, config, height / 1920) : [],
        [group.lines, config, fontsLoaded, height]
    );

    const lineOffsets = useMemo(
        () => fontsLoaded && fontScales.length
            ? calculateLinePositions(group.lines, config, containerWidth, fontScales, LINE_SPACING)
            : [],
        [group.lines, config, fontsLoaded, containerWidth, fontScales]
    );

    if (!fontsLoaded || !lineOffsets.length || !fontScales.length) {
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
                            fontScale={fontScales[lineIndex]}
                            animationType={getAnimationType(config, line.font_type)}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};