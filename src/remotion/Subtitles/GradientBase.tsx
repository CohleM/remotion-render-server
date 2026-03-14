// src/remotion/Subtitles/GradientBase.tsx
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

// ─── GradientBase-specific constants ─────────────────────────────────────────
// Negative spacing creates the overlapping/intersecting effect between lines.
// Scaled at runtime so it stays proportional across resolutions.

const LINE_SPACING_BASE = -25;

// ─── buildGradientMask ────────────────────────────────────────────────────────
// Unique to GradientBase — fades text out toward the bottom of each line
// using a CSS mask, creating the gradient dissolve effect.

const buildGradientMask = (): React.CSSProperties => {
    const gradient = 'linear-gradient(to bottom, rgba(0,0,0,1) 0%, rgba(0,0,0,1) 40%, rgba(0,0,0,0.7) 70%, rgba(0,0,0,0) 100%)';
    return {
        WebkitMaskImage: gradient,
        maskImage: gradient,
        WebkitMaskSize: '100% 100%',
        maskSize: '100% 100%',
    };
};

// ─── LineText ─────────────────────────────────────────────────────────────────
// Unique to GradientBase: applies a gradient mask to the text div,
// and does NOT apply fontScale (no equal-width scaling here).

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
    const gradientMask = useMemo(() => buildGradientMask(), []);

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
        ...gradientMask,
    }), [translateYOffset, style, textShadow, textStroke, gradientMask]);

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

// ─── GradientBase ─────────────────────────────────────────────────────────────

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const GradientBase: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);

    // Scale the negative line spacing proportionally to the render resolution
    const lineSpacing = useMemo(
        () => Math.round(LINE_SPACING_BASE * (height / 1920)),
        [height]
    );

    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('GradientBase: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    // GradientBase does not use fontScales — lines render at native font sizes.
    const lineOffsets = useMemo(
        () => fontsLoaded
            ? calculateLinePositions(group.lines, config, containerWidth, [], lineSpacing)
            : [],
        [group.lines, config, fontsLoaded, containerWidth, lineSpacing]
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