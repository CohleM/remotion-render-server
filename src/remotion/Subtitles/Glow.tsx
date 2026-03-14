// src/remotion/Subtitles/Glow.tsx
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
    calculateLinePositions,
    scaleConfig,
    ANIMATION_ANTICIPATION_FRAMES,
} from './shared/subtitleUtils';
import { useFontsLoaded, WordText } from './shared/SubtitlePrimitives';

// ─── Glow-specific font preload ───────────────────────────────────────────────
// CormorantGaramond is only used in Glow — all other fonts are loaded in
// SubtitlePrimitives. Import and call it here so it's available when needed.

import { loadFont as loadCormorantGaramond } from '@remotion/google-fonts/CormorantGaramond';
loadCormorantGaramond();

const LINE_SPACING = 0;

// ─── buildGlowTextShadow ──────────────────────────────────────────────────────
// Unique to Glow: replaces the standard buildTextShadow from shared utils with
// a multi-layer glow effect. Uses hex-to-rgb conversion for opacity control.

const hexToRgb = (hex: string): { r: number; g: number; b: number } | null => {
    const result = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex);
    return result ? {
        r: parseInt(result[1], 16),
        g: parseInt(result[2], 16),
        b: parseInt(result[3], 16),
    } : null;
};

const buildGlowTextShadow = (style: FontStyleDefinition): string => {
    const shadows: string[] = [];

    // Subtle drop shadow for readability
    shadows.push('2px 2px 4px rgba(0,0,0,0.4)');

    // Multi-layer glow effect
    if (style.shadow && style.shadow !== 'none') {
        const baseColor = style.shadowColor || style.color || '#ffffff';
        const rgb = hexToRgb(baseColor) || { r: 255, g: 255, b: 255 };

        const glowLayers = [
            { blur: 10, opacity: 0.8 },  // Inner tight glow
            { blur: 0, opacity: 0.5 },  // Mid spread
            { blur: 100, opacity: 1.0 },  // Atmospheric falloff
        ];

        const intensityMultiplier =
            style.shadow === 'small' ? 0.6 :
                style.shadow === 'medium' ? 1.0 : 1.4;

        glowLayers.forEach(({ blur, opacity }) => {
            const finalOpacity = Math.min(opacity * intensityMultiplier, 1);
            shadows.push(`0 0 ${blur}px rgba(${rgb.r}, ${rgb.g}, ${rgb.b}, ${finalOpacity})`);
        });
    }

    // Stroke/outline effect
    if (style.strokeWeight && style.strokeWeight !== 'none') {
        const width = style.strokeWeight === 'small' ? 1 : style.strokeWeight === 'medium' ? 2 : 3;
        const color = style.strokeColor || '#000000';
        for (let x = -width; x <= width; x++) {
            for (let y = -width; y <= width; y++) {
                if (x !== 0 || y !== 0) {
                    shadows.push(`${x}px ${y}px 0 ${color}`);
                }
            }
        }
    }

    return shadows.join(', ');
};

// ─── LineText ─────────────────────────────────────────────────────────────────
// Unique to Glow: uses buildGlowTextShadow instead of the shared buildTextShadow.

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
    const textShadow = useMemo(() => buildGlowTextShadow(style), [style]);

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

// ─── Glow ─────────────────────────────────────────────────────────────────────

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const Glow: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);
    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('Glow: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    // Glow does not use fontScales — lines render at their native font sizes.
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