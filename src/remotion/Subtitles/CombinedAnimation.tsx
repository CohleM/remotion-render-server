// src/remotion/Subtitles/CombinedAnimation.tsx
import React, { useMemo } from 'react';
import { AbsoluteFill, Sequence, useVideoConfig } from 'remotion';
import { memo } from 'react';
import { SubtitleGroup, Line } from '../../../types/subtitles';
import { SubtitleStyleConfig, FontStyleDefinition } from '../../../types/style';
import {
    getFontStyle,
    buildTextShadow,
    calculateFontScales,
    calculateLinePositions,
    scaleConfig,
    ANIMATION_ANTICIPATION_FRAMES,
} from './shared/subtitleUtils';
import { useFontsLoaded, WordText } from './shared/SubtitlePrimitives';

// ─── CombinedAnimation-specific: seeded random animation picker ──────────────

export type AnimationType =
    | 'slide-up'
    | 'slide-down'
    | 'slide-left'
    | 'slide-right'
    | 'scale'
    | 'fade-blur';

const ALL_ANIMATION_TYPES: AnimationType[] = [
    'slide-up',
    'slide-down',
    'slide-left',
    'slide-right',
    'scale',
    'fade-blur',
];

class SeededRandom {
    private seed: number;

    constructor(seed: string | number) {
        if (typeof seed === 'string') {
            let hash = 0;
            for (let i = 0; i < seed.length; i++) {
                const char = seed.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash;
            }
            this.seed = Math.abs(hash);
        } else {
            this.seed = seed;
        }
    }

    next(): number {
        this.seed = (this.seed * 9301 + 49297) % 233280;
        return this.seed / 233280;
    }

    shuffle<T>(array: T[]): T[] {
        const shuffled = [...array];
        for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(this.next() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        return shuffled;
    }
}

const getDistinctAnimationsForLines = (
    lineCount: number,
    seed: string | number
): AnimationType[] => {
    const rng = new SeededRandom(seed);
    if (lineCount <= ALL_ANIMATION_TYPES.length) {
        return rng.shuffle(ALL_ANIMATION_TYPES).slice(0, lineCount);
    }
    const animations: AnimationType[] = [];
    while (animations.length < lineCount) {
        animations.push(...rng.shuffle(ALL_ANIMATION_TYPES));
    }
    return animations.slice(0, lineCount);
};

// ─── LineText ────────────────────────────────────────────────────────────────
//
// CombinedAnimation's unique trait: each line gets an independently randomised
// animation type (handled above). The container/text styling is otherwise
// identical to a basic stacked layout.

const LineText = memo(function LineText({
    line,
    translateYOffset,
    style,
    captionPadding,
    animationType,
    fontScale,
}: {
    line: Line;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
    animationType: AnimationType;
    fontScale: number;
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

// ─── CombinedAnimation ───────────────────────────────────────────────────────

type Props = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const CombinedAnimation: React.FC<Props> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => scaleConfig(rawConfig, height), [rawConfig, height]);

    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('CombinedAnimation: invalid group data', group);
        return null;
    }

    const containerWidth = width * 0.9;

    const fontScales = useMemo(
        () => (fontsLoaded ? calculateFontScales(group.lines, config, height / 1920) : []),
        [group.lines, config, fontsLoaded]
    );

    // Deterministic seed from group start time — same group always gets the same animations.
    const animationSeed = useMemo(() => group.start.toFixed(3), [group.start]);

    const lineAnimations = useMemo(
        () => getDistinctAnimationsForLines(group.lines.length, animationSeed),
        [group.lines.length, animationSeed]
    );

    const lineOffsets = useMemo(
        () =>
            fontsLoaded && fontScales.length
                ? calculateLinePositions(group.lines, config, containerWidth, fontScales)
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
                            translateYOffset={lineOffsets[lineIndex]}
                            style={getFontStyle(config, line.font_type)}
                            captionPadding={captionPadding}
                            animationType={lineAnimations[lineIndex]}
                            fontScale={fontScales[lineIndex]}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};