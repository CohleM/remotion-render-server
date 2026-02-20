// src/remotion/Subtitles/ThreeLines.tsx
import React, { useMemo, useState, useEffect } from 'react';
import {
    AbsoluteFill,
    interpolate,
    useCurrentFrame,
    useVideoConfig,
    Sequence,
    delayRender,
    continueRender,
    spring
} from 'remotion';
import { memo } from 'react';
import { SubtitleGroup, Line } from '../../../types/subtitles';
import { SubtitleStyleConfig, FontStyleDefinition } from '../../../types/style';

// Pre-load common fonts to avoid delayRender issues
import { loadFont as loadInter } from '@remotion/google-fonts/Inter';
import { loadFont as loadBebas } from '@remotion/google-fonts/BebasNeue';
import { loadFont as loadPoppins } from '@remotion/google-fonts/Poppins';
import { loadFont as loadMontserrat } from '@remotion/google-fonts/Montserrat';
import { loadFont as loadOswald } from '@remotion/google-fonts/Oswald';

// Initialize fonts
loadInter();
loadBebas();
loadPoppins();
loadMontserrat();
loadOswald();

const LINE_SPACING = 0;
const MAX_FONT_SIZE = 100; // Maximum allowed font size in pixels
const ANIMATION_ANTICIPATION_FRAMES = 4;
const FADE_OUT_DURATION_FRAMES = 30;
const MAX_WORD_DISPLAY_SECONDS = 3;

// Animation types
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

// Seeded random number generator for deterministic animations
class SeededRandom {
    private seed: number;

    constructor(seed: string | number) {
        // Convert string seed to number using simple hash
        if (typeof seed === 'string') {
            let hash = 0;
            for (let i = 0; i < seed.length; i++) {
                const char = seed.charCodeAt(i);
                hash = ((hash << 5) - hash) + char;
                hash = hash & hash; // Convert to 32bit integer
            }
            this.seed = Math.abs(hash);
        } else {
            this.seed = seed;
        }
    }

    // Returns random number between 0 and 1 (deterministic based on seed)
    next(): number {
        // Linear Congruential Generator
        this.seed = (this.seed * 9301 + 49297) % 233280;
        return this.seed / 233280;
    }

    // Fisher-Yates shuffle with seeded random
    shuffle<T>(array: T[]): T[] {
        const shuffled = [...array];
        for (let i = shuffled.length - 1; i > 0; i--) {
            const j = Math.floor(this.next() * (i + 1));
            [shuffled[i], shuffled[j]] = [shuffled[j], shuffled[i]];
        }
        return shuffled;
    }
}

// Get distinct random animations for each line using seeded random
const getDistinctAnimationsForLines = (
    lineCount: number,
    seed: string | number
): AnimationType[] => {
    const rng = new SeededRandom(seed);

    if (lineCount <= ALL_ANIMATION_TYPES.length) {
        return rng.shuffle(ALL_ANIMATION_TYPES).slice(0, lineCount);
    } else {
        const animations: AnimationType[] = [];
        while (animations.length < lineCount) {
            const shuffled = rng.shuffle(ALL_ANIMATION_TYPES);
            animations.push(...shuffled);
        }
        return animations.slice(0, lineCount);
    }
};

// Get font styles from config safely
const getFontStyle = (config: SubtitleStyleConfig, fontType: string): FontStyleDefinition => {
    const defaultStyle: FontStyleDefinition = {
        fontSize: 60,
        fontWeight: 400,
        fontFamily: 'Arial',
        color: '#ffffff'
    };

    return config.fonts[fontType as keyof typeof config.fonts] || defaultStyle;
};

// Hook to wait for Google Fonts to load based on config
const useFontsLoaded = (config: SubtitleStyleConfig) => {
    const [loaded, setLoaded] = useState(false);
    const [handle] = useState(() => delayRender('Loading Google Fonts'));

    useEffect(() => {
        const loadAllFonts = async () => {
            const fontStyles = Object.values(config.fonts);

            try {
                await Promise.all(
                    fontStyles.map(style =>
                        document.fonts.load(
                            `${style.fontStyle || 'normal'} ${style.fontWeight} ${style.fontSize}px "${style.fontFamily}"`
                        )
                    )
                );
                setLoaded(true);
                continueRender(handle);
            } catch (err) {
                console.warn('Font loading failed:', err);
                setLoaded(true);
                continueRender(handle);
            }
        };

        loadAllFonts();
    }, [config, handle]);

    return loaded;
};

// Measure text width with a given font style
const measureTextWidth = (
    text: string,
    style: FontStyleDefinition
): number => {
    const tempSpan = document.createElement('span');
    tempSpan.style.position = 'absolute';
    tempSpan.style.visibility = 'hidden';
    tempSpan.style.fontSize = `${style.fontSize}px`;
    tempSpan.style.fontFamily = `"${style.fontFamily}", sans-serif`;
    tempSpan.style.fontWeight = String(style.fontWeight);
    tempSpan.style.fontStyle = style.fontStyle || 'normal';
    tempSpan.style.whiteSpace = 'nowrap';
    tempSpan.style.textTransform = style.uppercase ? 'uppercase' : 'none';
    tempSpan.textContent = text;

    document.body.appendChild(tempSpan);
    const width = tempSpan.offsetWidth;
    document.body.removeChild(tempSpan);

    return width;
};

// Calculate font size scale factors for each line to match widest line, with max cap
const calculateFontScales = (
    lines: Line[],
    config: SubtitleStyleConfig
): number[] => {
    if (lines.length === 0) return [];

    const lineWidths = lines.map((line) => {
        const style = getFontStyle(config, line.font_type);
        const text = line.words.map(w => w.word).join(' ');
        return {
            width: measureTextWidth(text, style),
            fontSize: style.fontSize
        };
    });

    const maxWidth = Math.max(...lineWidths.map(lw => lw.width));

    const scales = lineWidths.map((lw) => {
        if (lw.width === 0) return 1;

        // Calculate what the scaled font size would be
        const rawScale = maxWidth / lw.width;
        const scaledFontSize = lw.fontSize * rawScale;

        // If scaled font size exceeds MAX_FONT_SIZE, cap it
        if (scaledFontSize > MAX_FONT_SIZE) {
            return MAX_FONT_SIZE / lw.fontSize;
        }

        // Otherwise use the original scale to match widths
        return rawScale;
    });

    return scales;
};

// Measure actual rendered height of text content (with scaled font size)
const measureActualTextHeight = (
    text: string,
    style: FontStyleDefinition,
    containerWidth: number,
    scale: number = 1
): number => {
    const tempDiv = document.createElement('div');
    tempDiv.style.position = 'absolute';
    tempDiv.style.visibility = 'hidden';
    tempDiv.style.fontSize = `${style.fontSize * scale}px`;
    tempDiv.style.fontFamily = `"${style.fontFamily}", sans-serif`;
    tempDiv.style.fontWeight = String(style.fontWeight);
    tempDiv.style.fontStyle = style.fontStyle || 'normal';
    tempDiv.style.lineHeight = '1.0';
    tempDiv.style.width = `${containerWidth}px`;
    tempDiv.style.display = 'flex';
    tempDiv.style.flexWrap = 'wrap';
    tempDiv.style.justifyContent = 'center';
    tempDiv.style.alignItems = 'baseline';
    tempDiv.style.textTransform = style.uppercase ? 'uppercase' : 'none';

    const words = text.split(' ');
    words.forEach((word) => {
        const span = document.createElement('span');
        span.style.display = 'inline-block';
        span.style.marginRight = '0.3em';
        span.textContent = word;
        tempDiv.appendChild(span);
    });

    document.body.appendChild(tempDiv);
    const height = tempDiv.offsetHeight;
    document.body.removeChild(tempDiv);

    return height;
};

const calculateLinePositions = (
    lines: Line[],
    config: SubtitleStyleConfig,
    containerWidth: number,
    fontScales: number[]
): number[] => {
    if (lines.length === 0) return [];

    const lineHeights = lines.map((line, index) => {
        const style = getFontStyle(config, line.font_type);
        const text = line.words.map(w => w.word).join(' ');
        const scale = fontScales[index] || 1;
        return measureActualTextHeight(text, style, containerWidth, scale);
    });

    const offsets: number[] = [0];

    for (let i = 1; i < lines.length; i++) {
        const previousHeight = lineHeights[i - 1];
        const previousOffset = offsets[i - 1];
        offsets.push(previousOffset + previousHeight + LINE_SPACING);
    }
    return offsets;
};

// Build text shadow based on shadow settings
const buildTextShadow = (style: FontStyleDefinition): string => {
    const shadows: string[] = ['2px 2px 2px rgba(0,0,0,0.4)'];

    if (style.shadow && style.shadow !== 'none') {
        const blur = style.shadow === 'small' ? 10 : style.shadow === 'medium' ? 20 : 30;
        const color = style.shadowColor || style.color || '#ffffff';
        shadows.push(`0 0 ${blur}px ${color}`);
    }

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

// Hook to calculate animation values based on type
const useWordAnimation = (
    animationType: AnimationType,
    frame: number,
    fps: number,
    wordStartFrame: number
) => {
    const animationFrame = Math.max(0, frame - wordStartFrame);

    const springValue = useMemo(() => spring({
        frame: animationFrame,
        fps,
        config: { damping: 100, stiffness: 100 },
    }), [animationFrame, fps]);

    return useMemo(() => {
        switch (animationType) {
            case 'slide-up':
                return {
                    transform: `translateY(${interpolate(springValue, [0, 1], [50, 0])}px)`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };

            case 'slide-down':
                return {
                    transform: `translateY(${interpolate(springValue, [0, 1], [-50, 0])}px)`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };

            case 'slide-left':
                return {
                    transform: `translateX(${interpolate(springValue, [0, 1], [100, 0])}px)`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };

            case 'slide-right':
                return {
                    transform: `translateX(${interpolate(springValue, [0, 1], [-100, 0])}px)`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };

            case 'scale':
                return {
                    transform: `scale(${interpolate(springValue, [0, 1], [0.5, 1])})`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };

            case 'fade-blur':
                return {
                    transform: 'none',
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [10, 0])}px)`,
                };

            default:
                return {
                    transform: `translateY(${interpolate(springValue, [0, 1], [50, 0])}px)`,
                    opacity: interpolate(springValue, [0, 1], [0, 1]),
                    filter: `blur(${interpolate(springValue, [0, 1], [4, 0])}px)`,
                };
        }
    }, [animationType, springValue]);
};

const WordText = memo(function WordText({
    word,
    wordIndex,
    lineStart,
    wordStart,
    wordEnd,
    animationType,
}: {
    word: string;
    wordIndex: number;
    lineStart: number;
    wordStart: number;
    wordEnd: number;
    animationType: AnimationType;
}) {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const relativeWordStart = wordStart - lineStart;
    const wordStartFrame = Math.round(relativeWordStart * fps) - ANIMATION_ANTICIPATION_FRAMES;
    const realWordStartFrame = Math.round(relativeWordStart * fps);

    const maxDisplayFrames = MAX_WORD_DISPLAY_SECONDS * fps;
    const fadeOutStartFrame = realWordStartFrame + maxDisplayFrames;

    const relativeWordEnd = wordEnd - lineStart;
    const wordEndFrame = Math.round(relativeWordEnd * fps);
    const fadeOutEndFrame = Math.min(fadeOutStartFrame + FADE_OUT_DURATION_FRAMES, wordEndFrame);

    const entryAnimation = useWordAnimation(animationType, frame, fps, wordStartFrame);

    const { opacity, transform, filter } = useMemo(() => {
        const entryOpacity = entryAnimation.opacity;
        const entryTransform = entryAnimation.transform;
        const entryFilter = entryAnimation.filter;

        if (frame < fadeOutStartFrame) {
            return { opacity: entryOpacity, transform: entryTransform, filter: entryFilter };
        }

        if (frame >= fadeOutEndFrame) {
            return { opacity: 0, transform: entryTransform, filter: 'blur(10px)' };
        }

        const fadeOutProgress = (frame - fadeOutStartFrame) / (fadeOutEndFrame - fadeOutStartFrame);
        const fadeOutOpacity = interpolate(fadeOutProgress, [0, 1], [entryOpacity, 0]);
        const fadeOutBlur = interpolate(fadeOutProgress, [0, 1], [0, 10]);

        return { opacity: fadeOutOpacity, transform: entryTransform, filter: `blur(${fadeOutBlur}px)` };
    }, [entryAnimation, frame, fadeOutStartFrame, fadeOutEndFrame]);

    return (
        <span style={{
            display: 'inline-block',
            transform,
            opacity,
            filter,
            marginRight: '0.3em',
            whiteSpace: 'pre',
            willChange: 'transform, opacity, filter',
        }}>
            {word}
        </span>
    );
});

const LineText = memo(function LineText({
    line,
    lineIndex,
    translateYOffset,
    style,
    captionPadding,
    animationType,
    fontScale,
    lineEnd,
}: {
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
    animationType: AnimationType;
    fontScale: number;
    lineEnd: number;
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
        textShadow: textShadow,
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

type ThreeLinesProps = {
    group: SubtitleGroup;
    config: SubtitleStyleConfig;
    captionPadding?: number;
};

export const CombinedAnimation: React.FC<ThreeLinesProps> = ({
    group,
    config: rawConfig,
    captionPadding = 540,
}) => {
    const { fps, width, height } = useVideoConfig();

    const config = useMemo(() => {
        const scale = height / 1920;
        return {
            ...rawConfig,
            fonts: Object.fromEntries(
                Object.entries(rawConfig.fonts).map(([key, style]) => [
                    key,
                    { ...style, fontSize: Math.round(style.fontSize * scale) }
                ])
            ) as SubtitleStyleConfig['fonts']  // 👈 cast back to the original type
        };
    }, [rawConfig, height]);
    const fontsLoaded = useFontsLoaded(config);

    if (!group?.lines?.length) {
        console.error('Invalid group data:', group);
        return null;
    }

    const containerWidth = width * 0.9;

    // Calculate font scales to equalize line widths (with max cap)
    const fontScales = useMemo(() => {
        if (!fontsLoaded) return [];
        return calculateFontScales(group.lines, config);
    }, [group.lines, config, fontsLoaded]);

    // Create a deterministic seed based on group start time
    // This ensures the same group always gets the same animations
    const animationSeed = useMemo(() => {
        // Use group start time as seed - converts to string with fixed precision
        return group.start.toFixed(3);
    }, [group.start]);

    // Generate distinct random animations for each line using seeded random
    const lineAnimations = useMemo(() => {
        return getDistinctAnimationsForLines(group.lines.length, animationSeed);
    }, [group.lines.length, animationSeed]);

    const lineOffsets = useMemo(() => {
        if (!fontsLoaded || fontScales.length === 0) return [];
        return calculateLinePositions(group.lines, config, containerWidth, fontScales);
    }, [group.lines, config, fontsLoaded, containerWidth, fontScales]);

    if (!fontsLoaded || lineOffsets.length === 0 || fontScales.length === 0) {
        return null;
    }

    return (
        <AbsoluteFill>
            {group.lines.map((line, lineIndex) => {
                const relativeStart = line.start - group.start;
                const from = Math.max(0, Math.round(relativeStart * fps) - ANIMATION_ANTICIPATION_FRAMES);
                const fontStyle = getFontStyle(config, line.font_type);
                const animationType = lineAnimations[lineIndex];

                const nextLine = group.lines[lineIndex + 1];
                const lineEndTime = nextLine ? nextLine.start : (group.start + (line.words[line.words.length - 1]?.end || line.end));

                return (
                    <Sequence
                        key={`line-${lineIndex}`}
                        from={from}
                    >
                        <LineText
                            line={line}
                            lineIndex={lineIndex}
                            translateYOffset={lineOffsets[lineIndex]}
                            style={fontStyle}
                            captionPadding={captionPadding}
                            animationType={animationType}
                            fontScale={fontScales[lineIndex]}
                            lineEnd={lineEndTime}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};