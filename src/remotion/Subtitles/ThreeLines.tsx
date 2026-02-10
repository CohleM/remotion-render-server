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

// Measure actual rendered height of text content
const measureActualTextHeight = (
    text: string,
    style: FontStyleDefinition,
    containerWidth: number
): number => {
    const tempDiv = document.createElement('div');
    tempDiv.style.position = 'absolute';
    tempDiv.style.visibility = 'hidden';
    tempDiv.style.fontSize = `${style.fontSize}px`;
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
    containerWidth: number
): number[] => {
    if (lines.length === 0) return [];

    const lineHeights = lines.map((line) => {
        const style = getFontStyle(config, line.font_type);
        const text = line.words.map(w => w.word).join(' ');
        return measureActualTextHeight(text, style, containerWidth);
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
    const shadows: string[] = ['3px 3px 6px rgba(0,0,0,0.9)']; // Default shadow

    if (style.shadow && style.shadow !== 'none') {
        const blur = style.shadow === 'small' ? 10 : style.shadow === 'medium' ? 20 : 30;
        const color = style.shadowColor || style.color || '#ffffff';
        shadows.push(`0 0 ${blur}px ${color}`);
    }

    if (style.strokeWeight && style.strokeWeight !== 'none') {
        const width = style.strokeWeight === 'small' ? 1 : style.strokeWeight === 'medium' ? 2 : 3;
        const color = style.strokeColor || '#000000';
        // Create outline effect using multiple shadows
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

const WordText = memo(function WordText({
    word,
    wordIndex,
    lineStart,
    wordStart,
}: {
    word: string;
    wordIndex: number;
    lineStart: number;
    wordStart: number;
}) {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const relativeWordStart = wordStart - lineStart;
    const wordStartFrame = Math.round(relativeWordStart * fps);
    const animationFrame = Math.max(0, frame - wordStartFrame);

    // ✅ Use useMemo for expensive calculations
    const springValue = useMemo(() => spring({
        frame: animationFrame,
        fps,
        config: { damping: 100, stiffness: 100 },
    }), [animationFrame, fps]);

    const translateY = useMemo(() =>
        interpolate(springValue, [0, 1], [50, 0]),
        [springValue]);

    const opacity = useMemo(() =>
        interpolate(springValue, [0, 1], [0, 1]),
        [springValue]);

    return (
        <span style={{
            display: 'inline-block',
            transform: `translateY(${translateY}px)`,
            opacity,
            marginRight: '0.3em',
            whiteSpace: 'pre',
        }}>
            {word}
        </span>
    );
});

// Wrap LineText in memo
const LineText = memo(function LineText({
    line,
    lineIndex,
    translateYOffset,
    style,
    captionPadding,
}: {
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    style: FontStyleDefinition;
    captionPadding: number;
}) {
    // ✅ Memoize expensive style calculations
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
        textShadow: textShadow,
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
                        key={`${word.id}-${wordIndex}`} // Stable key using word.id
                        word={word.word}
                        wordIndex={wordIndex}
                        lineStart={line.start}
                        wordStart={word.start}
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

export const ThreeLines: React.FC<ThreeLinesProps> = ({
    group,
    config,
    captionPadding = 540
}) => {
    const { fps, width } = useVideoConfig();
    const fontsLoaded = useFontsLoaded(config);


    // console.log('this is CustomStyleConfigs', config.fonts.bold)

    if (!group?.lines?.length) {
        console.error('Invalid group data:', group);
        return null;
    }

    const containerWidth = width * 0.9;

    const lineOffsets = useMemo(() => {
        if (!fontsLoaded) return [];
        return calculateLinePositions(group.lines, config, containerWidth);
    }, [group.lines, config, fontsLoaded, containerWidth]);

    if (!fontsLoaded || lineOffsets.length === 0) {
        return null;
    }

    return (
        <AbsoluteFill>
            {group.lines.map((line, lineIndex) => {
                const relativeStart = line.start - group.start;
                const from = Math.round(relativeStart * fps);
                const fontStyle = getFontStyle(config, line.font_type);

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
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};