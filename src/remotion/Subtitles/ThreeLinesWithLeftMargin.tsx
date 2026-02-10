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

// Import Google Fonts
import { loadFont as loadInter } from '@remotion/google-fonts/Poppins';
import { loadFont as loadMontserrat } from '@remotion/google-fonts/Poppins';
import { loadFont as loadBebas } from '@remotion/google-fonts/BebasNeue';
import { loadFont as loadPlayfair } from '@remotion/google-fonts/PlayfairDisplay'; // Added for italic

type Word = {
    word: string;
    start: number;
    end: number;
};

type Line = {
    text: string;
    start: number;
    end: number;
    words: Word[];
    font_type: string;
};

type SubtitleGroup = {
    function: string;
    group_text: string;
    lines: Line[];
    start: number;
    end: number;
};

const LINE_SPACING = 0;

// Initialize fonts
const inter = loadInter();
const montserrat = loadMontserrat();
const bebas = loadBebas();
const playfair = loadPlayfair(); // Initialize italic font

// Font type mapping - now includes color property
const FONT_TYPE_MAP: Record<string, { fontSize: number; fontWeight: number; fontFamily: string; fontStyle?: string; color: string }> = {
    'bold': { fontSize: 120, fontWeight: 800, fontFamily: bebas.fontFamily, color: '#cf11d9' }, // Gold color for bold
    'thin': { fontSize: 50, fontWeight: 100, fontFamily: inter.fontFamily, color: 'white' },
    'normal': { fontSize: 60, fontWeight: 400, fontFamily: montserrat.fontFamily, color: 'white' },
    'italic': { fontSize: 60, fontWeight: 200, fontFamily: inter.fontFamily, fontStyle: 'italic', color: 'white' },
};

const DEFAULT_STYLE = { fontSize: 60, fontFamily: 'Arial, sans-serif', fontWeight: 800, color: 'white' };

// Get all unique font styles for loading
const getAllFontStyles = () => {
    return Object.values(FONT_TYPE_MAP);
};

// Hook to wait for Google Fonts to load with their specific weights
const useFontsLoaded = () => {
    const [loaded, setLoaded] = useState(false);
    const [handle] = useState(() => delayRender('Loading Google Fonts'));

    useEffect(() => {
        const loadAllFonts = async () => {
            // Load each font with its specific weight and size
            const fontStyles = getAllFontStyles();
            const fontPromises = fontStyles.map(style => {
                const fontStyle = style.fontStyle || 'normal';
                return document.fonts.load(`${fontStyle} ${style.fontWeight} ${style.fontSize}px "${style.fontFamily}"`);
            });

            try {
                await Promise.all(fontPromises);
                setLoaded(true);
                continueRender(handle);
            } catch (err) {
                console.warn('Font loading failed:', err);
                setLoaded(true);
                continueRender(handle);
            }
        };

        loadAllFonts();
    }, [handle]);

    return loaded;
};

// Measure actual rendered height of text content (including wrapped lines)
const measureActualTextHeight = (
    text: string,
    fontSize: number,
    fontFamily: string,
    fontWeight: number,
    containerWidth: number,
    fontStyle: string = 'normal'
): number => {
    const tempDiv = document.createElement('div');
    tempDiv.style.position = 'absolute';
    tempDiv.style.visibility = 'hidden';
    tempDiv.style.fontSize = `${fontSize}px`;
    tempDiv.style.fontFamily = `"${fontFamily}", sans-serif`;
    tempDiv.style.fontWeight = String(fontWeight);
    tempDiv.style.fontStyle = fontStyle;
    tempDiv.style.lineHeight = '1.0';
    tempDiv.style.width = `${containerWidth}px`;
    tempDiv.style.display = 'flex';
    tempDiv.style.flexWrap = 'wrap';
    tempDiv.style.justifyContent = 'center';
    tempDiv.style.alignItems = 'baseline';

    // Add words with spacing
    const words = text.split(' ');
    words.forEach((word, idx) => {
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

const calculateLinePositions = (lines: Line[], containerWidth: number): number[] => {
    if (lines.length === 0) return [];

    const lineHeights = lines.map((line) => {
        const style = FONT_TYPE_MAP[line.font_type] || DEFAULT_STYLE;
        const text = line.words.map(w => w.word).join(' ');
        return measureActualTextHeight(
            text,
            style.fontSize,
            style.fontFamily,
            style.fontWeight,
            containerWidth,
            style.fontStyle || 'normal'
        );
    });

    const offsets: number[] = [0]; // First line at top (translateY(0))

    for (let i = 1; i < lines.length; i++) {
        const previousHeight = lineHeights[i - 1];
        const previousOffset = offsets[i - 1];
        // Stack downward: previous position plus its height plus spacing
        offsets.push(previousOffset + previousHeight + LINE_SPACING);
    }
    return offsets;
};

const WordText: React.FC<{
    word: string;
    wordIndex: number;
    lineStart: number;
    wordStart: number;
}> = ({ word, wordIndex, lineStart, wordStart }) => {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const relativeWordStart = wordStart - lineStart;
    const wordStartFrame = Math.round(relativeWordStart * fps);
    const animationFrame = Math.max(0, frame - wordStartFrame);

    // Spring animation for smooth entrance
    const springValue = spring({
        frame: animationFrame,
        fps,
        config: {
            damping: 100,
            stiffness: 100,
            // mass: 0.5,
        },
    });

    // Animate from below (50px down) to current position
    const translateY = interpolate(
        springValue,
        [0, 1],
        [50, 0]
    );

    // Fade in opacity during the movement
    const opacity = interpolate(
        springValue,
        [0, 1],
        [0, 1]
    );

    return (
        <span
            style={{
                display: 'inline-block',
                transform: `translateY(${translateY}px)`,
                opacity,
                marginRight: '0.3em',
                whiteSpace: 'pre',
            }}
        >
            {word}
        </span>
    );
};

const LineText: React.FC<{
    line: Line;
    lineIndex: number;
    translateYOffset: number;
    fontType: string;
}> = ({ line, lineIndex, translateYOffset, fontType }) => {
    const style = FONT_TYPE_MAP[fontType] || DEFAULT_STYLE;

    // Apply glow effect to all text types using their respective colors
    const textShadow = fontType === 'bold'
        ? `0 0 20px ${style.color}, 3px 3px 6px rgba(0,0,0,0.9)` // Gold glow for bold
        : `0 0 15px ${style.color}, 3px 3px 6px rgba(0,0,0,0.9)`; // White glow for others

    console.log('translateYOffset:', translateYOffset);
    console.log('fontType:', fontType, 'style:', style);

    return (
        <AbsoluteFill
            style={{
                justifyContent: 'flex-start',
                alignItems: 'center',
                paddingTop: 1500
            }}
        >
            <div
                style={{
                    transform: `translateY(${translateYOffset}px)`,
                    fontSize: style.fontSize,
                    fontFamily: `"${style.fontFamily}", sans-serif`,
                    fontWeight: style.fontWeight,
                    fontStyle: style.fontStyle || 'normal',
                    color: style.color, // Now uses color from font type map
                    textAlign: 'center',
                    textShadow: textShadow, // Dynamic text shadow based on font type
                    lineHeight: 1.0,
                    display: 'flex',
                    flexWrap: 'wrap',
                    justifyContent: 'center',
                    alignItems: 'baseline',
                }}
            >
                {line.words.map((word, wordIndex) => (
                    <WordText
                        key={`word-${lineIndex}-${wordIndex}`}
                        word={word.word}
                        wordIndex={wordIndex}
                        lineStart={line.start}
                        wordStart={word.start}
                    />
                ))}
            </div>
        </AbsoluteFill>
    );
};

export const ThreeLinesWithLeftMargin: React.FC<{ group: SubtitleGroup }> = ({ group }) => {
    const { fps, width } = useVideoConfig();
    const fontsLoaded = useFontsLoaded();

    if (!group?.lines?.length) {
        console.error('Invalid group data:', group);
        return null;
    }

    const containerWidth = width * 0.9;
    const lineOffsets = useMemo(() => {
        if (!fontsLoaded) return [];
        return calculateLinePositions(group.lines, containerWidth);
    }, [group.lines, fontsLoaded, containerWidth]);

    if (!fontsLoaded || lineOffsets.length === 0) {
        return null;
    }

    return (
        <AbsoluteFill>
            {group.lines.map((line, lineIndex) => {
                const relativeStart = line.start - group.start;
                const from = Math.round(relativeStart * fps);

                return (
                    <Sequence
                        key={`line-${lineIndex}`}
                        from={from}
                    >
                        <LineText
                            line={line}
                            lineIndex={lineIndex}
                            translateYOffset={lineOffsets[lineIndex]}
                            fontType={line.font_type}
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};