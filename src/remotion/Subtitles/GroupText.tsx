import React, { useMemo, useState, useEffect, useRef } from 'react';
import {
    AbsoluteFill,
    interpolate,
    useCurrentFrame,
    useVideoConfig,
    Sequence,
    delayRender,
    continueRender
} from 'remotion';

// Import Google Fonts
import { loadFont as loadInter } from '@remotion/google-fonts/Inter';
import { loadFont as loadMontserrat } from '@remotion/google-fonts/Montserrat';
import { loadFont as loadOswald } from '@remotion/google-fonts/BebasNeue';
import { loadFont as loadPlayfair } from '@remotion/google-fonts/PlayfairDisplay';

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
};

type SubtitleGroup = {
    function: string;
    group_text: string;
    lines: Line[];
    start: number;
    end: number;
};

const LINE_SPACING = 20; // Add some spacing between lines

// Initialize fonts
const inter = loadInter();
const montserrat = loadMontserrat();
const oswald = loadOswald();
const playfair = loadPlayfair();

// Each line can have its own fontSize, fontFamily, and fontWeight
const GOOGLE_FONT_STYLES = [
    { fontSize: 30, fontWeight: 200, ...inter },      // Line 0: Inter Bold
    { fontSize: 120, fontWeight: 800, ...oswald }, // Line 1: Montserrat Regular
    { fontSize: 70, fontWeight: 700, ...oswald },     // Line 2: Oswald Bold
];

const DEFAULT_STYLE = { fontSize: 60, fontFamily: 'Arial, sans-serif', fontWeight: 800 };

// Hook to wait for Google Fonts to load with their specific weights
const useFontsLoaded = () => {
    const [loaded, setLoaded] = useState(false);
    const [handle] = useState(() => delayRender('Loading Google Fonts'));

    useEffect(() => {
        const loadAllFonts = async () => {
            // Load each font with its specific weight and size
            const fontPromises = GOOGLE_FONT_STYLES.map(style =>
                document.fonts.load(`${style.fontWeight} ${style.fontSize}px "${style.fontFamily}"`)
            );

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
    containerWidth: number
): number => {
    const tempDiv = document.createElement('div');
    tempDiv.style.position = 'absolute';
    tempDiv.style.visibility = 'hidden';
    tempDiv.style.fontSize = `${fontSize}px`;
    tempDiv.style.fontFamily = `"${fontFamily}", sans-serif`;
    tempDiv.style.fontWeight = String(fontWeight);
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

    const lineHeights = lines.map((line, index) => {
        const style = GOOGLE_FONT_STYLES[index % GOOGLE_FONT_STYLES.length];
        const text = line.words.map(w => w.word).join(' ');
        return measureActualTextHeight(
            text,
            style.fontSize,
            style.fontFamily,
            style.fontWeight,
            containerWidth
        );
    });

    const offsets: number[] = [0]; // First line at top (translateY(0))

    for (let i = 1; i < lines.length; i++) {
        const previousHeight = lineHeights[i - 1];
        const previousOffset = offsets[i - 1];
        // Stack downward: previous position plus its actual height plus spacing
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

    const opacity = interpolate(animationFrame, [0, 4], [0, 1], { extrapolateRight: 'clamp' });
    const scale = interpolate(animationFrame, [0, 6], [0.95, 1], { extrapolateRight: 'clamp' });

    return (
        <span
            style={{
                display: 'inline-block',
                transform: `scale(${scale})`,
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
}> = ({ line, lineIndex, translateYOffset }) => {
    const style = GOOGLE_FONT_STYLES[lineIndex % GOOGLE_FONT_STYLES.length] || DEFAULT_STYLE;

    return (
        <AbsoluteFill
            style={{
                justifyContent: 'flex-start',
                alignItems: 'center',
                paddingTop: 1500,
                paddingRight: 20,
                paddingLeft: 20,
            }}
        >
            <div
                style={{
                    transform: `translateY(${translateYOffset}px)`,
                    fontSize: style.fontSize,
                    fontFamily: `"${style.fontFamily}", sans-serif`,
                    fontWeight: style.fontWeight,
                    color: 'white',
                    textAlign: 'center',
                    textShadow: '3px 3px 6px rgba(0,0,0,0.9)',
                    lineHeight: 1.0,
                    display: 'flex',
                    flexWrap: 'wrap',
                    justifyContent: 'center',
                    alignItems: 'baseline',
                    maxWidth: '90%', // Add max width to control wrapping
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

export const GroupText: React.FC<{ group: SubtitleGroup }> = ({ group }) => {
    const { fps, width } = useVideoConfig();
    const fontsLoaded = useFontsLoaded();

    if (!group?.lines?.length) {
        console.error('Invalid group data:', group);
        return null;
    }

    // Calculate container width for text measurement (90% of video width)
    const containerWidth = width * 0.9;

    const lineOffsets = useMemo(() => {
        if (!fontsLoaded) return [];
        return calculateLinePositions(group.lines, containerWidth);
    }, [group.lines, fontsLoaded, containerWidth]);

    if (!fontsLoaded || lineOffsets.length === 0) {
        return null;
    }

    console.log('Line offsets:', lineOffsets);

    return (
        <AbsoluteFill style={{
            // Additional safety padding at the outermost level
            padding: `0 ${80}px`,
            boxSizing: 'border-box',
        }}>
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
                        />
                    </Sequence>
                );
            })}
        </AbsoluteFill>
    );
};