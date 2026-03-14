// shared/SubtitlePrimitives.tsx
import { memo, useMemo, useState, useEffect } from 'react';
import { interpolate, spring, useCurrentFrame, useVideoConfig, delayRender, continueRender } from 'remotion';
import { AnimationType, SubtitleStyleConfig } from '../../../../types/style';
import {
    ANIMATION_ANTICIPATION_FRAMES
} from './subtitleUtils';

// Font preloads (only need to happen once, anywhere)
import { loadFont as loadInter } from '@remotion/google-fonts/Inter';
import { loadFont as loadBebas } from '@remotion/google-fonts/BebasNeue';
import { loadFont as loadPoppins } from '@remotion/google-fonts/Poppins';
import { loadFont as loadMontserrat } from '@remotion/google-fonts/Montserrat';
import { loadFont as loadOswald } from '@remotion/google-fonts/Oswald';

loadInter(); loadBebas(); loadPoppins(); loadMontserrat(); loadOswald();

export const useFontsLoaded = (config: SubtitleStyleConfig) => {
    const [loaded, setLoaded] = useState(false);
    const [handle] = useState(() => delayRender('Loading Google Fonts'));
    useEffect(() => {
        Promise.all(
            Object.values(config.fonts).map(style =>
                document.fonts.load(`${style.fontStyle ?? 'normal'} ${style.fontWeight} ${style.fontSize}px "${style.fontFamily}"`)
            )
        )
            .catch(err => console.warn('Font loading failed:', err))
            .finally(() => { setLoaded(true); continueRender(handle); });
    }, [config, handle]);
    return loaded;
};

export const useWordAnimation = (
    animationType: AnimationType,
    frame: number,
    fps: number,
    wordStartFrame: number
) => {
    const springValue = useMemo(() => spring({
        frame: Math.max(0, frame - wordStartFrame),
        fps,
        config: { damping: 100, stiffness: 100 },
    }), [frame, wordStartFrame, fps]);

    return useMemo(() => {
        const t = springValue;
        const opacity = interpolate(t, [0, 1], [0, 1]);
        const blur = (from: number) => `blur(${interpolate(t, [0, 1], [from, 0])}px)`;
        switch (animationType) {
            case 'slide-up': return { transform: `translateY(${interpolate(t, [0, 1], [50, 0])}px)`, opacity, filter: blur(4) };
            case 'slide-down': return { transform: `translateY(${interpolate(t, [0, 1], [-50, 0])}px)`, opacity, filter: blur(4) };
            case 'slide-left': return { transform: `translateX(${interpolate(t, [0, 1], [100, 0])}px)`, opacity, filter: blur(4) };
            case 'slide-right': return { transform: `translateX(${interpolate(t, [0, 1], [-100, 0])}px)`, opacity, filter: blur(4) };
            case 'scale': return { transform: `scale(${interpolate(t, [0, 1], [0.5, 1])})`, opacity, filter: blur(4) };
            case 'fade-blur': return { transform: 'none', opacity, filter: blur(10) };
            default: return { transform: `translateY(${interpolate(t, [0, 1], [50, 0])}px)`, opacity, filter: blur(4) };
        }
    }, [animationType, springValue]);
};
export const WordText = memo(function WordText({
    word, wordStart, wordEnd, lineStart, animationType,
}: {
    word: string; wordIndex: number;
    lineStart: number; wordStart: number; wordEnd: number;
    animationType: AnimationType;
}) {
    const frame = useCurrentFrame();
    const { fps } = useVideoConfig();

    const startFrame = Math.round((wordStart - lineStart) * fps) - ANIMATION_ANTICIPATION_FRAMES;
    const entryAnimation = useWordAnimation(animationType, frame, fps, startFrame);

    return (
        <span style={{
            display: 'inline-block',
            transform: entryAnimation.transform,
            opacity: entryAnimation.opacity,
            filter: entryAnimation.filter,
            marginRight: '0.3em',
            whiteSpace: 'pre',
            willChange: 'transform, opacity, filter',
        }}>
            {word}
        </span>
    );
});