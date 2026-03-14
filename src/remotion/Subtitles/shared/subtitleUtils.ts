// shared/subtitleUtils.ts


import { Line } from '../../../../types/subtitles';
import { SubtitleStyleConfig, FontStyleDefinition, AnimationType } from '../../../../types/style';


export const ANIMATION_ANTICIPATION_FRAMES = 4;
export const FADE_OUT_DURATION_FRAMES = 30;
export const MAX_WORD_DISPLAY_SECONDS = 3;

export const getFontStyle = (
    config: SubtitleStyleConfig,
    fontType: string
): FontStyleDefinition =>
    config.fonts[fontType as keyof typeof config.fonts] ?? {
        fontSize: 60, fontWeight: 400, fontFamily: 'Arial', color: '#ffffff',
    };

export const getAnimationType = (
    config: SubtitleStyleConfig,
    fontType: string
): AnimationType =>
    config.fonts[fontType as keyof typeof config.fonts]?.animationType ?? 'fade-blur';

export const measureTextWidth = (text: string, style: FontStyleDefinition): number => {
    const span = document.createElement('span');
    Object.assign(span.style, {
        position: 'absolute', visibility: 'hidden', whiteSpace: 'nowrap',
        fontSize: `${style.fontSize}px`, fontFamily: `"${style.fontFamily}", sans-serif`,
        fontWeight: String(style.fontWeight), fontStyle: style.fontStyle ?? 'normal',
        textTransform: style.uppercase ? 'uppercase' : 'none',
    });
    span.textContent = text;
    document.body.appendChild(span);
    const width = span.offsetWidth;
    document.body.removeChild(span);
    return width;
};

export const measureActualTextHeight = (
    text: string,
    style: FontStyleDefinition,
    containerWidth: number,
    scale = 1
): number => {
    const div = document.createElement('div');
    Object.assign(div.style, {
        position: 'absolute', visibility: 'hidden', lineHeight: '1.0',
        fontSize: `${style.fontSize * scale}px`, fontFamily: `"${style.fontFamily}", sans-serif`,
        fontWeight: String(style.fontWeight), fontStyle: style.fontStyle ?? 'normal',
        width: `${containerWidth}px`, display: 'flex', flexWrap: 'wrap',
        justifyContent: 'center', alignItems: 'baseline',
        textTransform: style.uppercase ? 'uppercase' : 'none',
    });
    text.split(' ').forEach((word) => {
        const span = document.createElement('span');
        span.style.cssText = 'display:inline-block;margin-right:0.3em';
        span.textContent = word;
        div.appendChild(span);
    });
    document.body.appendChild(div);
    const height = div.offsetHeight;
    document.body.removeChild(div);
    return height;
};

// Add this constant near MAX_FONT_SIZE
export const MAX_FONT_SIZE = 150;
export const MAX_NON_BOLD_FONT_SIZE = 75; // cap for normal/italic lines



export const calculateFontScales = (
    lines: Line[],
    config: SubtitleStyleConfig,
    scale: number = 1,         // pass (height / 1920) from the component
): number[] => {
    if (!lines.length) return [];

    const maxFontSize = MAX_FONT_SIZE * scale;
    const maxNonBoldSize = MAX_NON_BOLD_FONT_SIZE * scale;

    const widths = lines.map((line) => {
        const style = getFontStyle(config, line.font_type);
        return {
            width: measureTextWidth(line.words.map(w => w.word).join(' '), style),
            fontSize: style.fontSize,
            isBold: line.font_type === 'bold',
        };
    });

    const maxWidth = Math.max(...widths.map(lw => lw.width));

    return widths.map(({ width, fontSize, isBold }) => {
        if (!width) return 1;
        const raw = maxWidth / width;
        const cap = isBold ? maxFontSize : maxNonBoldSize;
        return fontSize * raw > cap ? cap / fontSize : raw;
    });
};

export const calculateLinePositions = (
    lines: Line[],
    config: SubtitleStyleConfig,
    containerWidth: number,
    fontScales: number[],
    lineSpacing = 0
): number[] => {
    if (!lines.length) return [];
    const heights = lines.map((line, i) => {
        const style = getFontStyle(config, line.font_type);
        const text = line.words.map(w => w.word).join(' ');
        return measureActualTextHeight(text, style, containerWidth, fontScales[i] ?? 1);
    });
    return heights.reduce<number[]>((acc, h, i) => {
        acc.push(i === 0 ? 0 : acc[i - 1] + heights[i - 1] + lineSpacing);
        return acc;
    }, []);
};

export const buildTextShadow = (style: FontStyleDefinition): string => {
    const shadows = ['2px 2px 2px rgba(0,0,0,0.4)'];
    if (style.shadow && style.shadow !== 'none') {
        const blur = style.shadow === 'small' ? 10 : style.shadow === 'medium' ? 20 : 30;
        shadows.push(`0 0 ${blur}px ${style.shadowColor ?? style.color ?? '#ffffff'}`);
    }
    if (style.strokeWeight && style.strokeWeight !== 'none') {
        const w = style.strokeWeight === 'small' ? 1 : style.strokeWeight === 'medium' ? 2 : 3;
        const c = style.strokeColor ?? '#000000';
        for (let x = -w; x <= w; x++)
            for (let y = -w; y <= w; y++)
                if (x || y) shadows.push(`${x}px ${y}px 0 ${c}`);
    }
    return shadows.join(', ');
};

export const scaleConfig = (
    rawConfig: SubtitleStyleConfig,
    height: number
): SubtitleStyleConfig => {
    const scale = height / 1920;
    return {
        ...rawConfig,
        fonts: Object.fromEntries(
            Object.entries(rawConfig.fonts).map(([k, style]) => [
                k, { ...style, fontSize: Math.round(style.fontSize * scale) },
            ])
        ) as SubtitleStyleConfig['fonts'],
    };
};