// src/app/shared/types/style.ts
export type SubtitleStyle = {
    id: string;
    name: string;
    category: 'All' | 'Trend' | 'New' | 'Premium' | 'Emoji' | 'Speakers';
    preview?: string; // optional for now, can be image path
    isNew?: boolean;
    isPremium?: boolean;
};
export type AnimationType =
    | 'slide-up'
    | 'slide-down'
    | 'slide-left'
    | 'slide-right'
    | 'scale'
    | 'fade-blur';
export interface FontStyleDefinition {
    fontSize: number;
    fontWeight: number;
    fontFamily: string;
    fontStyle?: 'normal' | 'italic';
    color?: string;
    uppercase?: boolean;
    strokeWeight?: 'none' | 'small' | 'medium' | 'large';
    strokeColor?: string;
    shadow?: 'none' | 'small' | 'medium' | 'large';
    shadowColor?: string;
    animationType?: AnimationType;
}

export interface SubtitleStyleConfig {
    id: string;
    name: string;
    category: string;
    isNew?: boolean;
    isPremium?: boolean;
    fonts: {
        bold: FontStyleDefinition;
        thin: FontStyleDefinition;
        normal: FontStyleDefinition;
        italic: FontStyleDefinition;
    };
}

export type FontType = 'bold' | 'thin' | 'normal' | 'italic';