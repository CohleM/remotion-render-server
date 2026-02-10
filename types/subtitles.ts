// src/app/shared/types/subtitles.ts
export type Word = {
    id: string;
    word: string;
    start: number;
    end: number;
};

export type Line = {
    id: string;
    text: string;
    start: number;
    end: number;
    font_type: 'bold' | 'thin' | 'normal' | 'italic';
    words: Word[];
};

export type SubtitleGroup = {
    id: string;
    start: number;
    end: number;
    group_text?: string;
    lines: Line[];
    function?: string;
};