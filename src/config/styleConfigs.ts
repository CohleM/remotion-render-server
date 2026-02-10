import { SubtitleStyleConfig } from '../../types/style';
// import { loadFont } from '@remotion/google-fonts';

// Import all fonts you might use
import { loadFont as loadInter } from '@remotion/google-fonts/Inter';
import { loadFont as loadBebas } from '@remotion/google-fonts/BebasNeue';
import { loadFont as loadPoppins } from '@remotion/google-fonts/Poppins';
import { loadFont as loadMontserrat } from '@remotion/google-fonts/Montserrat';
import { loadFont as loadOswald } from '@remotion/google-fonts/Oswald';

// Load fonts to make them available
loadInter();
loadBebas();
loadPoppins();
loadMontserrat();
loadOswald();

export const defaultStyleConfigs: Record<string, SubtitleStyleConfig> = {
    matt: {
        id: 'matt',
        name: 'Matt',
        category: 'New',
        isNew: true,
        fonts: {
            bold: {
                fontSize: 120,
                fontWeight: 800,
                fontFamily: 'Bebas Neue',
                color: '#ffffff',
                uppercase: true
            },
            thin: {
                fontSize: 50,
                fontWeight: 300,
                fontFamily: 'Inter',
                color: '#ffffff'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff'
            },
            italic: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff'
            }
        }
    },
    basic: {
        id: 'basic',
        name: 'Basic ThreeLines',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 120,
                fontWeight: 800,
                fontFamily: 'Bebas Neue',
                color: '#ffffff'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff'
            },
            italic: {
                fontSize: 60,
                fontWeight: 200,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff'
            }
        }
    },
    // Add other styles (jess, jack, etc.) here...
};

// Helper to get config with overrides
export const getStyleConfig = (
    styleId: string,
    customConfigs?: Record<string, SubtitleStyleConfig>
): SubtitleStyleConfig => {
    const custom = customConfigs?.[styleId];
    const defaultConfig = defaultStyleConfigs[styleId] || defaultStyleConfigs.basic;

    if (!custom) return defaultConfig;

    return {
        ...defaultConfig,
        ...custom,
        fonts: {
            ...defaultConfig.fonts,
            ...custom.fonts
        }
    };
};