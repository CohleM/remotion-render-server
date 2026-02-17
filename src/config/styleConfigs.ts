import { SubtitleStyle, SubtitleStyleConfig } from '../../types/style';
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
    ThreeLines: {
        id: 'ThreeLines',
        name: 'ThreeLines',
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
    jack: {
        id: 'jack',
        name: 'jack',
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

    nick: {
        id: 'nick',
        name: 'nick',
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

    laura: {
        id: 'laura',
        name: 'laura',
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
    }, caleb: {
        id: 'caleb',
        name: 'caleb',
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
    kendrick: {
        id: 'kendrick',
        name: 'kendrick',
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
    lewis: {
        id: 'lewis',
        name: 'lewis',
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
    GB: {
        id: 'GB',
        name: 'Gradient Base',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 80,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f6ff4d',
                uppercase: true
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff'
            },
            normal: {
                fontSize: 60,
                fontWeight: 800,
                fontFamily: 'Inter',
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


export const styles: SubtitleStyle[] = [
    { id: 'matt', name: 'Matt', category: 'New', isNew: true },
    { id: 'ThreeLines', name: 'ThreeLines', category: 'Trend', isNew: true },
    { id: 'jack', name: 'Jack', category: 'New', isNew: true },
    { id: 'nick', name: 'Nick', category: 'New', isNew: true },
    { id: 'laura', name: 'Laura', category: 'Trend' },
    { id: 'kelly2', name: 'Kelly 2', category: 'Premium', isPremium: true },
    { id: 'caleb', name: 'Caleb', category: 'All' },
    { id: 'kendrick', name: 'Kendrick', category: 'Trend' },
    { id: 'lewis', name: 'Lewis', category: 'Premium', isPremium: true },
    { id: 'GB', name: 'Gradient Base', category: 'All' },
    { id: 'carlos', name: 'Carlos', category: 'All' },
    { id: 'luke', name: 'Luke', category: 'Speakers' },
    { id: 'mark', name: 'Mark', category: 'Premium', isPremium: true },
    { id: 'sara', name: 'Sara', category: 'Premium', isPremium: true },
    { id: 'daniel', name: 'Daniel', category: 'Premium', isPremium: true },
    { id: 'dan2', name: 'Dan 2', category: 'All' },
    { id: 'hormozi4', name: 'Hormozi 4', category: 'Trend', isPremium: true },
    { id: 'basic', name: 'Basic ThreeLines', category: 'All' },
];

export const categories = ['All', 'Trend', 'New', 'Premium', 'Emoji', 'Speakers'] as const;
