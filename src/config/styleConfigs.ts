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
                fontSize: 60,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f6ff4d',
                uppercase: false,
                animationType: 'slide-up'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                animationType: 'fade-blur'
            },
            normal: {
                fontSize: 60,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#ffffff',
                animationType: 'fade-blur'
            },
            italic: {
                fontSize: 60,
                fontWeight: 300,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff',
                animationType: 'slide-up'
            }
        }
    },
    GBI: {
        id: 'GBI',
        name: 'Gradient Base Italic',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 60,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f6ff4d',
                uppercase: true,
                animationType: 'slide-up'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                animationType: 'fade-blur'
            },
            normal: {
                fontSize: 60,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#ffffff',
                animationType: 'fade-blur'
            },
            italic: {
                fontSize: 100,
                fontWeight: 700,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#f6ff4d',
                animationType: 'slide-up'
            }
        }
    },
    FaB: {
        id: 'FaB',
        name: 'Fade And Blur',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 80,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f6ff4d',
                uppercase: true,
                animationType: 'fade-blur'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                animationType: 'fade-blur'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'

            },
            italic: {
                fontSize: 60,
                fontWeight: 200,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff',
                animationType: 'fade-blur'
            }
        }
    },
    FaB1: {
        id: 'FaB1',
        name: 'Fade And Blur1',
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
                fontWeight: 600,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
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
    Combo: {
        id: 'Combo',
        name: 'Combo',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 100,
                fontWeight: 900,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
            },
            italic: {
                fontSize: 60,
                fontWeight: 300,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
            }
        }
    },
    NaI: {
        id: 'NaI',
        name: 'Normal and Italic',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 100,
                fontWeight: 800,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'

            },
            italic: {
                fontSize: 100,
                fontWeight: 700,
                fontFamily: 'Cormorant Garamond',
                fontStyle: 'italic',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'
            }
        }
    },
    NaB: {
        id: 'NaB',
        name: 'Normal and Bold',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 80,
                fontWeight: 900,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'

            },
            italic: {
                fontSize: 100,
                fontWeight: 700,
                fontFamily: 'Cormorant Garamond',
                fontStyle: 'italic',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'fade-blur'
            }
        }
    },
    Glow: {
        id: 'Glow',
        name: 'Glow',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 80,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f9fc19',
                uppercase: false,
                shadow: "large",
                shadowColor: '#f9fc19',
                animationType: 'slide-up'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            italic: {
                fontSize: 60,
                fontWeight: 200,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            }
        }
    },
    GlowI: {
        id: 'GlowI',
        name: 'Glow Italic',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 80,
                fontWeight: 800,
                fontFamily: 'Inter',
                color: '#f9fc19',
                uppercase: false,
                shadow: "large",
                shadowColor: '#f9fc19',
                animationType: 'slide-up'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            italic: {
                fontSize: 100,
                fontWeight: 700,
                fontFamily: 'Cormorant Garamond',
                fontStyle: 'italic',
                color: '#f9fc19',
                shadow: "large",
                shadowColor: '#f9fc19',
                animationType: 'slide-up'
            }
        }
    },
    EW: {
        id: 'EW',
        name: 'Equal Width',
        category: 'All',
        fonts: {
            bold: {
                fontSize: 100,
                fontWeight: 900,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            thin: {
                fontSize: 50,
                fontWeight: 100,
                fontFamily: 'Inter',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            normal: {
                fontSize: 60,
                fontWeight: 400,
                fontFamily: 'Poppins',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
            },
            italic: {
                fontSize: 60,
                fontWeight: 300,
                fontFamily: 'Inter',
                fontStyle: 'italic',
                color: '#ffffff',
                shadow: "large",
                shadowColor: '#ffffff',
                animationType: 'slide-up'
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
    { id: 'GB', name: 'Gradient Base', category: 'All' },
    { id: 'GBI', name: 'Gradient Base Italic', category: 'All' },
    { id: 'EW', name: 'Equal Width', category: 'All', isPremium: true },
    { id: 'Glow', name: 'Glow', category: 'All' },
    { id: 'GlowI', name: 'Glow Italic', category: 'All' },
    { id: 'FaB', name: 'Fade And Blur', category: 'All', },
    { id: 'Combo', name: 'Combo', category: 'Premium' },
    { id: 'NaI', name: 'Normal and Italic', category: 'All', },
    { id: 'NaB', name: 'Normal and Bold', category: 'All', },
];

export const categories = ['All', 'Premium'] as const;
