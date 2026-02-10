import { getStyleConfig } from '../config/styleConfigs';
import { SubtitleStyleConfig } from '../../types/style';
import { SubtitleGroup } from '../../types/subtitles';
import { ThreeLines } from './Subtitles/ThreeLines';

type StyleRendererProps = {
    group: SubtitleGroup;
    style?: string;
    captionPadding?: number;
    customConfigs?: Record<string, SubtitleStyleConfig>; // Add this
};

export const StyleRenderer: React.FC<StyleRendererProps> = ({
    group,
    style = 'basic',
    captionPadding,
    customConfigs
}) => {
    const config = getStyleConfig(style, customConfigs);

    switch (style) {
        case 'matt':
        case 'jess':
        case 'jack':
        case 'basic':
        default:
            return <ThreeLines
                group={group}
                config={config}
                captionPadding={captionPadding}
            />;
    }
};