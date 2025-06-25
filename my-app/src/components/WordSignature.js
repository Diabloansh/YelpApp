"use client";

import { WordCloud } from '@isoterik/react-word-cloud';
import { useState, useCallback } from 'react';

const defaultFontSize = (word) => Math.sqrt(word.value) * 1.8;

const SelfContainedWordRenderer = ({ wordData }) => {
    const [isHovered, setIsHovered] = useState(false);

    const handleMouseOver = () => setIsHovered(true);
    const handleMouseOut = () => setIsHovered(false);

    return (
        <text
            key={wordData.text + wordData.value}
            textAnchor="middle"
            transform={`translate(${wordData.x}, ${wordData.y}) rotate(${wordData.rotate})`}
            style={{
                fontSize: wordData.size,
                fontFamily: wordData.font,
                fontWeight: wordData.weight,
                fill: 'var(--color-accent-primary)',
                cursor: 'pointer',
                transition: 'filter 0.2s ease-out',
                filter: isHovered ? 'brightness(1.5)' : 'brightness(1)',
            }}
            onMouseOver={handleMouseOver}
            onMouseOut={handleMouseOut}
        >
            {wordData.text}
        </text>
    );
};

export default function WordSignature({ data }) {
    if (!data || !data.signature || data.signature.length === 0) {
        return <p className="text-[var(--color-text-secondary)]">Word signature data is not available.</p>;
    }

    const words = data.signature.map(item => ({
        text: item.term,
        value: item.score * 1000,
    }));

    if (typeof window === 'undefined') {
        return null;
    }

    const renderWordWithLocalHover = useCallback((wordData) => (
        <SelfContainedWordRenderer wordData={wordData} />
    ), []);

    return (
        <div style={{ width: '100%', height: '400px' }}>
            <WordCloud
                words={words}
                width={500}
                height={400}
                font={'var(--font-inter)'}
                fontSize={defaultFontSize}
                rotate={() => (Math.random() > 0.7 ? (Math.random() > 0.5 ? 90 : -90) : 0)}
                padding={7}
                enableTooltip={true}
                renderWord={renderWordWithLocalHover}
            />
        </div>
    );
}