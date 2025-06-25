"use client";

import { useState } from 'react';
import { scaleLinear } from 'd3-scale'; // Using d3-scale for better color interpolation

// --- Configuration ---
const daysOfWeek = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"];
const hoursOfDay = Array.from({ length: 24 }, (_, i) => i); // 0-23
const CELL_PADDING = 1; // Space between cells
const CELL_SIZE = 18; // Size of each cell
const Y_AXIS_WIDTH = 40; // Space for day labels
const X_AXIS_HEIGHT = 30; // Space for hour labels

// --- Color Scale ---
// Using d3-scale for a smoother gradient from a very light grey/green to Spotify green
// Adjust the range colors as needed
const colorScale = scaleLinear()
    .range(['#282828', '#1DB954']) // From dark grey to Spotify green
    .clamp(true); // Clamp values outside the domain

// --- Component ---
export default function ReviewRhythm({ data }) {
    const [hoveredCell, setHoveredCell] = useState(null); // { day: number, hour: number, count: number }

    if (!data || !data.data || Object.keys(data.data).length === 0) {
        return <p className="text-[var(--color-text-secondary)]">Review rhythm data is not available.</p>;
    }

    // --- Data Processing ---
    let processedDataMap = new Map(); // Use a Map for efficient lookup: "day-hour" -> count
    let maxValue = 0;
    for (const dayStr in data.data) {
        const day = parseInt(dayStr); // API day is 1-7
        for (const hourStr in data.data[dayStr]) {
            const hour = parseInt(hourStr);
            const count = data.data[dayStr][hourStr];
            processedDataMap.set(`${day - 1}-${hour}`, count); // Key: "0-0", "0-1", ... "6-23"
            if (count > maxValue) {
                maxValue = count;
            }
        }
    }
    // Update color scale domain based on max value
    colorScale.domain([0, Math.max(maxValue, 1)]); // Ensure domain is at least [0, 1]

    // --- Dimensions ---
    const chartWidth = Y_AXIS_WIDTH + hoursOfDay.length * (CELL_SIZE + CELL_PADDING);
    const chartHeight = X_AXIS_HEIGHT + daysOfWeek.length * (CELL_SIZE + CELL_PADDING);

    // --- Tooltip ---
    const renderTooltip = () => {
        if (!hoveredCell) return null;
        const { day, hour, count } = hoveredCell;
        // Position tooltip near the hovered cell
        const xPos = Y_AXIS_WIDTH + hour * (CELL_SIZE + CELL_PADDING) + CELL_SIZE / 2;
        const yPos = X_AXIS_HEIGHT + day * (CELL_SIZE + CELL_PADDING) - 10; // Position above cell

        return (
            <div
                className="absolute p-2 bg-[var(--color-background)] text-xs text-[var(--color-foreground)] border border-[var(--color-accent-primary)] rounded shadow-lg pointer-events-none"
                style={{
                    left: `${xPos}px`,
                    top: `${yPos}px`,
                    transform: 'translate(-50%, -100%)', // Center above point
                    whiteSpace: 'nowrap',
                    zIndex: 10,
                }}
            >
                <p>{`${daysOfWeek[day]}, ${hour}:00 - ${hour + 1}:00`}</p>
                <p>Reviews: <span className="font-bold">{count}</span></p>
            </div>
        );
    };

    // --- Rendering ---
    return (
        <div className="relative" style={{ width: chartWidth, height: chartHeight }}>
            <svg width={chartWidth} height={chartHeight}>
                <g transform={`translate(${Y_AXIS_WIDTH}, ${X_AXIS_HEIGHT})`}>
                    {/* Render Cells */}
                    {daysOfWeek.map((_, dayIndex) => (
                        hoursOfDay.map((hour) => {
                            const count = processedDataMap.get(`${dayIndex}-${hour}`) || 0;
                            const isHovered = hoveredCell && hoveredCell.day === dayIndex && hoveredCell.hour === hour;

                            return (
                                <rect
                                    key={`cell-${dayIndex}-${hour}`}
                                    x={hour * (CELL_SIZE + CELL_PADDING)}
                                    y={dayIndex * (CELL_SIZE + CELL_PADDING)}
                                    width={CELL_SIZE}
                                    height={CELL_SIZE}
                                    fill={colorScale(count)}
                                    style={{
                                        transition: 'filter 0.15s ease-out',
                                        filter: isHovered ? 'brightness(1.4)' : 'brightness(1)', // Subtle brightness hover
                                        cursor: 'pointer'
                                    }}
                                    rx={2} // Slightly rounded corners
                                    ry={2}
                                    onMouseEnter={() => setHoveredCell({ day: dayIndex, hour, count })}
                                    onMouseLeave={() => setHoveredCell(null)}
                                />
                            );
                        })
                    ))}
                </g>

                {/* Render Y Axis Labels (Days) */}
                <g transform={`translate(0, ${X_AXIS_HEIGHT})`}>
                    {daysOfWeek.map((dayLabel, index) => (
                        <text
                            key={`y-label-${index}`}
                            x={Y_AXIS_WIDTH - 8} // Position right of axis line
                            y={index * (CELL_SIZE + CELL_PADDING) + CELL_SIZE / 2}
                            dy="0.35em" // Vertical alignment
                            textAnchor="end"
                            fontSize="10"
                            fill="var(--color-text-secondary)"
                        >
                            {dayLabel}
                        </text>
                    ))}
                </g>

                {/* Render X Axis Labels (Hours) */}
                <g transform={`translate(${Y_AXIS_WIDTH}, 0)`}>
                    {hoursOfDay.map((hour) => (
                        (hour % 3 === 0) && ( // Show label every 3 hours
                            <text
                                key={`x-label-${hour}`}
                                x={hour * (CELL_SIZE + CELL_PADDING) + CELL_SIZE / 2}
                                y={X_AXIS_HEIGHT - 8} // Position above axis line
                                textAnchor="middle"
                                fontSize="10"
                                fill="var(--color-text-secondary)"
                            >
                                {`${hour}:00`}
                            </text>
                        )
                    ))}
                </g>
            </svg>
            {/* Render Tooltip outside SVG */}
            {renderTooltip()}
        </div>
    );
}
