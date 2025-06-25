"use client";

import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';

export default function SentimentTimeline({ data }) {
    if (!data || !data.timeline || Object.keys(data.timeline).length === 0) {
        return <p className="text-[var(--color-text-secondary)]">Sentiment timeline data is not available.</p>;
    }

    // Transform timeline data from {year: score} to [{year: year, score: score}, ...]
    // And sort by year to ensure the line chart draws correctly
    const chartData = Object.entries(data.timeline)
        .map(([year, score]) => ({
            year: parseInt(year), // Ensure year is a number for proper sorting and axis display
            moodScore: parseFloat(score.toFixed(2)) // Ensure score is a number and formatted
        }))
        .sort((a, b) => a.year - b.year);

    // Determine Y-axis domain if needed, e.g., from 0 to 1 or based on data range
    // For mood score (0.7 * stars/5 + 0.3 * polarity), range is roughly -0.3 to 1.0
    // Let's set a sensible default or calculate dynamically.
    // For simplicity, Recharts will auto-detect domain, but we can specify if needed.
    // const yDomain = [-0.3, 1]; // Example fixed domain

    return (
        <div style={{ width: '100%', height: 300 }}>
            <ResponsiveContainer>
                <LineChart
                    data={chartData}
                    margin={{
                        top: 5,
                        right: 30,
                        left: 0, // Adjusted left margin for Y-axis labels
                        bottom: 5,
                    }}
                >
                    <CartesianGrid strokeDasharray="3 3" stroke="var(--color-text-secondary, #4A5568)" />
                    <XAxis
                        dataKey="year"
                        tick={{ fill: 'var(--color-text-secondary, #A0AEC0)', fontSize: 12 }}
                        stroke="var(--color-text-secondary, #718096)"
                    />
                    <YAxis
                        tick={{ fill: 'var(--color-text-secondary, #A0AEC0)', fontSize: 12 }}
                        stroke="var(--color-text-secondary, #718096)"
                        // domain={yDomain} // Optional: if you want to fix the Y-axis scale
                        tickFormatter={(value) => value.toFixed(1)} // Format Y-axis ticks
                    />
                    <Tooltip
                        contentStyle={{
                            backgroundColor: 'var(--background, #1A202C)',
                            borderColor: 'var(--color-accent-primary, #38A169)',
                            color: 'var(--color-foreground, #FFFFFF)'
                        }}
                        itemStyle={{ color: 'var(--color-foreground, #FFFFFF)' }}
                        cursor={{ fill: 'rgba(var(--color-accent-primary-rgb, 29, 185, 84), 0.1)' }} // Assuming accent primary RGB
                    />
                    <Legend wrapperStyle={{ color: 'var(--color-text-secondary, #A0AEC0)' }} />
                    <Line
                        type="monotone"
                        dataKey="moodScore"
                        name="Avg. Mood Score"
                        stroke="var(--color-accent-primary, #38A169)"
                        strokeWidth={2}
                        activeDot={{ r: 6, fill: 'var(--color-accent-primary, #38A169)', stroke: 'var(--background, #1A202C)', strokeWidth: 2 }}
                        dot={{ r: 4, fill: 'var(--color-accent-primary, #38A169)' }}
                        animationDuration={1500} // Animation for the line drawing in
                        animationEasing="ease-in-out"
                    />
                </LineChart>
            </ResponsiveContainer>
        </div>
    );
}
