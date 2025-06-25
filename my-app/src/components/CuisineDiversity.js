"use client";

export default function CuisineDiversity({ data }) {
    if (!data || !data.category_counts || data.diversity_score === undefined) {
        return <p className="text-[var(--color-text-secondary)]">Cuisine diversity data is not available.</p>;
    }

    const { category_counts, diversity_score } = data;

    let diversityLevel = "";
    if (diversity_score < 1.0) {
        diversityLevel = "Low Diversity";
    } else if (diversity_score < 2.0) {
        diversityLevel = "Medium Diversity";
    } else {
        diversityLevel = "High Diversity";
    }

    // Sort categories by count descending and take top 10
    const sortedCategories = Object.entries(category_counts)
        .sort(([, countA], [, countB]) => countB - countA)
        .slice(0, 10);

    return (
        <div className="space-y-4">
            <div>
                <p className="text-lg mb-1">Your top {sortedCategories.length > 0 ? Math.min(sortedCategories.length, 10) : ''} categories are:</p>
                {sortedCategories.length > 0 ? (
                    <ul className="space-y-1 pl-4">
                        {sortedCategories.map(([category, count]) => (
                            <li key={category} className="flex justify-between items-center">
                                <span className="text-md text-[var(--color-foreground)]">{category}</span>
                                <span className="text-sm text-[var(--color-text-secondary)] bg-[var(--background)] px-2 py-0.5 rounded">
                                    {count} review{count > 1 ? 's' : ''}
                                </span>
                            </li>
                        ))}
                    </ul>
                ) : (
                    <p className="text-[var(--color-text-secondary)] pl-4">No specific categories to show.</p>
                )}
            </div>
            <div>
                <p className="text-lg">
                    Diversity Score:
                    <span className="font-semibold text-[var(--color-accent-primary)] ml-2 mr-1">{diversity_score.toFixed(2)}</span>
                    (<span className="italic">{diversityLevel}</span>)
                </p>
            </div>
        </div>
    );
}
