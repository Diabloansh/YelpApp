"use client";

export default function Recommendations({ data }) {
    if (!data || !data.recommendations || data.recommendations.length === 0) {
        return <p className="text-[var(--color-text-secondary)]">No recommendations available at this time.</p>;
    }

    const { recommendations } = data;

    return (
        <div className="space-y-4">
            <p className="text-lg mb-2">
                Based on your preferences, you might like:
            </p>
            <ul className="space-y-3">
                {recommendations.map((rec, index) => (
                    <li key={rec.business_id || index} className="p-3 bg-[var(--background)] rounded-md shadow">
                        {/* MODIFIED SECTION for Name and Rating */}
                        <div className="flex justify-between items-center mb-1">
                            <h3 className="text-md font-semibold text-[var(--color-accent-primary)]">
                                {rec.name}
                            </h3>
                            {/* Display avgStar if available */}
                            {typeof rec.avgStar === 'number' && (
                                <span className="text-sm text-gray-400 flex items-center">
                                    {/* 
                                        Using a simple unicode star. 
                                        Replace with an icon component if preferred: e.g., <StarIcon className="h-4 w-4 text-yellow-400 mr-1" /> 
                                    */}
                                    {rec.avgStar.toFixed(1)}
                                    <span className="ml-1">⭐</span>
                                </span>
                            )}
                        </div>

                        {rec.categories && rec.categories.length > 0 && (
                            <div className="text-xs mt-1"> {/* Added mt-1 for a little space */}
                                {rec.categories.map(category => (
                                    <span
                                        key={category}
                                        className="inline-block bg-gray-700 text-gray-300 px-2 py-0.5 rounded-full mr-1 mb-1"
                                    >
                                        {category}
                                    </span>
                                ))}
                            </div>
                        )}
                    </li>
                ))}
            </ul>
        </div>
    );
}