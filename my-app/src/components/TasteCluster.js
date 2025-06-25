"use client";

export default function TasteCluster({ data }) {
    if (!data || data.cluster_id === null || data.cluster_id === undefined) {
        return <p className="text-[var(--color-text-secondary)]">Taste cluster data is not available.</p>;
    }

    const { cluster_id, top_categories } = data;

    return (
        <div className="space-y-4">
            <p className="text-lg">
                You are part of Taste Cluster:
                <span className="font-bold text-[var(--color-accent-primary)] ml-2">{cluster_id}</span>
            </p>

            {top_categories && top_categories.length > 0 ? (
                <div>
                    <p className="text-md mb-2 text-[var(--color-text-secondary)]">You and your cluster of friends love:</p>
                    <div className="flex flex-wrap gap-2">
                        {top_categories.map(item => (
                            <div
                                key={item.category}
                                className="bg-[var(--background)] text-[var(--color-foreground)] px-3 py-1 rounded-full text-sm shadow"
                                title={`Count: ${item.count}`}
                            >
                                {item.category}
                            </div>
                        ))}
                    </div>
                </div>
            ) : (
                <p className="text-[var(--color-text-secondary)]">No specific top categories to show for this cluster.</p>
            )}
        </div>
    );
}
