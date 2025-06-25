"use client";

export default function HiddenGems({ data }) {
    if (!data || !data.gems || data.gems.length === 0) {
        return <p className="text-[var(--color-text-secondary)]">No hidden gems data to display.</p>;
    }

    const { gems } = data;

    return (
        <div className="space-y-4">
            <p className="text-lg mb-2">
                The {gems.length} hidden gem{gems.length > 1 ? 's' : ''} you found that blew up are:
            </p>
            <ul className="space-y-3">
                {gems.map((gem, index) => (
                    <li key={gem.business_id || index} className="p-3 bg-[var(--background)] rounded-md shadow">
                        <h3 className="text-md font-semibold text-[var(--color-accent-primary)] mb-1">{gem.business_name}</h3>
                        <div className="text-sm text-[var(--color-text-secondary)]">
                            <p>Reviewed on: <span className="text-[var(--color-foreground)]">{new Date(gem.user_review_date).toLocaleDateString()}</span></p>
                            <p>Reviews then: <span className="text-[var(--color-foreground)]">{gem.reviews_at_time}</span></p>
                            <p>Reviews now: <span className="text-[var(--color-foreground)]">{gem.current_review_count}</span></p>
                        </div>
                    </li>
                ))}
            </ul>
        </div>
    );
}
