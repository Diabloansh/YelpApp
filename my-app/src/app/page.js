"use client"; // Mark this as a Client Component

import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic'; // Import dynamic
import InfluencePercentile from '../components/InfluencePercentile';
import CuisineDiversity from '../components/CuisineDiversity';
import HiddenGems from '../components/HiddenGems';
import Recommendations from '../components/Recommendations';
import TasteCluster from '../components/TasteCluster';
// import WordSignature from '../components/WordSignature'; // Remove static import
import SentimentTimeline from '../components/SentimentTimeline';
import ReviewRhythm from '../components/ReviewRhythm';

// Dynamically import WordSignature with SSR disabled
const WordSignature = dynamic(() => import('../components/WordSignature'), {
  ssr: false,
  loading: () => <p className="text-[var(--color-text-secondary)]">Loading word cloud...</p>
});

export default function HomePage() {
  const [userId, setUserId] = useState('');
  const [profileData, setProfileData] = useState(null);
  const [isLoading, setIsLoading] = useState(false);
  const [error, setError] = useState(null);

  const fetchProfileData = async () => {
    if (!userId.trim()) {
      setError("Please enter a User ID.");
      setProfileData(null);
      return;
    }
    setIsLoading(true);
    setError(null);
    setProfileData(null); // Clear previous data

    try {
      // Assume the backend is running on http://localhost:8000
      const response = await fetch(`http://localhost:8000/api/users/${userId}/full-profile`);
      if (!response.ok) {
        const errorData = await response.json().catch(() => ({ detail: "Failed to fetch data. Invalid JSON response." }));
        throw new Error(errorData.detail || `HTTP error! status: ${response.status}`);
      }
      const data = await response.json();
      setProfileData(data);
    } catch (err) {
      console.error("Fetch error:", err);
      setError(err.message || "An unexpected error occurred.");
      setProfileData(null);
    } finally {
      setIsLoading(false);
    }
  };

  const handleSubmit = (e) => {
    e.preventDefault();
    fetchProfileData();
  };

  return (
    <main className="flex min-h-screen flex-col items-center justify-start p-8 bg-[var(--color-background)] text-[var(--color-foreground)]">
      <div className="z-10 w-full max-w-5xl items-center justify-between font-mono text-sm lg:flex mb-12">
        <h1 className="text-4xl font-heading mb-4 text-center lg:text-left w-full">Your User Profile Insights</h1>
      </div>

      <form onSubmit={handleSubmit} className="mb-8 flex items-center gap-4">
        <input
          type="text"
          value={userId}
          onChange={(e) => setUserId(e.target.value)}
          placeholder="Enter User ID (e.g., u-xxxxx)"
          className="px-4 py-2 border border-gray-600 rounded-md bg-[var(--color-card-background)] text-[var(--color-foreground)] focus:ring-2 focus:ring-[var(--color-accent-primary)] focus:border-[var(--color-accent-primary)] outline-none"
        />
        <button
          type="submit"
          disabled={isLoading}
          className="px-6 py-2 bg-[var(--color-accent-primary)] text-white font-semibold rounded-md hover:opacity-90 disabled:opacity-50 disabled:cursor-not-allowed"
        >
          {isLoading ? 'Loading...' : 'Get Profile'}
        </button>
      </form>

      {error && (
        <div className="mb-8 p-4 text-red-400 bg-red-900/30 border border-red-700 rounded-md w-full max-w-md text-center">
          <p>Error: {error}</p>
        </div>
      )}

      {isLoading && (
        <div className="text-xl text-[var(--color-text-secondary)]">Fetching your profile data...</div>
      )}

      {profileData && !isLoading && (
        <div className="w-full max-w-5xl grid grid-cols-1 md:grid-cols-2 gap-4"> {/* Reduced gap */}
          {/* Top Row: Review Rhythm + Influence */}
          <div className="bg-[var(--color-card-background)] rounded-lg shadow-md md:col-span-2 grid grid-cols-1 lg:grid-cols-3 gap-4 p-4"> {/* Reduced padding and gap */}
            {/* Review Rhythm Section (takes 2/3 width on large screens) */}
            <div className="lg:col-span-2">
              <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Review Rhythm</h2> {/* Slightly smaller heading */}
              {profileData.review_rhythm ?
                <ReviewRhythm data={profileData.review_rhythm} /> :
                <p className="text-[var(--color-text-secondary)]">No Review Rhythm data available.</p>
              }
              {profileData.errors?.review_rhythm && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.review_rhythm}</p>}
            </div>
            {/* Influence Percentile Section (takes 1/3 width on large screens) */}
            <div className="lg:col-span-1 flex flex-col justify-center items-center lg:items-start">
              <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Influence</h2> {/* Slightly smaller heading */}
              {profileData.influence_percentile ?
                <InfluencePercentile data={profileData.influence_percentile} /> :
                <p className="text-[var(--color-text-secondary)]">No Influence Percentile data available.</p>
              }
              {profileData.errors?.influence_percentile && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.influence_percentile}</p>}
            </div>
          </div>

          {/* Other components remain below */}
          {/* Reduced padding (p-4) and gap (gap-4) for all cards */}
          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Cuisine Diversity</h2> {/* Smaller heading */}
            {profileData.cuisine_diversity ?
              <CuisineDiversity data={profileData.cuisine_diversity} /> :
              <p className="text-[var(--color-text-secondary)]">No Cuisine Diversity data available.</p>
            }
            {profileData.errors?.cuisine_diversity && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.cuisine_diversity}</p>}
          </div>

          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Sentiment Timeline</h2> {/* Smaller heading */}
            {profileData.sentiment_timeline ?
              <SentimentTimeline data={profileData.sentiment_timeline} /> :
              <p className="text-[var(--color-text-secondary)]">No Sentiment Timeline data available.</p>
            }
            {profileData.errors?.sentiment_timeline && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.sentiment_timeline}</p>}
          </div>

          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Word Signature</h2> {/* Smaller heading */}
            {profileData.word_signature ?
              <WordSignature data={profileData.word_signature} /> :
              <p className="text-[var(--color-text-secondary)]">No Word Signature data available.</p>
            }
            {profileData.errors?.word_signature && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.word_signature}</p>}
          </div>

          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Hidden Gems</h2> {/* Smaller heading */}
            {profileData.hidden_gems ?
              <HiddenGems data={profileData.hidden_gems} /> :
              <p className="text-[var(--color-text-secondary)]">No Hidden Gems data available.</p>
            }
            {profileData.errors?.hidden_gems && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.hidden_gems}</p>}
          </div>

          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Taste Cluster</h2> {/* Smaller heading */}
            {profileData.taste_cluster ?
              <TasteCluster data={profileData.taste_cluster} /> :
              <p className="text-[var(--color-text-secondary)]">No Taste Cluster data available.</p>
            }
            {profileData.errors?.taste_cluster && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.taste_cluster}</p>}
          </div>

          <div className="p-4 bg-[var(--color-card-background)] rounded-lg shadow-md"> {/* Reduced padding */}
            <h2 className="text-xl font-heading mb-2 text-[var(--color-accent-primary)]">Recommendations</h2> {/* Smaller heading */}
            {profileData.recommendations ?
              <Recommendations data={profileData.recommendations} /> :
              <p className="text-[var(--color-text-secondary)]">No Recommendations data available.</p>
            }
            {profileData.errors?.recommendations && <p className="text-red-400 text-sm mt-1">Error: {profileData.errors.recommendations}</p>}
          </div>

          {/* Influence Percentile is now combined with Review Rhythm */}
        </div>
      )}
    </main>
  );
}
