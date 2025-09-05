/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.streaming;

import org.opensearch.common.metrics.CounterMetric;
import org.opensearch.common.metrics.MeanMetric;
import org.opensearch.telemetry.metrics.Counter;
import org.opensearch.telemetry.metrics.Histogram;
import org.opensearch.telemetry.metrics.MetricsRegistry;
import org.opensearch.telemetry.metrics.tags.Tags;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

/**
 * Comprehensive metrics tracking for streaming search operations.
 * Provides detailed insights for production monitoring and optimization.
 */
public class StreamingSearchMetrics {

    // Core metrics
    private final CounterMetric totalStreamingSearches = new CounterMetric();
    private final CounterMetric successfulStreamingSearches = new CounterMetric();
    private final CounterMetric failedStreamingSearches = new CounterMetric();
    private final CounterMetric fallbackToNormalSearches = new CounterMetric();
    
    // Performance metrics
    private final MeanMetric timeToFirstResult = new MeanMetric();
    private final MeanMetric totalSearchTime = new MeanMetric();
    private final MeanMetric emissionLatency = new MeanMetric();
    private final MeanMetric confidenceAtEmission = new MeanMetric();
    
    // Efficiency metrics
    private final LongAdder totalDocsEvaluated = new LongAdder();
    private final LongAdder totalDocsSkipped = new LongAdder();
    private final LongAdder totalBlocksProcessed = new LongAdder();
    private final LongAdder totalBlocksSkipped = new LongAdder();
    private final MeanMetric skipRatio = new MeanMetric();
    
    // Emission metrics
    private final CounterMetric totalEmissions = new CounterMetric();
    private final MeanMetric docsPerEmission = new MeanMetric();
    private final MeanMetric emissionsPerSearch = new MeanMetric();
    private final AtomicLong totalDocsEmitted = new AtomicLong();
    
    // Quality metrics
    private final MeanMetric precisionRate = new MeanMetric();
    private final MeanMetric recallRate = new MeanMetric();
    private final CounterMetric confidenceViolations = new CounterMetric();
    private final CounterMetric reorderingRequired = new CounterMetric();
    
    // Resource metrics
    private final AtomicLong currentActiveStreams = new AtomicLong();
    private final AtomicLong peakActiveStreams = new AtomicLong();
    private final AtomicLong totalMemoryUsed = new AtomicLong();
    private final AtomicLong peakMemoryUsed = new AtomicLong();
    
    // Network metrics
    private final LongAdder totalBytesStreamed = new LongAdder();
    private final CounterMetric clientDisconnections = new CounterMetric();
    private final CounterMetric backpressureEvents = new CounterMetric();
    
    // Circuit breaker metrics
    private final CounterMetric circuitBreakerTrips = new CounterMetric();
    private final AtomicLong currentCircuitBreakerMemory = new AtomicLong();
    
    // Per-index metrics
    private final Map<String, IndexStreamingMetrics> indexMetrics = new ConcurrentHashMap<>();
    
    // OpenTelemetry metrics
    private Counter streamingSearchCounter;
    private Histogram timeToFirstResultHistogram;
    private Histogram confidenceHistogram;
    private Counter emissionCounter;
    
    public StreamingSearchMetrics(MetricsRegistry metricsRegistry) {
        if (metricsRegistry != null) {
            initializeOpenTelemetryMetrics(metricsRegistry);
        }
    }
    
    private void initializeOpenTelemetryMetrics(MetricsRegistry registry) {
        // Register OpenTelemetry metrics
        this.streamingSearchCounter = registry.createCounter(
            "streaming_search_requests_total",
            "Total number of streaming search requests",
            "requests"
        );
        
        this.timeToFirstResultHistogram = registry.createHistogram(
            "streaming_search_time_to_first_result",
            "Time to first result in streaming search",
            "milliseconds"
        );
        
        this.confidenceHistogram = registry.createHistogram(
            "streaming_search_confidence_at_emission",
            "Confidence level when emitting results",
            "ratio"
        );
        
        this.emissionCounter = registry.createCounter(
            "streaming_search_emissions_total",
            "Total number of result emissions",
            "emissions"
        );
    }
    
    /**
     * Record the start of a streaming search
     */
    public StreamingSearchContext startSearch(String index) {
        totalStreamingSearches.inc();
        long activeStreams = currentActiveStreams.incrementAndGet();
        updatePeakActiveStreams(activeStreams);
        
        if (streamingSearchCounter != null) {
            streamingSearchCounter.add(1, Tags.create().addTag("index", index));
        }
        
        return new StreamingSearchContext(index, System.nanoTime());
    }
    
    /**
     * Record successful search completion
     */
    public void recordSuccess(StreamingSearchContext context) {
        successfulStreamingSearches.inc();
        currentActiveStreams.decrementAndGet();
        
        long duration = System.nanoTime() - context.startTime;
        totalSearchTime.inc(duration / 1_000_000); // Convert to milliseconds
        
        if (context.firstResultTime > 0) {
            long timeToFirst = (context.firstResultTime - context.startTime) / 1_000_000;
            timeToFirstResult.inc(timeToFirst);
            
            if (timeToFirstResultHistogram != null) {
                timeToFirstResultHistogram.record(timeToFirst, Tags.create()
                    .addTag("index", context.index));
            }
        }
        
        // Update per-index metrics
        getIndexMetrics(context.index).recordSuccess(duration / 1_000_000);
    }
    
    /**
     * Record search failure
     */
    public void recordFailure(StreamingSearchContext context, Throwable error) {
        failedStreamingSearches.inc();
        currentActiveStreams.decrementAndGet();
        
        getIndexMetrics(context.index).recordFailure(error.getClass().getSimpleName());
    }
    
    /**
     * Record fallback to normal search
     */
    public void recordFallback(String reason) {
        fallbackToNormalSearches.inc();
        currentActiveStreams.decrementAndGet();
    }
    
    /**
     * Record emission event
     */
    public void recordEmission(StreamingSearchContext context, int docsEmitted, float confidence) {
        totalEmissions.inc();
        docsPerEmission.inc(docsEmitted);
        totalDocsEmitted.addAndGet(docsEmitted);
        confidenceAtEmission.inc((long)(confidence * 100));
        
        if (context.firstResultTime == 0) {
            context.firstResultTime = System.nanoTime();
        }
        
        context.totalEmissions++;
        context.totalDocsEmitted += docsEmitted;
        
        // Record emission latency
        long now = System.nanoTime();
        if (context.lastEmissionTime > 0) {
            emissionLatency.inc((now - context.lastEmissionTime) / 1_000_000);
        }
        context.lastEmissionTime = now;
        
        if (emissionCounter != null) {
            emissionCounter.add(docsEmitted, Tags.create()
                .addTag("index", context.index)
                .addTag("batch", String.valueOf(context.totalEmissions)));
        }
        
        if (confidenceHistogram != null) {
            confidenceHistogram.record((long)(confidence * 100), Tags.create()
                .addTag("index", context.index));
        }
    }
    
    /**
     * Record block processing statistics
     */
    public void recordBlockProcessing(int docsEvaluated, int docsSkipped, 
                                     int blocksProcessed, int blocksSkipped) {
        totalDocsEvaluated.add(docsEvaluated);
        totalDocsSkipped.add(docsSkipped);
        totalBlocksProcessed.add(blocksProcessed);
        totalBlocksSkipped.add(blocksSkipped);
        
        if (blocksProcessed + blocksSkipped > 0) {
            float ratio = (float) blocksSkipped / (blocksProcessed + blocksSkipped);
            skipRatio.inc((long)(ratio * 100));
        }
    }
    
    /**
     * Record memory usage
     */
    public void recordMemoryUsage(long bytesUsed) {
        totalMemoryUsed.addAndGet(bytesUsed);
        long currentPeak = peakMemoryUsed.get();
        if (bytesUsed > currentPeak) {
            peakMemoryUsed.compareAndSet(currentPeak, bytesUsed);
        }
    }
    
    /**
     * Record network statistics
     */
    public void recordBytesStreamed(long bytes) {
        totalBytesStreamed.add(bytes);
    }
    
    public void recordClientDisconnection() {
        clientDisconnections.inc();
    }
    
    public void recordBackpressure() {
        backpressureEvents.inc();
    }
    
    /**
     * Record circuit breaker event
     */
    public void recordCircuitBreakerTrip() {
        circuitBreakerTrips.inc();
    }
    
    public void updateCircuitBreakerMemory(long bytes) {
        currentCircuitBreakerMemory.set(bytes);
    }
    
    /**
     * Record quality metrics
     */
    public void recordQualityMetrics(float precision, float recall) {
        precisionRate.inc((long)(precision * 100));
        recallRate.inc((long)(recall * 100));
    }
    
    public void recordConfidenceViolation() {
        confidenceViolations.inc();
    }
    
    public void recordReordering() {
        reorderingRequired.inc();
    }
    
    private void updatePeakActiveStreams(long current) {
        long peak = peakActiveStreams.get();
        while (current > peak) {
            if (peakActiveStreams.compareAndSet(peak, current)) {
                break;
            }
            peak = peakActiveStreams.get();
        }
    }
    
    private IndexStreamingMetrics getIndexMetrics(String index) {
        return indexMetrics.computeIfAbsent(index, k -> new IndexStreamingMetrics());
    }
    
    /**
     * Get current statistics
     */
    public StreamingSearchStats getStats() {
        return new StreamingSearchStats(this);
    }
    
    /**
     * Context for tracking individual search metrics
     */
    public static class StreamingSearchContext {
        public final String index;
        public final long startTime;
        public long firstResultTime = 0;
        public long lastEmissionTime = 0;
        public int totalEmissions = 0;
        public int totalDocsEmitted = 0;
        
        StreamingSearchContext(String index, long startTime) {
            this.index = index;
            this.startTime = startTime;
        }
    }
    
    /**
     * Per-index metrics
     */
    private static class IndexStreamingMetrics {
        private final CounterMetric searches = new CounterMetric();
        private final CounterMetric successes = new CounterMetric();
        private final Map<String, LongAdder> failuresByType = new ConcurrentHashMap<>();
        private final MeanMetric avgSearchTime = new MeanMetric();
        
        void recordSuccess(long duration) {
            searches.inc();
            successes.inc();
            avgSearchTime.inc(duration);
        }
        
        void recordFailure(String errorType) {
            searches.inc();
            failuresByType.computeIfAbsent(errorType, k -> new LongAdder()).increment();
        }
    }
    
    /**
     * Statistics snapshot
     */
    public static class StreamingSearchStats {
        public final long totalSearches;
        public final long successfulSearches;
        public final long failedSearches;
        public final long fallbacks;
        public final double avgTimeToFirstResult;
        public final double avgTotalSearchTime;
        public final double avgConfidenceAtEmission;
        public final long totalEmissions;
        public final double avgDocsPerEmission;
        public final long totalDocsEmitted;
        public final long totalDocsEvaluated;
        public final long totalDocsSkipped;
        public final double skipRatio;
        public final long currentActiveStreams;
        public final long peakActiveStreams;
        public final long totalMemoryUsed;
        public final long peakMemoryUsed;
        public final long totalBytesStreamed;
        public final long clientDisconnections;
        public final long backpressureEvents;
        public final long circuitBreakerTrips;
        
        StreamingSearchStats(StreamingSearchMetrics metrics) {
            this.totalSearches = metrics.totalStreamingSearches.count();
            this.successfulSearches = metrics.successfulStreamingSearches.count();
            this.failedSearches = metrics.failedStreamingSearches.count();
            this.fallbacks = metrics.fallbackToNormalSearches.count();
            this.avgTimeToFirstResult = metrics.timeToFirstResult.mean();
            this.avgTotalSearchTime = metrics.totalSearchTime.mean();
            this.avgConfidenceAtEmission = metrics.confidenceAtEmission.mean() / 100.0;
            this.totalEmissions = metrics.totalEmissions.count();
            this.avgDocsPerEmission = metrics.docsPerEmission.mean();
            this.totalDocsEmitted = metrics.totalDocsEmitted.get();
            this.totalDocsEvaluated = metrics.totalDocsEvaluated.sum();
            this.totalDocsSkipped = metrics.totalDocsSkipped.sum();
            
            long totalBlocks = metrics.totalBlocksProcessed.sum() + metrics.totalBlocksSkipped.sum();
            this.skipRatio = totalBlocks > 0 ? 
                (double) metrics.totalBlocksSkipped.sum() / totalBlocks : 0;
            
            this.currentActiveStreams = metrics.currentActiveStreams.get();
            this.peakActiveStreams = metrics.peakActiveStreams.get();
            this.totalMemoryUsed = metrics.totalMemoryUsed.get();
            this.peakMemoryUsed = metrics.peakMemoryUsed.get();
            this.totalBytesStreamed = metrics.totalBytesStreamed.sum();
            this.clientDisconnections = metrics.clientDisconnections.count();
            this.backpressureEvents = metrics.backpressureEvents.count();
            this.circuitBreakerTrips = metrics.circuitBreakerTrips.count();
        }
    }
}