package com.parquet.parquetdataformat.memory;

import org.opensearch.common.settings.Settings;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Monitors off-heap memory usage and triggers backpressure mechanisms.
 * Tracks Arrow buffer allocations and provides pressure metrics for
 * controlling writer creation and flush intervals.
 */
public class MemoryPressureMonitor {
    
    public enum PressureLevel {
        LOW(0.0, 0.7),        // < 70% utilization
        MODERATE(0.7, 0.85),  // 70-85% utilization  
        HIGH(0.85, 0.95),     // 85-95% utilization
        CRITICAL(0.95, 1.0);  // > 95% utilization
        
        private final double min;
        private final double max;
        
        PressureLevel(double min, double max) {
            this.min = min;
            this.max = max;
        }
        
        public static PressureLevel fromRatio(double ratio) {
            for (PressureLevel level : values()) {
                if (ratio >= level.min && ratio < level.max) {
                    return level;
                }
            }
            return CRITICAL;
        }
    }
    
    private final MemoryMXBean memoryBean;
    private final ScheduledExecutorService scheduler;
    private final AtomicLong directMemoryUsed;
    private final AtomicLong directMemoryMax;
    private final AtomicReference<PressureLevel> currentPressure;
    private final AtomicLong allocationCount;
    private final AtomicLong deallocationCount;
    private final AtomicLong failedAllocationCount;
    
    // Configuration
    private final double criticalThreshold;
    private final double highThreshold;
    private final long maxDirectMemory;
    
    public MemoryPressureMonitor(Settings settings) {
        this.memoryBean = ManagementFactory.getMemoryMXBean();
        this.scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
            Thread t = new Thread(r, "parquet-memory-monitor");
            t.setDaemon(true);
            return t;
        });
        
        this.directMemoryUsed = new AtomicLong(0);
        this.currentPressure = new AtomicReference<>(PressureLevel.LOW);
        this.allocationCount = new AtomicLong(0);
        this.deallocationCount = new AtomicLong(0);
        this.failedAllocationCount = new AtomicLong(0);
        
        // Parse configuration
        this.criticalThreshold = settings.getAsDouble("parquet.memory.critical_threshold", 0.95);
        this.highThreshold = settings.getAsDouble("parquet.memory.high_threshold", 0.85);
        this.maxDirectMemory = getMaxDirectMemory();
        this.directMemoryMax = new AtomicLong(maxDirectMemory);
        
        // Start monitoring
        startMonitoring();
    }
    
    /**
     * Checks if an allocation should be rejected based on current memory pressure.
     * 
     * @param requestedBytes Number of bytes requested for allocation
     * @return true if allocation should be rejected
     */
    public boolean shouldRejectAllocation(long requestedBytes) {
        PressureLevel pressure = currentPressure.get();
        
        // Always reject if critical
        if (pressure == PressureLevel.CRITICAL) {
            return true;
        }
        
        // Check if allocation would push us over threshold
        long currentUsage = directMemoryUsed.get();
        long afterAllocation = currentUsage + requestedBytes;
        double futureRatio = (double) afterAllocation / maxDirectMemory;
        
        return switch (pressure) {
            case HIGH -> futureRatio > criticalThreshold;
            case MODERATE -> futureRatio > highThreshold;
            case LOW -> false;
            case CRITICAL -> true; // Already handled above
        };
    }
    
    /**
     * Records an allocation event.
     * 
     * @param size Size of the allocation
     */
    public void recordAllocation(long size) {
        directMemoryUsed.addAndGet(size);
        allocationCount.incrementAndGet();
        updatePressureLevel();
    }
    
    /**
     * Records a deallocation event.
     * 
     * @param size Size of the deallocation
     */
    public void recordDeallocation(long size) {
        directMemoryUsed.addAndGet(-size);
        deallocationCount.incrementAndGet();
        updatePressureLevel();
    }
    
    /**
     * Records a failed allocation event.
     * 
     * @param size Size of the failed allocation
     * @param reason Reason for failure
     */
    public void recordFailedAllocation(long size, String reason) {
        failedAllocationCount.incrementAndGet();
        // Could log detailed failure information here
    }
    
    /**
     * Gets the current memory pressure as a ratio (0.0 to 1.0).
     * 
     * @return Current memory pressure ratio
     */
    public double getCurrentPressure() {
        return (double) directMemoryUsed.get() / maxDirectMemory;
    }
    
    /**
     * Gets the current pressure level enum.
     * 
     * @return Current PressureLevel
     */
    public PressureLevel getCurrentPressureLevel() {
        return currentPressure.get();
    }
    
    /**
     * Gets current memory statistics.
     * 
     * @return MemoryStats with current usage information
     */
    public MemoryStats getStats() {
        return new MemoryStats(
            directMemoryUsed.get(),
            maxDirectMemory,
            getCurrentPressure(),
            currentPressure.get(),
            allocationCount.get(),
            deallocationCount.get(),
            failedAllocationCount.get()
        );
    }
    
    /**
     * Triggers early refresh if memory pressure is high.
     * 
     * @return true if early refresh should be triggered
     */
    public boolean shouldTriggerEarlyRefresh() {
        PressureLevel pressure = currentPressure.get();
        return pressure == PressureLevel.HIGH || pressure == PressureLevel.CRITICAL;
    }
    
    /**
     * Gets recommended writer limit based on current memory pressure.
     * 
     * @param baseLimit Base number of writers without pressure
     * @return Adjusted writer limit
     */
    public int getRecommendedWriterLimit(int baseLimit) {
        return switch (currentPressure.get()) {
            case LOW -> baseLimit;
            case MODERATE -> (int) (baseLimit * 0.8);
            case HIGH -> (int) (baseLimit * 0.5);
            case CRITICAL -> 1; // Minimal writers only
        };
    }
    
    private void startMonitoring() {
        scheduler.scheduleAtFixedRate(this::updatePressureLevel, 1, 1, TimeUnit.SECONDS);
    }
    
    private void updatePressureLevel() {
        double ratio = getCurrentPressure();
        PressureLevel newLevel = PressureLevel.fromRatio(ratio);
        PressureLevel oldLevel = currentPressure.getAndSet(newLevel);
        
        // Log pressure level changes
        if (newLevel != oldLevel) {
            System.out.println(String.format(
                "[MEMORY] Pressure level changed: %s -> %s (%.2f%%)", 
                oldLevel, newLevel, ratio * 100));
        }
    }
    
    private long getMaxDirectMemory() {
        // Use heap max / 4 as a reasonable default for direct memory
        long heapMax = ManagementFactory.getMemoryMXBean().getHeapMemoryUsage().getMax();
        return heapMax > 0 ? heapMax / 4 : 1024 * 1024 * 1024; // 1GB fallback
    }
    
    /**
     * Closes the monitor and stops background tasks.
     */
    public void close() {
        scheduler.shutdown();
        try {
            if (!scheduler.awaitTermination(5, TimeUnit.SECONDS)) {
                scheduler.shutdownNow();
            }
        } catch (InterruptedException e) {
            scheduler.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
    
    /**
     * Memory statistics for monitoring.
     */
    public static class MemoryStats {
        private final long usedBytes;
        private final long maxBytes;
        private final double pressureRatio;
        private final PressureLevel pressureLevel;
        private final long allocationCount;
        private final long deallocationCount;
        private final long failedAllocationCount;
        
        public MemoryStats(long usedBytes, long maxBytes, double pressureRatio,
                          PressureLevel pressureLevel, long allocationCount,
                          long deallocationCount, long failedAllocationCount) {
            this.usedBytes = usedBytes;
            this.maxBytes = maxBytes;
            this.pressureRatio = pressureRatio;
            this.pressureLevel = pressureLevel;
            this.allocationCount = allocationCount;
            this.deallocationCount = deallocationCount;
            this.failedAllocationCount = failedAllocationCount;
        }
        
        public long getUsedBytes() { return usedBytes; }
        public long getMaxBytes() { return maxBytes; }
        public double getPressureRatio() { return pressureRatio; }
        public PressureLevel getPressureLevel() { return pressureLevel; }
        public long getAllocationCount() { return allocationCount; }
        public long getDeallocationCount() { return deallocationCount; }
        public long getFailedAllocationCount() { return failedAllocationCount; }
        public long getAvailableBytes() { return maxBytes - usedBytes; }
    }
}
