package com.parquet.parquetdataformat.rowid;

import java.util.concurrent.atomic.AtomicLong;

/**
 * Atomic, monotonic row ID generator as specified in the Project Mustang design.
 * Ensures that each parquet file has sequential row IDs starting from 0,
 * maintaining a 1:1 mapping between docs indexed in Lucene and parquet rows.
 */
public class RowIdGenerator {
    
    private final AtomicLong globalCounter;
    private final String generatorId;
    
    public RowIdGenerator(String generatorId) {
        this.generatorId = generatorId;
        this.globalCounter = new AtomicLong(0);
    }
    
    /**
     * Generates the next monotonic row ID.
     * Thread-safe and atomic operation.
     * 
     * @return Next sequential row ID
     */
    public long nextRowId() {
        return globalCounter.getAndIncrement();
    }
    
    /**
     * Gets the current counter value without incrementing.
     * Useful for determining the number of rows generated so far.
     * 
     * @return Current counter value
     */
    public long getCurrentCount() {
        return globalCounter.get();
    }
    
    /**
     * Resets the counter to zero.
     * Should only be used during testing or system reinitialization.
     */
    public void reset() {
        globalCounter.set(0);
    }
    
    /**
     * Gets the generator ID for tracking purposes.
     * 
     * @return Generator identifier
     */
    public String getGeneratorId() {
        return generatorId;
    }
    
    /**
     * Gets generation statistics.
     * 
     * @return GenerationStats with current state
     */
    public GenerationStats getStats() {
        return new GenerationStats(generatorId, globalCounter.get());
    }
    
    /**
     * Statistics for row ID generation.
     */
    public static class GenerationStats {
        private final String generatorId;
        private final long totalGenerated;
        
        public GenerationStats(String generatorId, long totalGenerated) {
            this.generatorId = generatorId;
            this.totalGenerated = totalGenerated;
        }
        
        public String getGeneratorId() { return generatorId; }
        public long getTotalGenerated() { return totalGenerated; }
    }
}
