/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion.search;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

/**
 * Global registry for tracking all active DatafusionReaders across all shards.
 * This is a singleton that maintains weak references to all readers and their metadata.
 */
public class DatafusionReaderRegistry {
    private static final Logger logger = LogManager.getLogger(DatafusionReaderRegistry.class);
    
    private static final DatafusionReaderRegistry INSTANCE = new DatafusionReaderRegistry();
    
    // Track readers with their metadata
    private final Map<Long, ReaderInfo> activeReaders = new ConcurrentHashMap<>();
    private final AtomicLong readerIdGenerator = new AtomicLong(0);
    
    /**
     * Internal class to hold reader metadata
     */
    public static class ReaderInfo {
        public final long readerId;
        public final String shardId;
        public final String directoryPath;
        public final DatafusionReader reader;
        public final long registrationTime;
        
        public ReaderInfo(long readerId, String shardId, String directoryPath, DatafusionReader reader) {
            this.readerId = readerId;
            this.shardId = shardId;
            this.directoryPath = directoryPath;
            this.reader = reader;
            this.registrationTime = System.currentTimeMillis();
        }
        
        public int getRefCount() {
            return reader.getRefCount();
        }
    }
    
    private DatafusionReaderRegistry() {
        // Private constructor for singleton
    }
    
    public static DatafusionReaderRegistry getInstance() {
        return INSTANCE;
    }
    
    /**
     * Register a new reader with the registry
     * @param shardId The shard ID this reader belongs to
     * @param reader The DatafusionReader instance
     * @return The unique reader ID assigned
     */
    public long registerReader(String shardId, DatafusionReader reader) {
        long readerId = readerIdGenerator.incrementAndGet();
        ReaderInfo info = new ReaderInfo(readerId, shardId, reader.directoryPath, reader);
        activeReaders.put(readerId, info);
        logger.debug("Registered reader {} for shard {} with path {}", readerId, shardId, reader.directoryPath);
        return readerId;
    }
    
    /**
     * Unregister a reader from the registry
     * @param readerId The reader ID to unregister
     */
    public void unregisterReader(long readerId) {
        ReaderInfo removed = activeReaders.remove(readerId);
        if (removed != null) {
            logger.debug("Unregistered reader {} for shard {}", readerId, removed.shardId);
        }
    }
    
    /**
     * Get all active readers' reference counts
     * @return Map of reader ID to reference count
     */
    public Map<Long, Integer> getAllRefCounts() {
        return activeReaders.entrySet().stream()
            .collect(Collectors.toMap(
                Map.Entry::getKey,
                entry -> entry.getValue().getRefCount()
            ));
    }
    
    /**
     * Get detailed information about all active readers
     * @return Map of reader info
     */
    public Map<Long, ReaderInfo> getAllReaderInfo() {
        return new ConcurrentHashMap<>(activeReaders);
    }
    
    /**
     * Log all active readers and their reference counts
     * @param prefix Optional prefix for the log message
     */
    public void logAllReaderRefCounts(String prefix) {
        if (activeReaders.isEmpty()) {
            logger.info("{} No active DatafusionReaders", prefix != null ? prefix : "");
            return;
        }
        
        StringBuilder sb = new StringBuilder();
        sb.append(prefix != null ? prefix : "").append(" Active DatafusionReaders:\n");
        sb.append("=====================================\n");
        sb.append(String.format("Total Active Readers: %d\n", activeReaders.size()));
        
        // Group by shard
        Map<String, Set<ReaderInfo>> byShardId = activeReaders.values().stream()
            .collect(Collectors.groupingBy(
                info -> info.shardId,
                Collectors.toSet()
            ));
        
        byShardId.forEach((shardId, readers) -> {
            sb.append(String.format("\nShard: %s (Readers: %d)\n", shardId, readers.size()));
            readers.forEach(info -> {
                long ageMs = System.currentTimeMillis() - info.registrationTime;
                sb.append(String.format("  - Reader ID: %d, RefCount: %d, Path: %s, Age: %dms\n",
                    info.readerId,
                    info.getRefCount(),
                    info.directoryPath,
                    ageMs
                ));
            });
        });
        
        // Summary statistics
        int totalRefCount = activeReaders.values().stream()
            .mapToInt(ReaderInfo::getRefCount)
            .sum();
        sb.append("\n-------------------------------------\n");
        sb.append(String.format("Total Reference Count: %d\n", totalRefCount));
        sb.append("=====================================");
        
        logger.info(sb.toString());
    }
    
    /**
     * Get the total number of active readers
     * @return The count of active readers
     */
    public int getActiveReaderCount() {
        return activeReaders.size();
    }
    
    /**
     * Get the total reference count across all readers
     * @return The sum of all reference counts
     */
    public int getTotalRefCount() {
        return activeReaders.values().stream()
            .mapToInt(ReaderInfo::getRefCount)
            .sum();
    }
    
    /**
     * Clear all tracked readers (use with caution, mainly for testing)
     */
    public void clear() {
        activeReaders.clear();
        logger.info("Cleared all tracked readers from registry");
    }
    
    /**
     * Clean up stale readers (readers with refcount=0 or from non-existent indices)
     * @return Number of readers cleaned up
     */
    public int cleanupStaleReaders() {
        int cleanedUp = 0;
        var iterator = activeReaders.entrySet().iterator();
        while (iterator.hasNext()) {
            var entry = iterator.next();
            ReaderInfo info = entry.getValue();
            if (info.getRefCount() == 0) {
                iterator.remove();
                cleanedUp++;
                logger.info("Cleaned up stale reader {} for shard {} with refcount=0", 
                    info.readerId, info.shardId);
            }
        }
        if (cleanedUp > 0) {
            logger.info("Cleaned up {} stale readers from registry", cleanedUp);
        }
        return cleanedUp;
    }
}
