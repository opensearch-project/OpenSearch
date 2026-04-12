/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.be.datafusion.nativelib.NativeBridge;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;

import java.io.IOException;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Node-level service managing the DataFusion native runtime lifecycle.
 * <p>
 * Initializes the Tokio runtime manager (dedicated CPU + IO thread pools)
 * and the DataFusion global runtime (memory pool, disk spill, cache).
 * All per-query {@link DatafusionSearchExecEngine} instances share these resources.
 * <p>
 * Use {@link #builder()} to construct.
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);

    private final long memoryPoolLimit;
    private final long spillMemoryLimit;
    private final String spillDirectory;
    private final int cpuThreads;

    /** Handle to the native DataFusion global runtime (memory pool + cache). */
    private volatile NativeRuntimeHandle runtimeHandle;

    /** Shared Arrow allocator for all DataFusion result streams on this node. */
    private volatile RootAllocator rootAllocator;

    /** Counter for generating unique child allocator names. */
    private final AtomicLong allocatorCounter = new AtomicLong();

    private DataFusionService(Builder builder) {
        this.memoryPoolLimit = builder.memoryPoolLimit;
        this.spillMemoryLimit = builder.spillMemoryLimit;
        this.spillDirectory = builder.spillDirectory;
        this.cpuThreads = builder.cpuThreads;
    }

    /** Creates a new builder. */
    public static Builder builder() {
        return new Builder();
    }

    @Override
    protected void doStart() {
        logger.debug("Starting DataFusion service");
        NativeBridge.initTokioRuntimeManager(cpuThreads);
        logger.debug("Tokio runtime manager initialized with {} CPU threads", cpuThreads);

        long ptr = NativeBridge.createGlobalRuntime(memoryPoolLimit, 0L, spillDirectory, spillMemoryLimit);
        this.runtimeHandle = new NativeRuntimeHandle(ptr);
        this.rootAllocator = new RootAllocator(memoryPoolLimit);
        logger.debug("DataFusion service started — memory pool {}B, spill limit {}B", memoryPoolLimit, spillMemoryLimit);
    }

    @Override
    protected void doStop() {
        logger.debug("Stopping DataFusion service");
        try {
            releaseRuntime();
        } finally {
            try {
                if (rootAllocator != null) {
                    rootAllocator.close();
                    rootAllocator = null;
                }
            } finally {
                NativeBridge.shutdownTokioRuntimeManager();
            }
        }
        logger.debug("DataFusion service stopped");
    }

    @Override
    protected void doClose() throws IOException {
        releaseRuntime();
    }

    /**
     * Returns the handle to the native DataFusion global runtime.
     *
     * @throws IllegalStateException if the service has not been started
     */
    public NativeRuntimeHandle getNativeRuntime() {
        NativeRuntimeHandle handle = runtimeHandle;
        if (handle == null) {
            throw new IllegalStateException("DataFusionService has not been started");
        }
        return handle;
    }

    // Cache management (node-level, delegates to native runtime)

    /**
     * Creates a new child allocator from the shared root allocator.
     * Each child has independent accounting but shares the root's memory limit.
     *
     * @return a new child {@link BufferAllocator}
     * @throws IllegalStateException if the service has not been started
     */
    public BufferAllocator newChildAllocator() {
        RootAllocator alloc = rootAllocator;
        if (alloc == null) {
            throw new IllegalStateException("DataFusionService has not been started");
        }
        return alloc.newChildAllocator("datafusion-stream-" + allocatorCounter.getAndIncrement(), 0, alloc.getLimit());
    }

    /**
     * Notifies the native cache that new files are available for caching.
     * @param filePaths absolute paths of the new files
     */
    public void onFilesAdded(Collection<String> filePaths) {
        if (filePaths == null || filePaths.isEmpty()) return;
        try {
            NativeBridge.cacheManagerAddFiles(runtimeHandle.get(), filePaths.toArray(new String[0]));
        } catch (Exception e) {
            logger.warn("Failed to register new files with native cache", e);
        }
    }

    /**
     * Notifies the native cache that files have been deleted and should be evicted.
     * @param filePaths absolute paths of the deleted files
     */
    public void onFilesDeleted(Collection<String> filePaths) {
        if (filePaths == null || filePaths.isEmpty()) return;
        try {
            NativeBridge.cacheManagerRemoveFiles(runtimeHandle.get(), filePaths.toArray(new String[0]));
        } catch (Exception e) {
            logger.warn("Failed to evict deleted files from native cache", e);
        }
    }

    private void releaseRuntime() {
        NativeRuntimeHandle handle = runtimeHandle;
        if (handle != null) {
            handle.close();
            runtimeHandle = null;
            logger.debug("DataFusion native runtime released");
        }
    }

    /**
     * Builder for {@link DataFusionService}.
     */
    public static class Builder {
        private long memoryPoolLimit = Runtime.getRuntime().maxMemory() / 4;
        private long spillMemoryLimit = Runtime.getRuntime().maxMemory() / 8;
        private String spillDirectory = System.getProperty("java.io.tmpdir");
        private int cpuThreads = Runtime.getRuntime().availableProcessors();

        private Builder() {}

        /**
         * Sets the maximum bytes for the DataFusion memory pool.
         * @param bytes memory limit
         */
        public Builder memoryPoolLimit(long bytes) {
            this.memoryPoolLimit = bytes;
            return this;
        }

        /**
         * Sets the maximum bytes before spilling to disk.
         * @param bytes spill limit
         */
        public Builder spillMemoryLimit(long bytes) {
            this.spillMemoryLimit = bytes;
            return this;
        }

        /**
         * Sets the directory for spill files.
         * @param path spill directory
         */
        public Builder spillDirectory(String path) {
            this.spillDirectory = path;
            return this;
        }

        /**
         * Sets the number of CPU threads for the dedicated executor.
         * @param threads CPU thread count
         */
        public Builder cpuThreads(int threads) {
            this.cpuThreads = threads;
            return this;
        }

        /** Builds the {@link DataFusionService}. */
        public DataFusionService build() {
            return new DataFusionService(this);
        }
    }
}
