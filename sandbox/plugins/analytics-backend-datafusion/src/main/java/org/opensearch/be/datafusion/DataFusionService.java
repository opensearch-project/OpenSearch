/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;

import java.io.IOException;

/**
 * Node-level service managing the DataFusion native runtime lifecycle.
 * <p>
 * All per-shard {@link DatafusionSearchExecEngine} instances share the single
 * Tokio runtime and memory pool owned by this service. The service loads the
 * native JNI library on start and tears down the runtime on stop/close.
 */
public class DataFusionService extends AbstractLifecycleComponent {

    private static final Logger logger = LogManager.getLogger(DataFusionService.class);
    private static final String NATIVE_LIBRARY_NAME = "opensearch_datafusion_jni";

    private final long memoryPoolLimit;
    private final String spillDirectory;
    private final long spillMemoryLimit;

    /** Handle to the native DataFusion global runtime (Tokio + memory pool). */
    private volatile NativeRuntimeHandle runtimeHandle;

    /**
     * Creates a new DataFusionService.
     *
     * @param memoryPoolLimit maximum bytes for the DataFusion memory pool
     * @param spillDirectory  directory for spill files when memory is exceeded
     * @param spillMemoryLimit maximum bytes before spilling to disk
     */
    public DataFusionService(long memoryPoolLimit, String spillDirectory, long spillMemoryLimit) {
        this.memoryPoolLimit = memoryPoolLimit;
        this.spillDirectory = spillDirectory;
        this.spillMemoryLimit = spillMemoryLimit;
    }

    @Override
    protected void doStart() {
        logger.info("Starting DataFusion service — loading native library [{}]", NATIVE_LIBRARY_NAME);
        try {
            System.loadLibrary(NATIVE_LIBRARY_NAME);
        } catch (UnsatisfiedLinkError e) {
            throw new IllegalStateException("Failed to load native library: " + NATIVE_LIBRARY_NAME, e);
        }

        // TODO: initialize Tokio runtime and memory pool via NativeBridge
        // long ptr = NativeBridge.createGlobalRuntime(memoryPoolLimit, spillDirectory, spillMemoryLimit);
        long ptr = 1L; // placeholder until NativeBridge is wired
        this.runtimeHandle = new NativeRuntimeHandle(ptr);
        logger.info("DataFusion service started");
    }

    @Override
    protected void doStop() {
        logger.info("Stopping DataFusion service");
        releaseRuntime();
    }

    @Override
    protected void doClose() throws IOException {
        releaseRuntime();
    }

    /**
     * Returns the handle to the native DataFusion global runtime.
     * All consumers should hold this reference and call {@link NativeRuntimeHandle#get()}
     * at JNI invocation time to obtain the current live pointer.
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

    /**
     * Returns the cache manager for per-shard cache management.
     * Used by DatafusionReaderManager to evict stale entries on file deletion.
     */
    // TODO: uncomment when CacheManager class is available
    // public CacheManager getCacheManager() { return cacheManager; }

    private void releaseRuntime() {
        NativeRuntimeHandle handle = runtimeHandle;
        if (handle != null) {
            handle.close();
            runtimeHandle = null;
            logger.info("DataFusion native runtime released");
        }
    }
}
