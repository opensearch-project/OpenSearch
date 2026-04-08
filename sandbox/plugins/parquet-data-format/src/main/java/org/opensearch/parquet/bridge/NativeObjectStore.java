/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet.bridge;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.Closeable;
import java.io.IOException;
import java.lang.ref.Cleaner;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Java handle for a native Rust {@code Arc<dyn ObjectStore>}.
 *
 * <p>Created once per storage backend (local filesystem, S3, GCS, Azure) and
 * shared across all {@link NativeParquetWriter} instances targeting that backend.
 * The underlying native resource is reference-counted on the Rust side, so
 * writers that clone the handle keep it alive independently.
 *
 * <p>A {@link Cleaner} is registered as a safety net to release the native handle if
 * {@link #close()} is not called before the object becomes unreachable.
 *
 * <p>Must be {@link #close() closed} when no longer needed to release the
 * Rust-side {@code Arc}.
 */
public class NativeObjectStore implements Closeable {

    private static final Logger logger = LogManager.getLogger(NativeObjectStore.class);
    private static final Cleaner CLEANER = Cleaner.create();

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final long nativeHandle;
    private final String storeType;
    private final Cleaner.Cleanable cleanable;

    /**
     * Creates a native object store.
     *
     * @param storeType  backend type: {@code "local"}, {@code "s3"}, {@code "gcs"}, or {@code "azure"}
     * @param configJson JSON string with backend-specific configuration
     * @throws IOException if the native store creation fails
     */
    public NativeObjectStore(String storeType, String configJson) throws IOException {
        this.storeType = storeType;
        this.nativeHandle = RustBridge.createObjectStore(storeType, configJson);
        if (this.nativeHandle == 0) {
            throw new IOException("Native object store creation returned null handle for type: " + storeType);
        }
        this.cleanable = CLEANER.register(this, new CleanupAction(nativeHandle, storeType));
    }

    /**
     * Returns the opaque native handle for use by {@link NativeParquetWriter}.
     *
     * @return the native handle
     */
    long getNativeHandle() {
        if (closed.get()) {
            throw new IllegalStateException("Object store already closed: " + storeType);
        }
        return nativeHandle;
    }

    /**
     * Returns the store type identifier.
     *
     * @return the store type
     */
    public String getStoreType() {
        return storeType;
    }

    /**
     * Releases the native object store resources. Safe to call multiple times.
     * Deregisters the Cleaner so the destructor does not run twice.
     */
    @Override
    public void close() {
        if (closed.compareAndSet(false, true)) {
            cleanable.clean();
        }
    }

    /**
     * Static cleanup action — captures only the handle and type string,
     * not the enclosing NativeObjectStore instance.
     */
    private static class CleanupAction implements Runnable {
        private final long handle;
        private final String storeType;

        CleanupAction(long handle, String storeType) {
            this.handle = handle;
            this.storeType = storeType;
        }

        @Override
        public void run() {
            logger.warn("Native object store [{}] was not explicitly closed — releasing via Cleaner", storeType);
            RustBridge.destroyObjectStore(handle);
        }
    }
}
