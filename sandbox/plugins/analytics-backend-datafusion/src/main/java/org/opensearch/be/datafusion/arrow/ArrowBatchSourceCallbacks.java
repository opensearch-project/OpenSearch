/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.datafusion.arrow;

import org.apache.arrow.c.ArrowArray;
import org.apache.arrow.c.ArrowSchema;
import org.apache.arrow.c.Data;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.core.tasks.TaskCancelledException;

import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;

/** Java upcall targets for a native DataFusion Arrow batch source. */
public final class ArrowBatchSourceCallbacks {

    public static final long CANCELLED = -1L;
    public static final long ERROR = -2L;
    public static final long EMPTY_BATCH = -3L;

    private static final Logger LOGGER = LogManager.getLogger(ArrowBatchSourceCallbacks.class);
    private static final AtomicLong NEXT_BINDING_ID = new AtomicLong(1L);
    private static final ConcurrentHashMap<Long, Binding> BINDINGS = new ConcurrentHashMap<>();

    private ArrowBatchSourceCallbacks() {}

    /**
     * Registers one query-scoped factory and returns its independently allocated binding.
     * The factory must allocate batches through a caller-owned, breaker-accounted allocator;
     * native consumers import those externally accounted buffers without taking ownership of
     * their memory admission.
     */
    public static Registration register(ArrowBatchSourceFactory factory, DelegationThreadTracker tracker) {
        if (factory == null) {
            throw new NullPointerException("factory");
        }
        while (true) {
            long bindingId = NEXT_BINDING_ID.getAndIncrement();
            if (bindingId <= 0L) {
                throw new IllegalStateException("Arrow batch source binding IDs exhausted");
            }
            Binding binding = new Binding(bindingId, factory, tracker);
            if (BINDINGS.putIfAbsent(bindingId, binding) == null) {
                return new Registration(bindingId, binding);
            }
        }
    }

    /** Owns one callback binding. Close requests cleanup and is idempotent. */
    public static final class Registration implements AutoCloseable {
        private final long bindingId;
        private final Binding binding;
        private final AtomicBoolean closed = new AtomicBoolean();

        private Registration(long bindingId, Binding binding) {
            this.bindingId = bindingId;
            this.binding = binding;
        }

        public long bindingId() {
            return bindingId;
        }

        public Map<String, Long> metrics() {
            return binding.metrics();
        }

        @Override
        public void close() {
            if (closed.compareAndSet(false, true)) {
                binding.requestClose();
            }
        }
    }

    /** FFM upcall: opens a projected source and returns its positive key. */
    public static int createSource(
        long bindingId,
        MemorySegment projectionPointer,
        long projectionLength,
        MemorySegment errorPointer,
        long errorCapacity
    ) {
        CallbackLease lease = acquire(bindingId, false);
        if (lease == null) {
            writeError(errorPointer, errorCapacity, "Arrow batch source binding is closed: " + bindingId);
            return -1;
        }
        try (lease) {
            int length = Math.toIntExact(projectionLength);
            if (length < 0) {
                throw new IllegalArgumentException("projection length must be non-negative");
            }
            int[] projection;
            if (length == 0) {
                projection = new int[0];
            } else {
                if (projectionPointer == null || projectionPointer.equals(MemorySegment.NULL)) {
                    throw new IllegalArgumentException("projection pointer is null");
                }
                long byteLength = (long) length * Integer.BYTES;
                MemorySegment projectionView = projectionPointer.byteSize() >= byteLength
                    ? projectionPointer.asSlice(0L, byteLength)
                    : projectionPointer.reinterpret(byteLength);
                projection = projectionView.toArray(ValueLayout.JAVA_INT);
            }
            return lease.binding.open(projection);
        } catch (Throwable throwable) {
            writeError(errorPointer, errorCapacity, throwable.toString());
            LOGGER.debug("Failed to create Arrow batch source for binding {}", bindingId, throwable);
            return -1;
        }
    }

    /** FFM upcall: exports the next owned batch through Arrow C Data. */
    public static long nextBatch(
        long bindingId,
        int sourceKey,
        MemorySegment arrayPointer,
        MemorySegment schemaPointer,
        MemorySegment errorPointer,
        long errorCapacity
    ) {
        CallbackLease lease = acquire(bindingId, false);
        if (lease == null) {
            writeError(errorPointer, errorCapacity, "Arrow batch source binding is closed: " + bindingId);
            return ERROR;
        }
        try (lease) {
            SourceEntry source = lease.binding.source(sourceKey);
            if (source == null) {
                writeError(errorPointer, errorCapacity, "Unknown Arrow batch source key: " + sourceKey);
                return ERROR;
            }
            return source.exportNextBatch(arrayPointer, schemaPointer);
        } catch (TaskCancelledException cancelled) {
            return CANCELLED;
        } catch (Throwable throwable) {
            writeError(errorPointer, errorCapacity, throwable.toString());
            LOGGER.debug("Failed to read Arrow batch source binding {} key {}", bindingId, sourceKey, throwable);
            return ERROR;
        }
    }

    /** FFM upcall: cooperatively cancels one source without waiting for an active pull. */
    public static void cancelSource(long bindingId, int sourceKey) {
        CallbackLease lease = acquire(bindingId, true);
        if (lease == null) {
            return;
        }
        try (lease) {
            lease.binding.cancel(sourceKey);
        } catch (Throwable throwable) {
            LOGGER.warn("Failed to cancel Arrow batch source binding {} key {}", bindingId, sourceKey, throwable);
        }
    }

    /** FFM upcall: releases one source. Unknown or already-released keys are ignored. */
    public static void releaseSource(long bindingId, int sourceKey) {
        CallbackLease lease = acquire(bindingId, true);
        if (lease == null) {
            return;
        }
        try (lease) {
            lease.binding.release(sourceKey);
        } catch (Throwable throwable) {
            LOGGER.warn("Failed to release Arrow batch source binding {} key {}", bindingId, sourceKey, throwable);
        }
    }

    private static CallbackLease acquire(long bindingId, boolean allowClosing) {
        Binding binding = BINDINGS.get(bindingId);
        if (binding == null || binding.acquire(allowClosing) == false) {
            return null;
        }
        return new CallbackLease(binding);
    }

    private static void writeError(MemorySegment output, long capacity, String message) {
        if (output == null || output.equals(MemorySegment.NULL) || capacity <= 0L) {
            return;
        }
        byte[] bytes = message.getBytes(StandardCharsets.UTF_8);
        int length = Math.toIntExact(Math.min(bytes.length, capacity - 1L));
        MemorySegment view = output.reinterpret(capacity);
        MemorySegment.copy(bytes, 0, view, ValueLayout.JAVA_BYTE, 0L, length);
        view.set(ValueLayout.JAVA_BYTE, length, (byte) 0);
    }

    private static final class CallbackLease implements AutoCloseable {
        private final Binding binding;
        private final long trackedThreadId;
        private boolean closed;

        private CallbackLease(Binding binding) {
            this.binding = binding;
            this.trackedThreadId = binding.trackStart();
        }

        @Override
        public void close() {
            if (closed == false) {
                closed = true;
                try {
                    binding.trackEnd(trackedThreadId);
                } finally {
                    binding.releaseCallback();
                }
            }
        }
    }

    private static final class Binding {
        private final long bindingId;
        private final ArrowBatchSourceFactory factory;
        private final DelegationThreadTracker tracker;
        private final Object openLock = new Object();
        private final Map<Integer, SourceEntry> sources = new HashMap<>();
        private int nextSourceKey = 1;
        private int activeCallbacks;
        private boolean closing;
        private boolean factoryClosed;
        private Map<String, Long> finalMetrics = Map.of();

        private Binding(long bindingId, ArrowBatchSourceFactory factory, DelegationThreadTracker tracker) {
            this.bindingId = bindingId;
            this.factory = factory;
            this.tracker = tracker;
        }

        private synchronized boolean acquire(boolean allowClosing) {
            if (factoryClosed || (closing && allowClosing == false)) {
                return false;
            }
            activeCallbacks++;
            return true;
        }

        private int open(int[] projection) throws Exception {
            synchronized (this) {
                if (closing) {
                    throw new IllegalStateException("Arrow batch source binding is closing: " + bindingId);
                }
            }
            ArrowBatchSource source;
            synchronized (openLock) {
                synchronized (this) {
                    if (closing) {
                        throw new IllegalStateException("Arrow batch source binding is closing: " + bindingId);
                    }
                }
                source = factory.open(projection);
            }
            if (source == null) {
                throw new IllegalStateException("Arrow batch source factory returned null");
            }
            SourceEntry entry = new SourceEntry(source);
            String rejection = null;
            int sourceKey = -1;
            synchronized (this) {
                if (closing) {
                    rejection = "Arrow batch source binding is closing: " + bindingId;
                } else {
                    sourceKey = nextSourceKey++;
                    if (sourceKey <= 0) {
                        rejection = "Arrow batch source keys exhausted for binding " + bindingId;
                    } else {
                        sources.put(sourceKey, entry);
                    }
                }
            }
            if (rejection != null) {
                entry.close();
                throw new IllegalStateException(rejection);
            }
            return sourceKey;
        }

        private synchronized SourceEntry source(int sourceKey) {
            return sources.get(sourceKey);
        }

        private synchronized void cancel(int sourceKey) {
            SourceEntry source = sources.get(sourceKey);
            if (source != null) {
                source.cancel();
            }
        }

        private synchronized void release(int sourceKey) {
            SourceEntry source = sources.remove(sourceKey);
            if (source != null) {
                source.close();
            }
            tryFinishClose();
        }

        private void requestClose() {
            List<SourceEntry> openSources;
            synchronized (this) {
                closing = true;
                openSources = List.copyOf(sources.values());
            }
            for (SourceEntry source : openSources) {
                try {
                    source.cancel();
                } catch (Throwable throwable) {
                    LOGGER.warn("Failed to cancel Arrow batch source while closing binding {}", bindingId, throwable);
                }
            }
            synchronized (this) {
                tryFinishClose();
            }
        }

        private synchronized void releaseCallback() {
            activeCallbacks--;
            tryFinishClose();
        }

        private synchronized Map<String, Long> metrics() {
            return factoryClosed ? finalMetrics : Map.copyOf(factory.metrics());
        }

        private synchronized void tryFinishClose() {
            if (closing == false || factoryClosed || activeCallbacks != 0) {
                return;
            }
            // Native stream teardown normally releases each source, but an output stream can be
            // closed before its provider is polled. Registration owns final cleanup and must not
            // wait for a native release callback that may never arrive.
            List<SourceEntry> remainingSources = List.copyOf(sources.values());
            sources.clear();
            for (SourceEntry source : remainingSources) {
                try {
                    source.close();
                } catch (Throwable throwable) {
                    LOGGER.warn("Failed to close Arrow batch source while closing binding {}", bindingId, throwable);
                }
            }
            try {
                finalMetrics = Map.copyOf(factory.metrics());
            } catch (Throwable throwable) {
                LOGGER.warn("Failed to collect final Arrow batch source metrics for binding {}", bindingId, throwable);
            }
            factoryClosed = true;
            try {
                factory.close();
            } finally {
                BINDINGS.remove(bindingId, this);
            }
        }

        private long trackStart() {
            if (tracker == null) {
                return -1L;
            }
            try {
                return tracker.trackStart();
            } catch (Throwable throwable) {
                LOGGER.warn("Failed to start Arrow batch source callback tracking", throwable);
                return -1L;
            }
        }

        private void trackEnd(long threadId) {
            if (tracker == null || threadId < 0L) {
                return;
            }
            try {
                tracker.trackEnd(threadId);
            } catch (Throwable throwable) {
                LOGGER.warn("Failed to finish Arrow batch source callback tracking", throwable);
            }
        }
    }

    private static final class SourceEntry {
        private final ArrowBatchSource source;
        private final Object cancellationLock = new Object();
        private final AtomicBoolean cancellationRequested = new AtomicBoolean();
        private boolean closed;

        private SourceEntry(ArrowBatchSource source) {
            this.source = source;
        }

        private synchronized long exportNextBatch(MemorySegment arrayPointer, MemorySegment schemaPointer) throws Exception {
            if (closed) {
                throw new IllegalStateException("Arrow batch source is closed");
            }
            VectorSchemaRoot root = source.nextBatch();
            if (root == null) {
                return 0L;
            }
            try {
                ArrowArray array = ArrowArray.wrap(arrayPointer.address());
                ArrowSchema schema = ArrowSchema.wrap(schemaPointer.address());
                Data.exportVectorSchemaRoot(source.allocator(), root, null, array, schema);
                return root.getRowCount() == 0 ? EMPTY_BATCH : root.getRowCount();
            } finally {
                root.close();
            }
        }

        private void cancel() {
            synchronized (cancellationLock) {
                if (cancellationRequested.compareAndSet(false, true)) {
                    source.cancel();
                }
            }
        }

        private synchronized void close() {
            cancel();
            if (closed == false) {
                closed = true;
                source.close();
            }
        }
    }
}
