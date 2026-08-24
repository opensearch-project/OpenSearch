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
import org.apache.arrow.c.CDataDictionaryProvider;
import org.apache.arrow.c.Data;
import org.apache.arrow.memory.BufferAllocator;
import org.apache.arrow.memory.RootAllocator;
import org.apache.arrow.vector.BigIntVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.types.pojo.Schema;
import org.opensearch.analytics.spi.ArrowBatchSource;
import org.opensearch.analytics.spi.ArrowBatchSourceFactory;
import org.opensearch.analytics.spi.DelegationThreadTracker;
import org.opensearch.core.tasks.TaskCancelledException;
import org.opensearch.test.OpenSearchTestCase;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

public class ArrowBatchSourceCallbacksTests extends OpenSearchTestCase {

    private RootAllocator allocator;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        allocator = new RootAllocator(Long.MAX_VALUE);
    }

    @Override
    public void tearDown() throws Exception {
        allocator.close();
        super.tearDown();
    }

    public void testUniqueBindingIdsAndIdempotentClose() {
        TestFactory first = new TestFactory(allocator, SourceMode.VALUES);
        TestFactory second = new TestFactory(allocator, SourceMode.VALUES);
        ArrowBatchSourceCallbacks.Registration firstRegistration = ArrowBatchSourceCallbacks.register(first, null);
        ArrowBatchSourceCallbacks.Registration secondRegistration = ArrowBatchSourceCallbacks.register(second, null);
        assertTrue(firstRegistration.bindingId() > 0L);
        assertTrue(secondRegistration.bindingId() > 0L);
        assertNotEquals(firstRegistration.bindingId(), secondRegistration.bindingId());

        firstRegistration.close();
        firstRegistration.close();
        secondRegistration.close();
        assertEquals(1, first.closeCount.get());
        assertEquals(1, second.closeCount.get());
    }

    public void testProjectionBatchExportEofAndBalancedTracking() throws Exception {
        TestFactory factory = new TestFactory(allocator, SourceMode.VALUES);
        TestTracker tracker = new TestTracker();
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, tracker);
            Arena arena = Arena.ofConfined();
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)
        ) {
            MemorySegment error = arena.allocate(256L);
            int sourceKey = ArrowBatchSourceCallbacks.createSource(
                registration.bindingId(),
                MemorySegment.ofArray(new int[] { 2, 0 }),
                2L,
                error,
                error.byteSize()
            );
            assertTrue(sourceKey > 0);
            assertArrayEquals(new int[] { 2, 0 }, factory.projection);

            long rows = ArrowBatchSourceCallbacks.nextBatch(
                registration.bindingId(),
                sourceKey,
                MemorySegment.ofAddress(array.memoryAddress()),
                MemorySegment.ofAddress(cSchema.memoryAddress()),
                error,
                error.byteSize()
            );
            assertEquals(2L, rows);
            try (CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
                Schema schema = Data.importSchema(allocator, cSchema, dictionaries);
                try (VectorSchemaRoot imported = VectorSchemaRoot.create(schema, allocator)) {
                    Data.importIntoVectorSchemaRoot(allocator, array, imported, dictionaries);
                    BigIntVector values = (BigIntVector) imported.getVector(0);
                    assertEquals(11L, values.get(0));
                    assertEquals(22L, values.get(1));
                }
            }

            assertEquals(
                0L,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    sourceKey,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            ArrowBatchSourceCallbacks.cancelSource(registration.bindingId(), sourceKey);
            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), sourceKey);
        }
        assertEquals(5, tracker.starts.get());
        assertEquals(5, tracker.ends.get());
        assertEquals(1, factory.sourceCancelCount.get());
        assertEquals(1, factory.sourceCloseCount.get());
        assertEquals(1, factory.closeCount.get());
    }

    public void testCreateAndNextErrorsPreserveMessagesAndTerminateBuffer() {
        TestFactory createFailure = new TestFactory(allocator, SourceMode.CREATE_ERROR);
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(createFailure, null);
            Arena arena = Arena.ofConfined()
        ) {
            MemorySegment error = arena.allocate(256L);
            error.fill((byte) 'x');
            assertEquals(-1, ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, 12L));
            assertEquals(0, error.get(ValueLayout.JAVA_BYTE, 11L));
            assertEquals(
                -1,
                ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize())
            );
            assertTrue(readCString(error, error.byteSize()).contains("create failure details"));
        }

        TestFactory nextFailure = new TestFactory(allocator, SourceMode.NEXT_ERROR);
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(nextFailure, null);
            Arena arena = Arena.ofConfined()
        ) {
            MemorySegment error = arena.allocate(256L);
            int key = ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize());
            assertTrue(key > 0);
            assertEquals(
                ArrowBatchSourceCallbacks.ERROR,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            assertTrue(readCString(error, error.byteSize()).contains("next failure"));
            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), key);
        }
    }

    public void testCancellationStatus() {
        TestFactory factory = new TestFactory(allocator, SourceMode.CANCELLED);
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
            Arena arena = Arena.ofConfined()
        ) {
            MemorySegment error = arena.allocate(128L);
            int key = ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize());
            assertEquals(
                ArrowBatchSourceCallbacks.CANCELLED,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), key);
        }
    }

    public void testZeroRowBatchIsNotEof() throws Exception {
        TestFactory factory = new TestFactory(allocator, SourceMode.EMPTY);
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
            Arena arena = Arena.ofConfined();
            ArrowArray array = ArrowArray.allocateNew(allocator);
            ArrowSchema cSchema = ArrowSchema.allocateNew(allocator)
        ) {
            MemorySegment error = arena.allocate(128L);
            int key = ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize());
            assertEquals(
                ArrowBatchSourceCallbacks.EMPTY_BATCH,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.ofAddress(array.memoryAddress()),
                    MemorySegment.ofAddress(cSchema.memoryAddress()),
                    error,
                    error.byteSize()
                )
            );
            try (CDataDictionaryProvider dictionaries = new CDataDictionaryProvider()) {
                Schema schema = Data.importSchema(allocator, cSchema, dictionaries);
                try (VectorSchemaRoot imported = VectorSchemaRoot.create(schema, allocator)) {
                    Data.importIntoVectorSchemaRoot(allocator, array, imported, dictionaries);
                    assertEquals(0, imported.getRowCount());
                }
            }
            assertEquals(
                0L,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), key);
        }
    }

    public void testRegistrationCloseWaitsForInFlightOpen() throws Exception {
        BlockingOpenFactory factory = new BlockingOpenFactory(allocator);
        ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
        ExecutorService executor = Executors.newFixedThreadPool(2);
        try (Arena arena = Arena.ofShared()) {
            MemorySegment error = arena.allocate(128L);
            Future<Integer> create = executor.submit(
                () -> ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize())
            );
            assertTrue(factory.entered.await(10L, TimeUnit.SECONDS));
            Future<?> close = executor.submit(registration::close);
            close.get(10L, TimeUnit.SECONDS);
            assertFalse(factory.factoryClosed.await(100L, TimeUnit.MILLISECONDS));
            factory.proceed.countDown();
            assertEquals(-1, (int) create.get(10L, TimeUnit.SECONDS));
            assertEquals(1, factory.sourceCancelCount.get());
            assertTrue(factory.factoryClosed.await(10L, TimeUnit.SECONDS));
            assertEquals(1, factory.closeCount.get());
        } finally {
            registration.close();
            executor.shutdownNow();
        }
    }

    public void testRegistrationCloseRejectsNewPull() {
        TestFactory factory = new TestFactory(allocator, SourceMode.VALUES);
        try (
            ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
            Arena arena = Arena.ofConfined()
        ) {
            MemorySegment error = arena.allocate(128L);
            int key = ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize());
            assertTrue(key > 0);

            registration.close();

            assertEquals(
                ArrowBatchSourceCallbacks.ERROR,
                ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            assertTrue(readCString(error, error.byteSize()).contains("binding is closed"));
            assertEquals(0, factory.nextBatchCount.get());

            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), key);
        }
        assertEquals(1, factory.sourceCancelCount.get());
        assertEquals(1, factory.sourceCloseCount.get());
        assertEquals(1, factory.closeCount.get());
    }

    public void testRegistrationCloseCooperativelyCancelsPendingPull() throws Exception {
        CooperativeBlockingFactory factory = new CooperativeBlockingFactory(allocator);
        ArrowBatchSourceCallbacks.Registration registration = ArrowBatchSourceCallbacks.register(factory, null);
        ExecutorService executor = Executors.newSingleThreadExecutor();
        try (Arena arena = Arena.ofShared()) {
            MemorySegment error = arena.allocate(128L);
            int key = ArrowBatchSourceCallbacks.createSource(registration.bindingId(), MemorySegment.NULL, 0L, error, error.byteSize());
            Future<Long> next = executor.submit(
                () -> ArrowBatchSourceCallbacks.nextBatch(
                    registration.bindingId(),
                    key,
                    MemorySegment.NULL,
                    MemorySegment.NULL,
                    error,
                    error.byteSize()
                )
            );
            assertTrue(factory.entered.await(10L, TimeUnit.SECONDS));
            registration.close();
            assertEquals(ArrowBatchSourceCallbacks.CANCELLED, (long) next.get(10L, TimeUnit.SECONDS));
            assertEquals(1, factory.sourceCancelCount.get());
            assertEquals(1, factory.sourceCloseCount.get());
            assertEquals(1, factory.closeCount.get());
            ArrowBatchSourceCallbacks.releaseSource(registration.bindingId(), key);
            assertEquals(1, factory.closeCount.get());
        } finally {
            registration.close();
            executor.shutdownNow();
        }
    }

    private static String readCString(MemorySegment segment, long capacity) {
        byte[] bytes = segment.reinterpret(capacity).toArray(ValueLayout.JAVA_BYTE);
        int length = 0;
        while (length < bytes.length && bytes[length] != 0) {
            length++;
        }
        return new String(Arrays.copyOf(bytes, length), StandardCharsets.UTF_8);
    }

    private enum SourceMode {
        VALUES,
        CREATE_ERROR,
        NEXT_ERROR,
        CANCELLED,
        EMPTY
    }

    private static final class TestFactory implements ArrowBatchSourceFactory {
        private final BufferAllocator allocator;
        private final SourceMode mode;
        private final AtomicInteger closeCount = new AtomicInteger();
        private final AtomicInteger sourceCancelCount = new AtomicInteger();
        private final AtomicInteger sourceCloseCount = new AtomicInteger();
        private final AtomicInteger nextBatchCount = new AtomicInteger();
        private volatile int[] projection;

        private TestFactory(BufferAllocator allocator, SourceMode mode) {
            this.allocator = allocator;
            this.mode = mode;
        }

        @Override
        public ArrowBatchSource open(int[] projection) {
            if (mode == SourceMode.CREATE_ERROR) {
                throw new IllegalStateException("create failure details");
            }
            this.projection = projection.clone();
            return new ArrowBatchSource() {
                private final AtomicBoolean emitted = new AtomicBoolean();

                @Override
                public BufferAllocator allocator() {
                    return allocator;
                }

                @Override
                public VectorSchemaRoot nextBatch() {
                    nextBatchCount.incrementAndGet();
                    if (mode == SourceMode.NEXT_ERROR) {
                        throw new IllegalStateException("next failure details");
                    }
                    if (mode == SourceMode.CANCELLED) {
                        throw new TaskCancelledException("cancelled");
                    }
                    if (emitted.compareAndSet(false, true) == false) {
                        return null;
                    }
                    BigIntVector vector = new BigIntVector("value", allocator);
                    int rowCount = mode == SourceMode.EMPTY ? 0 : 2;
                    vector.allocateNew(rowCount);
                    if (rowCount > 0) {
                        vector.set(0, 11L);
                        vector.set(1, 22L);
                    }
                    vector.setValueCount(rowCount);
                    return new VectorSchemaRoot(java.util.List.of(vector));
                }

                @Override
                public void cancel() {
                    sourceCancelCount.incrementAndGet();
                }

                @Override
                public void close() {
                    sourceCloseCount.incrementAndGet();
                }
            };
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }

    private static final class TestTracker implements DelegationThreadTracker {
        private final AtomicInteger starts = new AtomicInteger();
        private final AtomicInteger ends = new AtomicInteger();

        @Override
        public long trackStart() {
            return starts.incrementAndGet();
        }

        @Override
        public void trackEnd(long threadId) {
            ends.incrementAndGet();
        }
    }

    private static final class BlockingOpenFactory implements ArrowBatchSourceFactory {
        private final BufferAllocator allocator;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch proceed = new CountDownLatch(1);
        private final CountDownLatch factoryClosed = new CountDownLatch(1);
        private final AtomicInteger sourceCancelCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        private BlockingOpenFactory(BufferAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public ArrowBatchSource open(int[] projection) throws InterruptedException {
            entered.countDown();
            assertTrue(proceed.await(10L, TimeUnit.SECONDS));
            return new ArrowBatchSource() {
                @Override
                public BufferAllocator allocator() {
                    return allocator;
                }

                @Override
                public VectorSchemaRoot nextBatch() {
                    return null;
                }

                @Override
                public void cancel() {
                    sourceCancelCount.incrementAndGet();
                }

                @Override
                public void close() {}
            };
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
            factoryClosed.countDown();
        }
    }

    private static final class CooperativeBlockingFactory implements ArrowBatchSourceFactory {
        private final BufferAllocator allocator;
        private final CountDownLatch entered = new CountDownLatch(1);
        private final CountDownLatch cancelled = new CountDownLatch(1);
        private final AtomicInteger sourceCancelCount = new AtomicInteger();
        private final AtomicInteger sourceCloseCount = new AtomicInteger();
        private final AtomicInteger closeCount = new AtomicInteger();

        private CooperativeBlockingFactory(BufferAllocator allocator) {
            this.allocator = allocator;
        }

        @Override
        public ArrowBatchSource open(int[] projection) {
            return new ArrowBatchSource() {
                @Override
                public BufferAllocator allocator() {
                    return allocator;
                }

                @Override
                public VectorSchemaRoot nextBatch() throws InterruptedException {
                    entered.countDown();
                    assertTrue(cancelled.await(10L, TimeUnit.SECONDS));
                    throw new TaskCancelledException("cooperatively cancelled");
                }

                @Override
                public void cancel() {
                    sourceCancelCount.incrementAndGet();
                    cancelled.countDown();
                }

                @Override
                public void close() {
                    sourceCloseCount.incrementAndGet();
                }
            };
        }

        @Override
        public void close() {
            closeCount.incrementAndGet();
        }
    }
}
