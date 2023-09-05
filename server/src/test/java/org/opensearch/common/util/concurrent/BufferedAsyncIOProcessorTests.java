/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.util.concurrent;

import org.junit.After;
import org.junit.Before;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.Collections;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static java.lang.Thread.sleep;

public class BufferedAsyncIOProcessorTests extends OpenSearchTestCase {

    private ThreadPool threadpool;
    private ThreadContext threadContext;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        threadpool = new TestThreadPool("buffered-async-io");
        threadContext = new ThreadContext(Settings.EMPTY);
    }

    @After
    public void cleanup() {
        terminate(threadpool);
    }

    public void testConsumerCanThrowExceptions() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new BufferedAsyncIOProcessor<>(
            logger,
            scaledRandomIntBetween(1, 2024),
            threadContext,
            threadpool,
            () -> TimeValue.timeValueMillis(50)
        ) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        processor.put(new Object(), (e) -> {
            notified.incrementAndGet();
            throw new RuntimeException();
        });
        try {
            sleep(200); // Give the processor few chances to run
        } catch (InterruptedException e) {
            logger.error("Error while trying to sleep", e);
        }
        assertEquals(2, notified.get());
        assertEquals(2, received.get());
    }

    public void testPreserveThreadContext() throws InterruptedException {
        final int threadCount = randomIntBetween(2, 10);
        final String testHeader = "test-header";

        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        CountDownLatch processed = new CountDownLatch(threadCount);
        AsyncIOProcessor<Object> processor = new BufferedAsyncIOProcessor<>(
            logger,
            scaledRandomIntBetween(1, 2024),
            threadContext,
            threadpool,
            () -> TimeValue.timeValueMillis(100)
        ) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        // all threads should be non-blocking.
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(getTestName() + "_" + i) {
            private final String response = randomAlphaOfLength(10);

            {
                setDaemon(true);
            }

            @Override
            public void run() {
                threadContext.addResponseHeader(testHeader, response);
                processor.put(new Object(), (e) -> {
                    final Map<String, List<String>> expected = Collections.singletonMap(testHeader, Collections.singletonList(response));
                    assertEquals(expected, threadContext.getResponseHeaders());
                    notified.incrementAndGet();
                    processed.countDown();
                });
            }
        }).collect(Collectors.toList());
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        assertTrue(processed.await(1, TimeUnit.SECONDS));

        assertEquals(threadCount, notified.get());
        assertEquals(threadCount, received.get());
        threads.forEach(t -> assertFalse(t.isAlive()));
    }

    public void testSlowConsumer() {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);

        AsyncIOProcessor<Object> processor = new BufferedAsyncIOProcessor<>(
            logger,
            scaledRandomIntBetween(1, 2024),
            threadContext,
            threadpool,
            () -> TimeValue.timeValueMillis(100)
        ) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        int threadCount = randomIntBetween(2, 10);
        Semaphore serializePutSemaphore = new Semaphore(1);
        CountDownLatch allDone = new CountDownLatch(threadCount);
        List<Thread> threads = IntStream.range(0, threadCount).mapToObj(i -> new Thread(getTestName() + "_" + i) {
            {
                setDaemon(true);
            }

            @Override
            public void run() {
                try {
                    assertTrue(serializePutSemaphore.tryAcquire(10, TimeUnit.SECONDS));
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
                processor.put(new Object(), (e) -> {
                    serializePutSemaphore.release();
                    notified.incrementAndGet();
                    allDone.countDown();
                });
            }
        }).collect(Collectors.toList());
        threads.forEach(Thread::start);
        threads.forEach(t -> {
            try {
                t.join(20000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        });
        try {
            assertTrue(allDone.await(20000, TimeUnit.MILLISECONDS));
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        assertEquals(threadCount, notified.get());
        assertEquals(threadCount, received.get());
        threads.forEach(t -> assertFalse(t.isAlive()));
    }

    public void testConsecutiveWritesAtLeastBufferIntervalAway() throws InterruptedException {
        AtomicInteger received = new AtomicInteger(0);
        AtomicInteger notified = new AtomicInteger(0);
        long bufferIntervalMs = randomLongBetween(150, 250);
        List<Long> writeInvocationTimes = new LinkedList<>();

        AsyncIOProcessor<Object> processor = new BufferedAsyncIOProcessor<>(
            logger,
            scaledRandomIntBetween(1, 2024),
            threadContext,
            threadpool,
            () -> TimeValue.timeValueMillis(bufferIntervalMs)
        ) {
            @Override
            protected void write(List<Tuple<Object, Consumer<Exception>>> candidates) throws IOException {
                received.addAndGet(candidates.size());
                writeInvocationTimes.add(System.nanoTime());
            }

            @Override
            protected String getBufferProcessThreadPoolName() {
                return ThreadPool.Names.TRANSLOG_SYNC;
            }
        };

        int runCount = randomIntBetween(3, 10);
        CountDownLatch processed = new CountDownLatch(runCount);
        IntStream.range(0, runCount).forEach(i -> {
            processor.put(new Object(), (e) -> {
                notified.incrementAndGet();
                processed.countDown();
            });
        });
        assertTrue(processed.await(bufferIntervalMs * (runCount + 1), TimeUnit.MILLISECONDS));
        assertEquals(runCount, notified.get());
        assertEquals(runCount, received.get());
        for (int i = 1; i < writeInvocationTimes.size(); i++) {
            // Resolution of System.nanoTime() is only as good as System.currentTimeMillis() and many operating systems
            // measure time in units of tens of milliseconds as per Java documentation
            // https://docs.oracle.com/en/java/javase/17/docs/api/java.base/java/lang/System.html#currentTimeMillis()
            // Keeping a buffer of 20 ms as we are getting time twice with each having resolution of +- 10ms.
            assertTrue(writeInvocationTimes.get(i) >= writeInvocationTimes.get(i - 1) + (bufferIntervalMs - 20) * 1_000_000);
        }
    }
}
