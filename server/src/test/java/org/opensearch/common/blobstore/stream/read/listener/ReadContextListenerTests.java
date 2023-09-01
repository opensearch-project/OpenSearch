/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.opensearch.action.LatchedActionListener;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.common.blobstore.stream.read.ReadContext;
import org.opensearch.common.io.InputStreamContainer;
import org.opensearch.core.action.ActionListener;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;

import static org.opensearch.common.blobstore.stream.read.listener.ListenerTestUtils.CountingCompletionListener;

public class ReadContextListenerTests extends OpenSearchTestCase {

    private Path path;
    private static ThreadPool threadPool;
    private static final int NUMBER_OF_PARTS = 5;
    private static final int PART_SIZE = 10;
    private static final String TEST_SEGMENT_FILE = "test_segment_file";

    @BeforeClass
    public static void setup() {
        threadPool = new TestThreadPool(ReadContextListenerTests.class.getName());
    }

    @AfterClass
    public static void cleanup() {
        threadPool.shutdown();
    }

    @Before
    public void init() throws Exception {
        path = createTempDir("ReadContextListenerTests");
    }

    public void testReadContextListener() throws InterruptedException, IOException {
        Path fileLocation = path.resolve(UUID.randomUUID().toString());
        List<InputStreamContainer> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(TEST_SEGMENT_FILE, fileLocation, threadPool, completionListener);
        ReadContext readContext = new ReadContext((long) PART_SIZE * NUMBER_OF_PARTS, blobPartStreams, null);
        readContextListener.onResponse(readContext);

        countDownLatch.await();

        assertTrue(Files.exists(fileLocation));
        assertEquals(NUMBER_OF_PARTS * PART_SIZE, Files.size(fileLocation));
    }

    public void testReadContextListenerFailure() throws InterruptedException {
        Path fileLocation = path.resolve(UUID.randomUUID().toString());
        List<InputStreamContainer> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(TEST_SEGMENT_FILE, fileLocation, threadPool, completionListener);
        InputStream badInputStream = new InputStream() {

            @Override
            public int read(byte[] b, int off, int len) throws IOException {
                return read();
            }

            @Override
            public int read() throws IOException {
                throw new IOException();
            }

            @Override
            public int available() {
                return PART_SIZE;
            }
        };

        blobPartStreams.add(NUMBER_OF_PARTS, new InputStreamContainer(badInputStream, PART_SIZE, PART_SIZE * NUMBER_OF_PARTS));
        ReadContext readContext = new ReadContext((long) (PART_SIZE + 1) * NUMBER_OF_PARTS, blobPartStreams, null);
        readContextListener.onResponse(readContext);

        countDownLatch.await();

        assertFalse(Files.exists(fileLocation));
    }

    public void testReadContextListenerException() {
        Path fileLocation = path.resolve(UUID.randomUUID().toString());
        CountingCompletionListener<String> listener = new CountingCompletionListener<String>();
        ReadContextListener readContextListener = new ReadContextListener(TEST_SEGMENT_FILE, fileLocation, threadPool, listener);
        IOException exception = new IOException();
        readContextListener.onFailure(exception);
        assertEquals(1, listener.getFailureCount());
        assertEquals(exception, listener.getException());
    }

    private List<InputStreamContainer> initializeBlobPartStreams() {
        List<InputStreamContainer> blobPartStreams = new ArrayList<>();
        for (int partNumber = 0; partNumber < NUMBER_OF_PARTS; partNumber++) {
            InputStream testStream = new ByteArrayInputStream(randomByteArrayOfLength(PART_SIZE));
            blobPartStreams.add(new InputStreamContainer(testStream, PART_SIZE, (long) partNumber * PART_SIZE));
        }
        return blobPartStreams;
    }
}
