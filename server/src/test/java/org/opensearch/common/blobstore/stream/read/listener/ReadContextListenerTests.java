/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.blobstore.stream.read.listener;

import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.function.UnaryOperator;

import static org.opensearch.common.blobstore.stream.read.listener.ListenerTestUtils.CountingCompletionListener;

/*
    WindowsFS tries to simulate file handles in a best case simulation.
    The deletion for the open file on an actual Windows system will be performed as soon as the last handle
    is closed, which this simulation does not account for. Preventing use of WindowsFS for these tests.
 */
@SuppressFileSystems("WindowsFS")
public class ReadContextListenerTests extends OpenSearchTestCase {

    private Path path;
    private static ThreadPool threadPool;
    private static final int NUMBER_OF_PARTS = 5;
    private static final int PART_SIZE = 10;
    private static final String TEST_SEGMENT_FILE = "test_segment_file";
    private static final int MAX_CONCURRENT_STREAMS = 10;

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
        List<ReadContext.StreamPartCreator> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(
            TEST_SEGMENT_FILE,
            fileLocation,
            completionListener,
            threadPool,
            UnaryOperator.identity(),
            MAX_CONCURRENT_STREAMS
        );
        ReadContext readContext = new ReadContext.Builder((long) PART_SIZE * NUMBER_OF_PARTS, blobPartStreams).build();
        readContextListener.onResponse(readContext);

        countDownLatch.await();

        assertTrue(Files.exists(fileLocation));
        assertEquals(NUMBER_OF_PARTS * PART_SIZE, Files.size(fileLocation));
    }

    public void testReadContextListenerFailure() throws Exception {
        Path fileLocation = path.resolve(UUID.randomUUID().toString());
        List<ReadContext.StreamPartCreator> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(
            TEST_SEGMENT_FILE,
            fileLocation,
            completionListener,
            threadPool,
            UnaryOperator.identity(),
            MAX_CONCURRENT_STREAMS
        );
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

        blobPartStreams.add(
            NUMBER_OF_PARTS,
            () -> CompletableFuture.supplyAsync(
                () -> new InputStreamContainer(badInputStream, PART_SIZE, PART_SIZE * NUMBER_OF_PARTS),
                threadPool.generic()
            )
        );
        ReadContext readContext = new ReadContext.Builder((long) (PART_SIZE + 1) * NUMBER_OF_PARTS, blobPartStreams).build();
        readContextListener.onResponse(readContext);

        countDownLatch.await();
        assertFalse(Files.exists(fileLocation));
        assertFalse(Files.exists(readContextListener.getTmpFileLocation()));
    }

    public void testReadContextListenerException() {
        Path fileLocation = path.resolve(UUID.randomUUID().toString());
        CountingCompletionListener<String> listener = new CountingCompletionListener<String>();
        ReadContextListener readContextListener = new ReadContextListener(
            TEST_SEGMENT_FILE,
            fileLocation,
            listener,
            threadPool,
            UnaryOperator.identity(),
            MAX_CONCURRENT_STREAMS
        );
        IOException exception = new IOException();
        readContextListener.onFailure(exception);
        assertEquals(1, listener.getFailureCount());
        assertEquals(exception, listener.getException());
    }

    public void testWriteToTempFile() throws Exception {
        final String fileName = UUID.randomUUID().toString();
        Path fileLocation = path.resolve(fileName);
        List<ReadContext.StreamPartCreator> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(
            TEST_SEGMENT_FILE,
            fileLocation,
            completionListener,
            threadPool,
            UnaryOperator.identity(),
            MAX_CONCURRENT_STREAMS
        );
        ByteArrayInputStream assertingStream = new ByteArrayInputStream(randomByteArrayOfLength(PART_SIZE)) {
            @Override
            public int read(byte[] b) throws IOException {
                assertTrue("parts written to temp file location", Files.exists(readContextListener.getTmpFileLocation()));
                return super.read(b);
            }
        };
        blobPartStreams.add(
            NUMBER_OF_PARTS,
            () -> CompletableFuture.supplyAsync(
                () -> new InputStreamContainer(assertingStream, PART_SIZE, PART_SIZE * NUMBER_OF_PARTS),
                threadPool.generic()
            )
        );
        ReadContext readContext = new ReadContext.Builder((long) (PART_SIZE + 1) * NUMBER_OF_PARTS + 1, blobPartStreams).build();
        readContextListener.onResponse(readContext);

        countDownLatch.await();
        assertTrue(Files.exists(fileLocation));
        assertFalse(Files.exists(readContextListener.getTmpFileLocation()));
    }

    public void testWriteToTempFile_alreadyExists_replacesFile() throws Exception {
        final String fileName = UUID.randomUUID().toString();
        Path fileLocation = path.resolve(fileName);
        // create an empty file at location.
        Files.createFile(fileLocation);
        assertEquals(0, Files.readAllBytes(fileLocation).length);
        List<ReadContext.StreamPartCreator> blobPartStreams = initializeBlobPartStreams();
        CountDownLatch countDownLatch = new CountDownLatch(1);
        ActionListener<String> completionListener = new LatchedActionListener<>(new PlainActionFuture<>(), countDownLatch);
        ReadContextListener readContextListener = new ReadContextListener(
            TEST_SEGMENT_FILE,
            fileLocation,
            completionListener,
            threadPool,
            UnaryOperator.identity(),
            MAX_CONCURRENT_STREAMS
        );
        ReadContext readContext = new ReadContext.Builder((long) (PART_SIZE + 1) * NUMBER_OF_PARTS, blobPartStreams).build();
        readContextListener.onResponse(readContext);

        countDownLatch.await();
        assertTrue(Files.exists(fileLocation));
        assertEquals(50, Files.readAllBytes(fileLocation).length);
        assertFalse(Files.exists(readContextListener.getTmpFileLocation()));
    }

    private List<ReadContext.StreamPartCreator> initializeBlobPartStreams() {
        List<ReadContext.StreamPartCreator> blobPartStreams = new ArrayList<>();
        for (int partNumber = 0; partNumber < NUMBER_OF_PARTS; partNumber++) {
            InputStream testStream = new ByteArrayInputStream(randomByteArrayOfLength(PART_SIZE));
            int finalPartNumber = partNumber;
            blobPartStreams.add(
                () -> CompletableFuture.supplyAsync(
                    () -> new InputStreamContainer(testStream, PART_SIZE, (long) finalPartNumber * PART_SIZE),
                    threadPool.generic()
                )
            );
        }
        return blobPartStreams;
    }
}
