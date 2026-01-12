/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.block.ClusterBlock;
import org.opensearch.cluster.block.ClusterBlockLevel;
import org.opensearch.cluster.block.ClusterBlocks;
import org.opensearch.cluster.metadata.IngestionSource;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionConsumerFactory;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.indices.pollingingest.mappers.DefaultIngestionMessageMapper;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumSet;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class DefaultStreamPollerTests extends OpenSearchTestCase {
    private DefaultStreamPoller poller;
    private FakeIngestionSource.FakeIngestionConsumerFactory fakeConsumerFactory;
    private MessageProcessorRunnable processorRunnable;
    private MessageProcessorRunnable.MessageProcessor processor;
    private List<byte[]> messages;
    private final int awaitTime = 300;
    private final int sleepTime = 300;
    private DropIngestionErrorStrategy errorStrategy;
    private PartitionedBlockingQueueContainer partitionedBlockingQueueContainer;
    private IngestionEngine engine;
    private IndexSettings indexSettings;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messages = new ArrayList<>();
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"2\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        fakeConsumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);
        processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        errorStrategy = new DropIngestionErrorStrategy("ingestion_source");
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, errorStrategy, "test_index", 0);
        partitionedBlockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        engine = mock(IngestionEngine.class);
        indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            fakeConsumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        partitionedBlockingQueueContainer.startProcessorThreads();
    }

    @After
    public void tearDown() throws Exception {
        if (!poller.isClosed()) {
            poller.close();
        }
        partitionedBlockingQueueContainer.close();
        super.tearDown();
    }

    public void testPauseAndResume() throws InterruptedException {
        // We'll use a latch that counts the number of messages processed.
        CountDownLatch pauseLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            pauseLatch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.pause();
        poller.start();

        // Wait briefly to ensure that no processing occurs.
        boolean processedWhilePaused = pauseLatch.await(awaitTime, TimeUnit.MILLISECONDS);
        // Expecting the latch NOT to reach zero because we are paused.
        assertFalse("Messages should not be processed while paused", processedWhilePaused);
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
        verify(processor, never()).process(any(), any());

        CountDownLatch resumeLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            resumeLatch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.resume();
        resumeLatch.await();
        assertFalse(poller.isPaused());
        // 2 messages are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testCloseWithoutStart() {
        poller.close();
        assertTrue(poller.isClosed());
    }

    public void testClose() throws InterruptedException {
        poller.start();
        waitUntil(() -> poller.getState() == DefaultStreamPoller.State.POLLING, awaitTime, TimeUnit.MILLISECONDS);
        poller.close();
        assertTrue(poller.isClosed());
        assertEquals(DefaultStreamPoller.State.CLOSED, poller.getState());
    }

    public void testResetStateEarliest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(1),
            fakeConsumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.EARLIEST,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        CountDownLatch latch = new CountDownLatch(2);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.start();
        latch.await();

        // 2 messages are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testResetStateLatest() throws InterruptedException {
        // Clear messages first and add them after poller starts
        // This ensures latestPointer() returns the correct value at initialization time
        messages.clear();

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            fakeConsumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.LATEST,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );

        // Set up latch to wait for 2 messages to be processed
        CountDownLatch latch = new CountDownLatch(2);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.start();
        waitUntil(() -> poller.getState() == DefaultStreamPoller.State.POLLING, awaitTime, TimeUnit.MILLISECONDS);

        // Verify batch start pointer was set to latest (which is 0 since messages list was empty)
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(0), poller.getBatchStartPointer());

        // Now add messages after poller has started with LATEST reset
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"2\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));

        // Wait for messages to be processed
        latch.await();

        // Verify that the messages added after starting from latest are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testResetStateRewindByOffset() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(2),
            fakeConsumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.RESET_BY_OFFSET,
            "1",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.start();
        latch.await();
        // 1 message is processed
        verify(processor, times(1)).process(any(), any());
    }

    public void testStartPollWithoutStart() {
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is not started!", e.getMessage());
        }
    }

    public void testStartClosedPoller() throws InterruptedException {
        poller.start();
        waitUntil(() -> poller.getState() == DefaultStreamPoller.State.POLLING, awaitTime, TimeUnit.MILLISECONDS);
        poller.close();
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is closed!", e.getMessage());
        }
    }

    public void testDropErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        FakeIngestionSource.FakeIngestionConsumer fakeConsumer = fakeConsumerFactory.createShardConsumer("", 0);
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch1 = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    true,
                    2,
                    100
                );
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch2 = fakeConsumer.readNext(
                    new FakeIngestionSource.FakeIngestionShardPointer(1),
                    true,
                    2,
                    100
                );
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyBoolean(), anyLong(), anyInt())).thenReturn(readResultsBatch1);
        when(mockConsumer.readNext(anyLong(), anyInt())).thenReturn(readResultsBatch2).thenReturn(Collections.emptyList());

        IngestionErrorStrategy errorStrategy = spy(new DropIngestionErrorStrategy("ingestion_source"));
        ArrayBlockingQueue mockQueue = mock(ArrayBlockingQueue.class);
        doThrow(new RuntimeException()).doNothing().when(mockQueue).put(any());
        processorRunnable = new MessageProcessorRunnable(mockQueue, processor, errorStrategy, "test_index", 0);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();

        IngestionConsumerFactory mockConsumerFactory = mock(IngestionConsumerFactory.class);
        when(mockConsumerFactory.createShardConsumer(anyString(), anyInt())).thenReturn(mockConsumer);

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            mockConsumerFactory,
            "",
            0,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        poller.start();
        Thread.sleep(sleepTime);
        PollingIngestStats pollingIngestStats = poller.getStats();

        assertThat(pollingIngestStats.getConsumerStats().totalPollerMessageFailureCount(), is(1L));
        assertThat(pollingIngestStats.getConsumerStats().totalPollerMessageDroppedCount(), is(1L));
        verify(errorStrategy, times(1)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.POLLING));
        verify(mockQueue, times(4)).put(any());
        blockingQueueContainer.close();
    }

    public void testBlockErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        FakeIngestionSource.FakeIngestionConsumer fakeConsumer = fakeConsumerFactory.createShardConsumer("", 0);
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch1 = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    true,
                    2,
                    100
                );
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch2 = fakeConsumer.readNext(
                    new FakeIngestionSource.FakeIngestionShardPointer(1),
                    true,
                    2,
                    100
                );
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyBoolean(), anyLong(), anyInt())).thenReturn(readResultsBatch1);
        when(mockConsumer.readNext(anyLong(), anyInt())).thenReturn(readResultsBatch2).thenReturn(Collections.emptyList());

        IngestionErrorStrategy errorStrategy = spy(new BlockIngestionErrorStrategy("ingestion_source"));
        ArrayBlockingQueue mockQueue = mock(ArrayBlockingQueue.class);
        doThrow(new RuntimeException()).doNothing().when(mockQueue).put(any());
        processorRunnable = new MessageProcessorRunnable(mockQueue, processor, errorStrategy, "test_index", 0);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();
        IngestionConsumerFactory mockConsumerFactory = mock(IngestionConsumerFactory.class);
        when(mockConsumerFactory.createShardConsumer(anyString(), anyInt())).thenReturn(mockConsumer);

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            mockConsumerFactory,
            "",
            0,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        poller.start();
        Thread.sleep(sleepTime);

        PollingIngestStats pollingIngestStats = poller.getStats();
        assertThat(pollingIngestStats.getConsumerStats().totalPollerMessageDroppedCount(), is(0L));
        verify(errorStrategy, times(1)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.POLLING));
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
        blockingQueueContainer.close();
    }

    public void testProcessingErrorWithBlockErrorIngestionStrategy() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));

        doThrow(new RuntimeException("Error processing update")).when(processor).process(any(), any());
        BlockIngestionErrorStrategy mockErrorStrategy = spy(new BlockIngestionErrorStrategy("ingestion_source"));
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, mockErrorStrategy, "test_index", 0);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            fakeConsumerFactory,
            "",
            0,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            mockErrorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        poller.start();
        Thread.sleep(sleepTime);

        verify(mockErrorStrategy, times(1)).handleError(any(), eq(IngestionErrorStrategy.ErrorStage.PROCESSING));
        verify(processor, times(1)).process(any(), any());
        // poller will continue to poll if an error is encountered during message processing but will be blocked by
        // the write to blockingQueue
        assertEquals(DefaultStreamPoller.State.POLLING, poller.getState());
        blockingQueueContainer.close();
    }

    public void testUpdateErrorStrategy() {
        assertTrue(poller.getErrorStrategy() instanceof DropIngestionErrorStrategy);
        assertTrue(processorRunnable.getErrorStrategy() instanceof DropIngestionErrorStrategy);
        poller.updateErrorStrategy(new BlockIngestionErrorStrategy("ingestion_source"));
        assertTrue(poller.getErrorStrategy() instanceof BlockIngestionErrorStrategy);
        assertTrue(processorRunnable.getErrorStrategy() instanceof BlockIngestionErrorStrategy);
    }

    public void testPersistedBatchStartPointer() throws TimeoutException, InterruptedException {
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"4\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        FakeIngestionSource.FakeIngestionConsumer fakeConsumer = fakeConsumerFactory.createShardConsumer("", 0);
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch1 = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    true,
                    2,
                    100
                );
        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResultsBatch2 = fakeConsumer.readNext(
                    new FakeIngestionSource.FakeIngestionShardPointer(2),
                    true,
                    2,
                    100
                );

        // This test publishes 4 messages, so use blocking queue of size 3. This ensures the poller is blocked when adding the 4th message
        // for validation.
        IngestionErrorStrategy errorStrategy = spy(new BlockIngestionErrorStrategy("ingestion_source"));
        doThrow(new RuntimeException()).when(processor).process(any(), any());
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(3), processor, errorStrategy, "test_index", 0);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyBoolean(), anyLong(), anyInt())).thenReturn(readResultsBatch1);

        when(mockConsumer.readNext(anyLong(), anyInt())).thenReturn(readResultsBatch2).thenReturn(Collections.emptyList());
        IngestionConsumerFactory mockConsumerFactory = mock(IngestionConsumerFactory.class);
        when(mockConsumerFactory.createShardConsumer(anyString(), anyInt())).thenReturn(mockConsumer);

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            mockConsumerFactory,
            "",
            0,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );
        poller.start();
        Thread.sleep(sleepTime);

        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(0), poller.getBatchStartPointer());
        blockingQueueContainer.close();
    }

    public void testClusterStateChange() {
        // set write block
        ClusterState state1 = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterState state2 = ClusterState.builder(ClusterName.DEFAULT)
            .blocks(
                ClusterBlocks.builder()
                    .addGlobalBlock(
                        new ClusterBlock(1, "description", true, true, true, RestStatus.ACCEPTED, EnumSet.allOf((ClusterBlockLevel.class)))
                    )
            )
            .build();

        ClusterChangedEvent event1 = new ClusterChangedEvent("test", state2, state1);
        poller.clusterChanged(event1);
        assertTrue(poller.isWriteBlockEnabled());

        // remove write block
        ClusterState state3 = ClusterState.builder(ClusterName.DEFAULT).build();
        ClusterChangedEvent event2 = new ClusterChangedEvent("test", state3, state2);
        poller.clusterChanged(event2);
        assertFalse(poller.isWriteBlockEnabled());

        // test no block change
        ClusterChangedEvent event3 = new ClusterChangedEvent("test", state3, state3);
        poller.clusterChanged(event3);
        assertFalse(poller.isWriteBlockEnabled());
    }

    public void testErrorApplyingClusterChange() {
        ClusterChangedEvent event = mock(ClusterChangedEvent.class);
        doThrow(new RuntimeException()).when(event).blocksChanged();
        assertThrows(RuntimeException.class, () -> poller.clusterChanged(event));
    }

    public void testConsumerInitializationRetry() throws Exception {
        // Create a mock consumer that will be returned on successful initialization
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(anyLong(), anyInt())).thenReturn(Collections.emptyList());

        // Create a mock consumer factory that fails on first call but succeeds on second call
        IngestionConsumerFactory mockConsumerFactory = mock(IngestionConsumerFactory.class);
        when(mockConsumerFactory.createShardConsumer(anyString(), anyInt())).thenThrow(
            new RuntimeException("Simulated consumer initialization failure")
        ).thenReturn(mockConsumer);

        // Create a poller with the mock factory
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            mockConsumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );

        poller.start();
        assertBusy(() -> {
            PollingIngestStats stats = poller.getStats();
            assertEquals(1, stats.getConsumerStats().totalConsumerErrorCount());
            assertEquals(StreamPoller.State.POLLING, poller.getState());
        }, 30, TimeUnit.SECONDS);

        // Verify the consumer factory was called twice (once for failure, once for success)
        verify(mockConsumerFactory, times(2)).createShardConsumer(anyString(), anyInt());
        assertNotNull(poller.getConsumer());
    }

    public void testConsumerReinitializationAfterProcessingMessages() throws Exception {
        // Initially publish 2 messages, and later publish 3rd message after consumer reinitialization
        messages.clear();
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"2\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));

        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);

        CountDownLatch initialLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            initialLatch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            consumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );

        // Start and wait for 2 messages to be processed
        poller.start();
        initialLatch.await();
        verify(processor, times(2)).process(any(), any());

        // Request consumer reinitialization
        CountDownLatch reinitLatch = new CountDownLatch(2);  // Expect 2 more messages: message 2 (reprocessed) and message 3 (new)
        doAnswer(invocation -> {
            reinitLatch.countDown();
            return null;
        }).when(processor).process(any(), any());

        // Create a mock ingestion source for reinitialization
        IngestionSource mockIngestionSource = new IngestionSource.Builder("test").build();
        poller.requestConsumerReinitialization(mockIngestionSource);

        // Add a 3rd message
        messages.add("{\"_id\":\"3\",\"_source\":{\"name\":\"charlie\", \"age\": 30}}".getBytes(StandardCharsets.UTF_8));

        // Wait for reprocessing of message 2 and processing of message 3
        reinitLatch.await();

        // Expect total 4 messages, as 2nd message is processed twice due to consumer reinitialization
        verify(processor, times(4)).process(any(), any());
    }

    public void testConsumerReinitializationWithNoInitialMessages() throws Exception {
        // Start with no messages
        messages.clear();

        // Set up latch to wait for message processing
        CountDownLatch latch = new CountDownLatch(1);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(processor).process(any(), any());

        FakeIngestionSource.FakeIngestionConsumerFactory consumerFactory = new FakeIngestionSource.FakeIngestionConsumerFactory(messages);

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            consumerFactory,
            "",
            0,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );

        // Start poller
        poller.start();
        Thread.sleep(sleepTime);

        // Verify no messages processed
        verify(processor, never()).process(any(), any());

        // Request consumer reinitialization
        IngestionSource mockIngestionSource = new IngestionSource.Builder("test").build();
        poller.requestConsumerReinitialization(mockIngestionSource);

        // Add a message
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));

        // Wait for the message to be processed
        assertTrue("Message should be processed within timeout", latch.await(30, TimeUnit.SECONDS));

        // Verify 1 message was processed
        verify(processor, times(1)).process(any(), any());
    }

    public void testGetBatchStartPointerWithNullInitialPointer() {
        // Create a mock blocking queue container that returns null pointers
        PartitionedBlockingQueueContainer mockContainer = mock(PartitionedBlockingQueueContainer.class);
        when(mockContainer.getCurrentShardPointers()).thenReturn(Arrays.asList(null, null, null));

        // Create poller with null initial batch start pointer
        poller = new DefaultStreamPoller(
            null,
            fakeConsumerFactory,
            "",
            0,
            mockContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            10000,
            indexSettings,
            new DefaultIngestionMessageMapper()
        );

        // When all queues return null and initialBatchStartPointer is null, getBatchStartPointer should return null
        assertNull(poller.getBatchStartPointer());
    }
}
