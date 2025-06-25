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
import org.opensearch.common.settings.Settings;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.index.engine.IngestionEngine;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
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
    private FakeIngestionSource.FakeIngestionConsumer fakeConsumer;
    private MessageProcessorRunnable processorRunnable;
    private MessageProcessorRunnable.MessageProcessor processor;
    private List<byte[]> messages;
    private Set<IngestionShardPointer> persistedPointers;
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
        fakeConsumer = new FakeIngestionSource.FakeIngestionConsumer(messages, 0);
        processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        errorStrategy = new DropIngestionErrorStrategy("ingestion_source");
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, errorStrategy);
        persistedPointers = new HashSet<>();
        partitionedBlockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        engine = mock(IngestionEngine.class);
        indexSettings = IndexSettingsModule.newIndexSettings("index", Settings.EMPTY);
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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

    public void testSkipProcessed() throws InterruptedException {
        messages.add("{\"name\":\"cathy\", \"age\": 21}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"name\":\"danny\", \"age\": 31}".getBytes(StandardCharsets.UTF_8));
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(1));
        persistedPointers.add(new FakeIngestionSource.FakeIngestionShardPointer(2));
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
        );

        CountDownLatch latch = new CountDownLatch(2);
        doAnswer(invocation -> {
            latch.countDown();
            return null;
        }).when(processor).process(any(), any());

        poller.start();
        latch.await();
        // 2 messages are processed, 2 messages are skipped
        verify(processor, times(2)).process(any(), any());
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getMaxPersistedPointer());
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
            persistedPointers,
            fakeConsumer,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.EARLIEST,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.LATEST,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
        );

        poller.start();
        waitUntil(() -> poller.getState() == DefaultStreamPoller.State.POLLING, awaitTime, TimeUnit.MILLISECONDS);
        // no messages processed
        verify(processor, never()).process(any(), any());
        // reset to the latest
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getBatchStartPointer());
    }

    public void testResetStateRewindByOffset() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(2),
            persistedPointers,
            fakeConsumer,
            partitionedBlockingQueueContainer,
            StreamPoller.ResetState.RESET_BY_OFFSET,
            "1",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
        processorRunnable = new MessageProcessorRunnable(mockQueue, processor, errorStrategy);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            mockConsumer,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
        processorRunnable = new MessageProcessorRunnable(mockQueue, processor, errorStrategy);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            mockConsumer,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor, mockErrorStrategy);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            mockErrorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(3), processor, errorStrategy);
        PartitionedBlockingQueueContainer blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        blockingQueueContainer.startProcessorThreads();
        IngestionShardConsumer mockConsumer = mock(IngestionShardConsumer.class);
        when(mockConsumer.getShardId()).thenReturn(0);
        when(mockConsumer.readNext(any(), anyBoolean(), anyLong(), anyInt())).thenReturn(readResultsBatch1);

        when(mockConsumer.readNext(anyLong(), anyInt())).thenReturn(readResultsBatch2).thenReturn(Collections.emptyList());

        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            mockConsumer,
            blockingQueueContainer,
            StreamPoller.ResetState.NONE,
            "",
            errorStrategy,
            StreamPoller.State.NONE,
            1000,
            1000,
            indexSettings
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
}
