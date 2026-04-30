/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.core.common.Strings;
import org.opensearch.index.IngestionShardConsumer;
import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.indices.pollingingest.mappers.DefaultIngestionMessageMapper;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeoutException;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class PartitionedBlockingQueueContainerTests extends OpenSearchTestCase {
    private MessageProcessorRunnable processorRunnable;
    private MessageProcessorRunnable.MessageProcessor processor;
    private PartitionedBlockingQueueContainer blockingQueueContainer;
    private FakeIngestionSource.FakeIngestionConsumer fakeConsumer;
    private List<byte[]> messages;
    private IngestionMessageMapper mapper;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messages = new ArrayList<>();
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        fakeConsumer = new FakeIngestionSource.FakeIngestionConsumer(messages, 0);
        processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        processorRunnable = new MessageProcessorRunnable(
            new ArrayBlockingQueue<>(5),
            processor,
            new DropIngestionErrorStrategy("ingestion_source"),
            "test_index",
            0
        );
        this.blockingQueueContainer = new PartitionedBlockingQueueContainer(processorRunnable, 0);
        this.mapper = new DefaultIngestionMessageMapper();
    }

    @After
    public void tearDown() throws Exception {
        blockingQueueContainer.close();
        super.tearDown();
    }

    public void testAddMessage() throws TimeoutException, InterruptedException {
        assertEquals(1, blockingQueueContainer.getPartitionToQueueMap().size());
        assertEquals(1, blockingQueueContainer.getPartitionToMessageProcessorMap().size());
        assertEquals(1, blockingQueueContainer.getPartitionToProcessorExecutorMap().size());

        List<
            IngestionShardConsumer.ReadResult<
                FakeIngestionSource.FakeIngestionShardPointer,
                FakeIngestionSource.FakeIngestionMessage>> readResults = fakeConsumer.readNext(
                    fakeConsumer.earliestPointer(),
                    true,
                    5,
                    100
                );

        CountDownLatch updatesLatch = new CountDownLatch(2);
        doAnswer(invocation -> {
            updatesLatch.countDown();
            return null;
        }).when(processor).process(any(), any());

        for (IngestionShardConsumer.ReadResult<
            FakeIngestionSource.FakeIngestionShardPointer,
            FakeIngestionSource.FakeIngestionMessage> readResult : readResults) {
            ShardUpdateMessage shardUpdateMessage = mapper.mapAndProcess(readResult.getPointer(), readResult.getMessage());
            blockingQueueContainer.add(shardUpdateMessage);
        }

        // verify ID is present on all messages
        blockingQueueContainer.getPartitionToQueueMap()
            .get(0)
            .forEach(update -> assertTrue(Strings.hasText((String) update.parsedPayloadMap().get("_id"))));

        // start processor threads and verify updates are processed
        blockingQueueContainer.startProcessorThreads();
        updatesLatch.await();
        assertEquals(2, blockingQueueContainer.getMessageProcessorMetrics().processedCounter().count());
        verify(processor, times(2)).process(any(), any());
    }

    public void testUpdateErrorStrategy() {
        assertTrue(processorRunnable.getErrorStrategy() instanceof DropIngestionErrorStrategy);
        blockingQueueContainer.updateErrorStrategy(new BlockIngestionErrorStrategy("source"));
        assertTrue(processorRunnable.getErrorStrategy() instanceof BlockIngestionErrorStrategy);
    }

    public void testGetCurrentPartitionPointersEmptyInLegacyMode() {
        // Default test setup uses single partition with no per-partition tracking
        assertTrue(blockingQueueContainer.getCurrentPartitionPointers().isEmpty());
    }

    public void testGetCurrentPartitionPointersAggregatesMinPerPartition() {
        // Build a container with 3 internal processor threads, each tracking partition-aware pointers
        List<MessageProcessorRunnable> runnables = new ArrayList<>();
        for (int i = 0; i < 3; i++) {
            runnables.add(
                new MessageProcessorRunnable(
                    new ArrayBlockingQueue<>(1),
                    mock(MessageProcessorRunnable.MessageProcessor.class),
                    new DropIngestionErrorStrategy("ingestion_source"),
                    "test_index",
                    0
                )
            );
        }
        PartitionedBlockingQueueContainer container = new PartitionedBlockingQueueContainer(runnables, 0);

        // Source partition 3:
        // processor[0] last saw offset 100
        // processor[1] last saw offset 80 <-- min for partition 3
        // processor[2] never saw partition 3
        // Source partition 7:
        // processor[0] never saw partition 7
        // processor[1] last saw offset 50
        // processor[2] last saw offset 60 <-- 50 wins for partition 7
        // Source partition 11:
        // only processor[2] saw it at offset 999
        runnables.get(0).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 100L));
        runnables.get(1).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 80L));
        runnables.get(1).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(7, 50L));
        runnables.get(2).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(7, 60L));
        runnables.get(2).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(11, 999L));

        Map<Integer, IngestionShardPointer> aggregated = container.getCurrentPartitionPointers();

        assertEquals("Three distinct partitions should be tracked", 3, aggregated.size());
        // Partition 3: min(100, 80) = 80
        assertEquals(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 80L), aggregated.get(3));
        // Partition 7: min(50, 60) = 50
        assertEquals(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(7, 50L), aggregated.get(7));
        // Partition 11: only one observer, value passes through
        assertEquals(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(11, 999L), aggregated.get(11));

        container.close();
    }

    public void testGetCurrentPartitionPointersLatestPointerPerPartitionWithinProcessor() {
        // Within a single processor, marking newer offsets for the same partition overwrites the entry.
        // The "min across processors" logic then sees this latest value.
        List<MessageProcessorRunnable> runnables = new ArrayList<>();
        runnables.add(
            new MessageProcessorRunnable(
                new ArrayBlockingQueue<>(1),
                mock(MessageProcessorRunnable.MessageProcessor.class),
                new DropIngestionErrorStrategy("ingestion_source"),
                "test_index",
                0
            )
        );
        PartitionedBlockingQueueContainer container = new PartitionedBlockingQueueContainer(runnables, 0);

        // Same partition, increasing offsets — only the latest is retained
        runnables.get(0).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 10L));
        runnables.get(0).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 20L));
        runnables.get(0).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 30L));

        Map<Integer, IngestionShardPointer> aggregated = container.getCurrentPartitionPointers();
        assertEquals(1, aggregated.size());
        assertEquals(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 30L), aggregated.get(3));

        container.close();
    }

    public void testGetCurrentPartitionPointersReturnsImmutableMap() {
        List<MessageProcessorRunnable> runnables = new ArrayList<>();
        runnables.add(
            new MessageProcessorRunnable(
                new ArrayBlockingQueue<>(1),
                mock(MessageProcessorRunnable.MessageProcessor.class),
                new DropIngestionErrorStrategy("ingestion_source"),
                "test_index",
                0
            )
        );
        PartitionedBlockingQueueContainer container = new PartitionedBlockingQueueContainer(runnables, 0);
        runnables.get(0).markCurrentPointer(new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(3, 10L));

        Map<Integer, IngestionShardPointer> aggregated = container.getCurrentPartitionPointers();
        expectThrows(
            UnsupportedOperationException.class,
            () -> aggregated.put(99, new MessageProcessorRunnablePerPartitionTests.TestSourcePartitionAwarePointer(99, 1L))
        );

        container.close();
    }
}
