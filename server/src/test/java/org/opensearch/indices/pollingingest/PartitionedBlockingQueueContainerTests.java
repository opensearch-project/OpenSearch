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
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.indices.pollingingest.mappers.DefaultIngestionMessageMapper;
import org.opensearch.indices.pollingingest.mappers.IngestionMessageMapper;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
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
}
