/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.pollingingest;

import org.opensearch.index.IngestionShardPointer;
import org.opensearch.index.engine.FakeIngestionSource;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class DefaultStreamPollerTests extends OpenSearchTestCase {
    private DefaultStreamPoller poller;
    private FakeIngestionSource.FakeIngestionConsumer fakeConsumer;
    private MessageProcessorRunnable processorRunnable;
    private MessageProcessorRunnable.MessageProcessor processor;
    private List<byte[]> messages;
    private Set<IngestionShardPointer> persistedPointers;
    private final int sleepTime = 300;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        messages = new ArrayList<>();
        ;
        messages.add("{\"_id\":\"1\",\"_source\":{\"name\":\"bob\", \"age\": 24}}".getBytes(StandardCharsets.UTF_8));
        messages.add("{\"_id\":\"2\",\"_source\":{\"name\":\"alice\", \"age\": 21}}".getBytes(StandardCharsets.UTF_8));
        fakeConsumer = new FakeIngestionSource.FakeIngestionConsumer(messages, 0);
        processor = mock(MessageProcessorRunnable.MessageProcessor.class);
        processorRunnable = new MessageProcessorRunnable(new ArrayBlockingQueue<>(5), processor);
        persistedPointers = new HashSet<>();
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.NONE
        );
    }

    @After
    public void tearDown() throws Exception {
        if (!poller.isClosed()) {
            poller.close();
        }
        super.tearDown();
    }

    public void testPauseAndResume() throws InterruptedException {
        poller.pause();
        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        assertEquals(DefaultStreamPoller.State.PAUSED, poller.getState());
        assertTrue(poller.isPaused());
        // no messages are processed
        verify(processor, never()).process(any(), any());

        poller.resume();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
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
            processorRunnable,
            StreamPoller.ResetState.NONE
        );
        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
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
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        poller.close();
        assertTrue(poller.isClosed());
        assertEquals(DefaultStreamPoller.State.CLOSED, poller.getState());
    }

    public void testResetStateEarliest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(1),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.EARLIEST
        );

        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run

        // 2 messages are processed
        verify(processor, times(2)).process(any(), any());
    }

    public void testResetStateLatest() throws InterruptedException {
        poller = new DefaultStreamPoller(
            new FakeIngestionSource.FakeIngestionShardPointer(0),
            persistedPointers,
            fakeConsumer,
            processorRunnable,
            StreamPoller.ResetState.LATEST
        );

        poller.start();
        Thread.sleep(sleepTime); // Allow some time for the poller to run
        // no messages processed
        verify(processor, never()).process(any(), any());
        // reset to the latest
        assertEquals(new FakeIngestionSource.FakeIngestionShardPointer(2), poller.getBatchStartPointer());
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
        Thread.sleep(sleepTime);
        poller.close();
        try {
            poller.startPoll();
            fail("Expected an exception to be thrown");
        } catch (IllegalStateException e) {
            assertEquals("poller is closed!", e.getMessage());
        }
    }
}
