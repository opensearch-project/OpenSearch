/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;
import org.opensearch.transport.client.Client;

import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@SuppressWarnings("unchecked")
public class TransportWriteAdvancedSettingsActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Client client;
    private TransportWriteAdvancedSettingsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        action = new TransportWriteAdvancedSettingsAction(
            mock(TransportService.class),
            new ActionFilters(java.util.Collections.emptySet()),
            client
        );
    }

    @Override
    public void tearDown() throws Exception {
        super.tearDown();
        ThreadPool.terminate(threadPool, 10, TimeUnit.SECONDS);
    }

    public void testWriteSettingsUpdateSuccess() throws Exception {
        Map<String, Object> document = Map.of("theme:darkMode", true, "dateFormat", "YYYY-MM-DD");

        IndexResponse indexResponse = new IndexResponse(new ShardId(".opensearch_dashboards", "_na_", 0), "config:3.7.0", 1, 1, 1, true);

        doAnswer(invocation -> {
            IndexRequest indexRequest = invocation.getArgument(0);
            assertEquals(".opensearch_dashboards", indexRequest.index());
            assertEquals("config:3.7.0", indexRequest.id());
            assertEquals(DocWriteRequest.OpType.INDEX, indexRequest.opType());
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AdvancedSettingsResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0", document);

        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(AdvancedSettingsResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(exceptionRef.get());
        assertNotNull(responseRef.get());
        assertEquals(document, responseRef.get().getSettings());
    }

    public void testWriteSettingsCreateSuccess() throws Exception {
        Map<String, Object> document = Map.of("theme:darkMode", false);

        IndexResponse indexResponse = new IndexResponse(new ShardId(".opensearch_dashboards", "_na_", 0), "config:3.7.0", 1, 1, 1, true);

        doAnswer(invocation -> {
            IndexRequest indexRequest = invocation.getArgument(0);
            assertEquals(".opensearch_dashboards", indexRequest.index());
            assertEquals("config:3.7.0", indexRequest.id());
            assertEquals(DocWriteRequest.OpType.CREATE, indexRequest.opType());
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onResponse(indexResponse);
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AdvancedSettingsResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest(
            ".opensearch_dashboards",
            "config:3.7.0",
            document,
            WriteAdvancedSettingsRequest.OperationType.CREATE
        );

        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(AdvancedSettingsResponse response) {
                responseRef.set(response);
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNull(exceptionRef.get());
        assertNotNull(responseRef.get());
        assertEquals(document, responseRef.get().getSettings());
    }

    public void testWriteSettingsFailure() throws Exception {
        Map<String, Object> document = Map.of("key", "value");
        RuntimeException indexException = new RuntimeException("index write failed");

        doAnswer(invocation -> {
            ActionListener<IndexResponse> listener = invocation.getArgument(1);
            listener.onFailure(indexException);
            return null;
        }).when(client).index(any(IndexRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        WriteAdvancedSettingsRequest request = new WriteAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0", document);

        action.doExecute(mock(Task.class), request, new ActionListener<>() {
            @Override
            public void onResponse(AdvancedSettingsResponse response) {
                latch.countDown();
            }

            @Override
            public void onFailure(Exception e) {
                exceptionRef.set(e);
                latch.countDown();
            }
        });

        assertTrue(latch.await(5, TimeUnit.SECONDS));
        assertNotNull(exceptionRef.get());
        assertSame(indexException, exceptionRef.get());
    }
}
