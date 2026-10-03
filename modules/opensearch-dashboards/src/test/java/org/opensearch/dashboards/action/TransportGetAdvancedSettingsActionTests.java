/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards.action;

import org.opensearch.OpenSearchStatusException;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.get.GetResult;
import org.opensearch.index.seqno.SequenceNumbers;
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
public class TransportGetAdvancedSettingsActionTests extends OpenSearchTestCase {

    private ThreadPool threadPool;
    private Client client;
    private TransportGetAdvancedSettingsAction action;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        client = mock(Client.class);
        when(client.threadPool()).thenReturn(threadPool);
        action = new TransportGetAdvancedSettingsAction(
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

    public void testGetSettingsSuccess() throws Exception {
        String source = "{\"theme:darkMode\":true,\"dateFormat\":\"YYYY-MM-DD\"}";
        GetResult getResult = new GetResult(".opensearch_dashboards", "config:3.7.0", 0, 1, 1, true, new BytesArray(source), null, null);
        GetResponse getResponse = new GetResponse(getResult);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<AdvancedSettingsResponse> responseRef = new AtomicReference<>();
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        GetAdvancedSettingsRequest request = new GetAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0");
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
        Map<String, Object> settings = responseRef.get().getSettings();
        assertEquals(true, settings.get("theme:darkMode"));
        assertEquals("YYYY-MM-DD", settings.get("dateFormat"));
    }

    public void testGetSettingsNotFound() throws Exception {
        GetResult getResult = new GetResult(
            ".opensearch_dashboards",
            "config:3.7.0",
            SequenceNumbers.UNASSIGNED_SEQ_NO,
            SequenceNumbers.UNASSIGNED_PRIMARY_TERM,
            0,
            false,
            null,
            null,
            null
        );
        GetResponse getResponse = new GetResponse(getResult);

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onResponse(getResponse);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        GetAdvancedSettingsRequest request = new GetAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0");
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
        assertTrue(exceptionRef.get() instanceof OpenSearchStatusException);
        assertEquals(RestStatus.NOT_FOUND, ((OpenSearchStatusException) exceptionRef.get()).status());
    }

    public void testGetSettingsIndexNotFound() throws Exception {
        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(new IndexNotFoundException(".opensearch_dashboards"));
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        GetAdvancedSettingsRequest request = new GetAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0");
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
        assertTrue(exceptionRef.get() instanceof OpenSearchStatusException);
        assertEquals(RestStatus.NOT_FOUND, ((OpenSearchStatusException) exceptionRef.get()).status());
    }

    public void testGetSettingsGenericFailure() throws Exception {
        RuntimeException genericException = new RuntimeException("cluster unavailable");

        doAnswer(invocation -> {
            ActionListener<GetResponse> listener = invocation.getArgument(1);
            listener.onFailure(genericException);
            return null;
        }).when(client).get(any(GetRequest.class), any(ActionListener.class));

        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Exception> exceptionRef = new AtomicReference<>();

        GetAdvancedSettingsRequest request = new GetAdvancedSettingsRequest(".opensearch_dashboards", "config:3.7.0");
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
        assertSame(genericException, exceptionRef.get());
    }
}
