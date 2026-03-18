/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.action;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.bulk.BulkAction;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.search.SearchAction;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.support.ActionFilterChain;
import org.opensearch.action.support.ActionRequestMetadata;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.tasks.Task;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.client.node.NodeClient;

import static org.mockito.Mockito.any;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;

@SuppressWarnings("unchecked")
public class SearchActionFilterTests extends OpenSearchTestCase {

    private final NodeClient client = mock(NodeClient.class);
    private final Task task = mock(Task.class);
    private final ActionListener<ActionResponse> listener = mock(ActionListener.class);
    private final ActionFilterChain<ActionRequest, ActionResponse> chain = mock(ActionFilterChain.class);
    private final ActionRequestMetadata<ActionRequest, ActionResponse> metadata = mock(ActionRequestMetadata.class);
    private final SearchActionFilter filter = new SearchActionFilter(client);

    public void testOrderIsMinValue() {
        assertEquals(Integer.MIN_VALUE, filter.order());
    }

    public void testPassesThroughNonSearchAction() {
        BulkRequest request = new BulkRequest();

        filter.apply(task, BulkAction.NAME, request, metadata, listener, chain);

        verify(chain).proceed(task, BulkAction.NAME, request, listener);
        verify(client, never()).execute(any(), any(), any());
    }

    // TODO: add tests to verify reroute only happens when the target index has the setting enabled

    public void testReroutesSearchAction() {
        SearchRequest request = new SearchRequest("test-index");

        filter.apply(task, SearchAction.NAME, request, metadata, listener, chain);

        verify(client).execute(eq(DslExecuteAction.INSTANCE), eq(request), any());
        verify(chain, never()).proceed(any(), any(), any(), any());
    }
}
