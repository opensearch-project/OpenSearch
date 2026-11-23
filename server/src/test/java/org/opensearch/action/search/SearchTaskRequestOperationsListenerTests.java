/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.tasks.TaskResourceTrackingService;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Collections;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

public class SearchTaskRequestOperationsListenerTests extends OpenSearchTestCase {

    public void testRequestWithoutSearchIsIgnored() {
        TaskResourceTrackingService taskResourceTrackingService = mock(TaskResourceTrackingService.class);
        SearchTaskRequestOperationsListener listener = new SearchTaskRequestOperationsListener(taskResourceTrackingService);
        SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);

        listener.onRequestEnd(searchRequestContext);
        listener.onRequestFailure(searchRequestContext);

        verifyNoInteractions(taskResourceTrackingService);
    }

    public void testRequestEndRefreshesTheTaskOfItsSearch() {
        TaskResourceTrackingService taskResourceTrackingService = mock(TaskResourceTrackingService.class);
        SearchTaskRequestOperationsListener listener = new SearchTaskRequestOperationsListener(taskResourceTrackingService);
        SearchTask task = new SearchTask(0, "n/a", "n/a", () -> "test", null, Collections.emptyMap());
        SearchPhaseContext phaseContext = mock(SearchPhaseContext.class);
        when(phaseContext.getTask()).thenReturn(task);
        SearchRequestContext searchRequestContext = mock(SearchRequestContext.class);
        when(searchRequestContext.getPhaseContext()).thenReturn(phaseContext);

        listener.onRequestEnd(searchRequestContext);
        verify(taskResourceTrackingService).refreshResourceStats(task);
        verify(taskResourceTrackingService).removeTaskResourceUsage();

        listener.onRequestFailure(searchRequestContext);
        verify(taskResourceTrackingService, times(2)).removeTaskResourceUsage();
    }
}
