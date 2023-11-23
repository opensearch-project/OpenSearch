/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class SearchTimeProviderTests extends OpenSearchTestCase {

    public void testSearchTimeProviderPhaseFailure() {
        TransportSearchAction.SearchTimeProvider testTimeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);
        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase mockSearchPhase = mock(SearchPhase.class);
        when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            testTimeProvider.onPhaseStart(ctx);
            assertNull(testTimeProvider.getPhaseTookTime(searchPhaseName));
            testTimeProvider.onPhaseFailure(ctx);
            assertNull(testTimeProvider.getPhaseTookTime(searchPhaseName));
        }
    }

    public void testSearchTimeProviderPhaseEnd() {
        TransportSearchAction.SearchTimeProvider testTimeProvider = new TransportSearchAction.SearchTimeProvider(0, 0, () -> 0);

        SearchPhaseContext ctx = mock(SearchPhaseContext.class);
        SearchPhase mockSearchPhase = mock(SearchPhase.class);
        when(ctx.getCurrentPhase()).thenReturn(mockSearchPhase);

        for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
            when(mockSearchPhase.getSearchPhaseName()).thenReturn(searchPhaseName);
            long tookTimeInMillis = randomIntBetween(1, 100);
            testTimeProvider.onPhaseStart(ctx);
            long startTime = System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(tookTimeInMillis);
            when(mockSearchPhase.getStartTimeInNanos()).thenReturn(startTime);
            assertNull(testTimeProvider.getPhaseTookTime(searchPhaseName));
            testTimeProvider.onPhaseEnd(ctx, new SearchRequestContext());
            assertThat(testTimeProvider.getPhaseTookTime(searchPhaseName), greaterThanOrEqualTo(tookTimeInMillis));
        }
    }
}
