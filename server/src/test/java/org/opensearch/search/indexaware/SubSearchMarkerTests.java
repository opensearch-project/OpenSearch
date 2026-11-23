/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.indexaware;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.test.OpenSearchTestCase;

public class SubSearchMarkerTests extends OpenSearchTestCase {

    public void testNotMarkedByDefault() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        assertFalse(SubSearchMarker.isMarked(threadContext));
    }

    public void testMarkedWhileTheReturnedContextIsOpen() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
            assertTrue(SubSearchMarker.isMarked(threadContext));
        }
    }

    public void testNotMarkedOnceTheReturnedContextIsClosed() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        SubSearchMarker.markForSubSearch(threadContext).close();

        assertFalse(SubSearchMarker.isMarked(threadContext));
    }

    // The three tests below cover what stashing the context instead would lose: the caller's transients,
    // the caller's request headers (which carry its identity), and response headers the sub-search adds.

    public void testMarkingKeepsTheCallersOtherTransients() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient("unrelated", "value");

        try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
            assertEquals("value", threadContext.<String>getTransient("unrelated"));
        }

        assertEquals("value", threadContext.<String>getTransient("unrelated"));
    }

    public void testMarkingKeepsTheCallersRequestHeaders() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putHeader("user", "alice");

        try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
            assertEquals("alice", threadContext.getHeader("user"));
        }

        assertEquals("alice", threadContext.getHeader("user"));
    }

    public void testRestorePreservesResponseHeadersAddedDuringTheSubSearch() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
            threadContext.addResponseHeader("sub-search-header", "value");
        }

        assertEquals("value", threadContext.getResponseHeaders().get("sub-search-header").get(0));
    }

    public void testNestedMarkingStaysMarkedUntilTheOutermostScopeCloses() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);

        try (ThreadContext.StoredContext outer = SubSearchMarker.markForSubSearch(threadContext)) {
            assertTrue(SubSearchMarker.isMarked(threadContext));
            try (ThreadContext.StoredContext inner = SubSearchMarker.markForSubSearch(threadContext)) {
                assertTrue(SubSearchMarker.isMarked(threadContext));
            }
            // Closing the inner scope must not remove the outer scope's marker.
            assertTrue(SubSearchMarker.isMarked(threadContext));
        }
        assertFalse(SubSearchMarker.isMarked(threadContext));
    }

    // The key is public, so other code could have put some other value under it. The two tests below
    // cover that: such a context does not count as marked, and marking it does not throw.

    public void testAKeyHoldingAnotherValueIsNotMarked() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(SubSearchMarker.SUB_SEARCH_TRANSIENT_NAME, "not the marker");

        assertFalse(SubSearchMarker.isMarked(threadContext));
    }

    public void testMarkingAContextWhoseKeyHoldsAnotherValueLeavesThatValueInPlace() {
        ThreadContext threadContext = new ThreadContext(Settings.EMPTY);
        threadContext.putTransient(SubSearchMarker.SUB_SEARCH_TRANSIENT_NAME, "not the marker");

        // A transient cannot be overwritten, so the marker cannot be set. The call must still not throw.
        try (ThreadContext.StoredContext ignored = SubSearchMarker.markForSubSearch(threadContext)) {
            assertEquals("not the marker", threadContext.<String>getTransient(SubSearchMarker.SUB_SEARCH_TRANSIENT_NAME));
        }
    }
}
