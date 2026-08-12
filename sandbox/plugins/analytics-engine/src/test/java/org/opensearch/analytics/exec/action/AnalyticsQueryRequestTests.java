/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec.action;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.QueryRequestContext;
import org.opensearch.cluster.ClusterState;
import org.opensearch.test.OpenSearchTestCase;

import static org.mockito.Mockito.mock;

/**
 * Tests for {@link AnalyticsQueryRequest}, focusing on IndicesOptions delegation.
 */
public class AnalyticsQueryRequestTests extends OpenSearchTestCase {

    /**
     * When a QueryRequestContext with custom IndicesOptions is present, indicesOptions()
     * must return those options — not the hardcoded strictExpandOpen() default.
     */
    public void testIndicesOptionsReflectsQueryRequestContext() {
        IndicesOptions customOptions = IndicesOptions.fromOptions(true, true, true, true);
        ClusterState state = mock(ClusterState.class);
        QueryRequestContext ctx = new QueryRequestContext(state, null, null, null, customOptions);

        AnalyticsQueryRequest request = new AnalyticsQueryRequest(null, ctx, new String[] { "test-index" });

        assertSame("indicesOptions() must return the QueryRequestContext's options when present", customOptions, request.indicesOptions());
    }

    /**
     * When QueryRequestContext is null (deserialized case), indicesOptions() must fall back
     * to strictExpandOpen() for backward compatibility.
     */
    public void testIndicesOptionsFallsBackToStrictExpandOpenWhenContextNull() {
        AnalyticsQueryRequest request = new AnalyticsQueryRequest(null, null, new String[] { "test-index" });

        assertEquals(
            "indicesOptions() must fall back to strictExpandOpen() when queryCtx is null",
            IndicesOptions.strictExpandOpen(),
            request.indicesOptions()
        );
    }

    /**
     * When QueryRequestContext has null indicesOptions, fall back to strictExpandOpen().
     */
    public void testIndicesOptionsFallsBackWhenContextOptionsNull() {
        ClusterState state = mock(ClusterState.class);
        QueryRequestContext ctx = new QueryRequestContext(state, null, null, null, null);

        AnalyticsQueryRequest request = new AnalyticsQueryRequest(null, ctx, new String[] { "test-index" });

        assertEquals(
            "indicesOptions() must fall back to strictExpandOpen() when context's options are null",
            IndicesOptions.strictExpandOpen(),
            request.indicesOptions()
        );
    }
}
