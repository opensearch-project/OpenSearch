/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.rules.action.top_queries;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.plugin.insights.QueryInsightsTestUtils;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Tests for {@link TopQueries}.
 */
public class TopQueriesTests extends OpenSearchTestCase {

    public void testTopQueries() throws IOException {
        TopQueries topQueries = QueryInsightsTestUtils.createRandomTopQueries();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            topQueries.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                TopQueries readTopQueries = new TopQueries(in);
                assertTrue(
                    QueryInsightsTestUtils.checkRecordsEquals(topQueries.getTopQueriesRecord(), readTopQueries.getTopQueriesRecord())
                );
            }
        }
    }
}
