/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.nested;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.ObjectMapper;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.query.support.NestedScope;
import org.opensearch.search.aggregations.AggregationBuilder;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class NestedAggregationDepthTests extends OpenSearchTestCase {
    public void testNestedDepthExceeded() throws Exception {
        assertDepthLimit(new NestedAggregationBuilder("too_deep", "a"));
    }

    public void testReverseNestedDepthExceeded() throws Exception {
        assertDepthLimit(new ReverseNestedAggregationBuilder("too_deep"));
    }

    private void assertDepthLimit(AggregationBuilder deepest) throws Exception {
        var settings = IndexSettingsModule.newIndexSettings(
            "test",
            Settings.builder().put(IndexSettings.MAX_NESTED_QUERY_DEPTH_SETTING.getKey(), 3).build()
        );
        var scope = new NestedScope(settings);
        var mapper = mock(ObjectMapper.class);
        when(mapper.nested()).thenReturn(ObjectMapper.Nested.newNested());
        var context = mock(QueryShardContext.class);
        when(context.getUsageService()).thenReturn(mock(AggregationUsageService.class));
        when(context.nestedScope()).thenReturn(scope);
        when(context.getObjectMapper("a")).thenReturn(mapper);

        // Preserve an enclosing scope while building and unwinding sub-aggregations.
        scope.nextLevel(mapper);
        var atLimit = new NestedAggregationBuilder("nested", "a").subAggregation(new ReverseNestedAggregationBuilder("reverse"));
        assertNotNull(atLimit.build(context, null));
        assertSame(mapper, scope.getObjectMapper());

        var tooDeep = new NestedAggregationBuilder("nested", "a").subAggregation(
            new ReverseNestedAggregationBuilder("reverse").subAggregation(deepest)
        );
        var exception = expectThrows(IllegalArgumentException.class, () -> tooDeep.build(context, null));
        assertThat(exception.getMessage(), containsString("depth of Nested Query is [4]"));
        assertThat(exception.getMessage(), containsString("allowed maximum of [3]"));
        assertThat(exception.getMessage(), containsString("index.query.max_nested_depth"));
        assertSame(mapper, scope.getObjectMapper());
        assertSame(mapper, scope.previousLevel());
        assertNull(scope.getObjectMapper());
    }
}
