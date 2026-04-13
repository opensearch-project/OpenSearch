/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.result;

import org.junit.Test;
import org.opensearch.dsl.aggregation.AggregationRegistry;

import java.util.List;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class AggregationResponseBuilderTests {

    @Test
    public void testConstructorWithEmptyResults() {
        AggregationRegistry registry = mock(AggregationRegistry.class);
        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());
        assertNotNull(builder);
    }

    @Test
    public void testBuildEmptyAggregations() throws Exception {
        AggregationRegistry registry = mock(AggregationRegistry.class);
        AggregationResponseBuilder builder = new AggregationResponseBuilder(registry, List.of());

        var aggs = builder.build(List.of());
        assertNotNull(aggs);
        assertEquals(0, aggs.asList().size());
    }
}
