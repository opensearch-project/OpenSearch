/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to this file be licensed
 * under the Apache-2.0 license or a compatible open source license.
 */

package org.opensearch.dsl.aggregation.metric;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.dsl.DslQueryExecutorPlugin;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

public class InternalAnalyticsCardinalityTests extends OpenSearchTestCase {

    public void testNamedWriteableRoundTrip() throws Exception {
        InternalAnalyticsCardinality original = new InternalAnalyticsCardinality("unique_services", 42L, Map.of("owner", "observability"));

        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.writeNamedWriteable(original);
            NamedWriteableRegistry registry = new NamedWriteableRegistry(new DslQueryExecutorPlugin().getNamedWriteables());
            try (StreamInput in = new NamedWriteableAwareStreamInput(out.bytes().streamInput(), registry)) {
                InternalAggregation copy = in.readNamedWriteable(InternalAggregation.class);
                assertEquals(original, copy);
                assertEquals(42L, ((InternalAnalyticsCardinality) copy).getValue());
                assertEquals(original.getMetadata(), copy.getMetadata());
            }
        }
    }

    public void testOnlySingleAlreadyReducedValueCanBeReduced() {
        InternalAnalyticsCardinality cardinality = new InternalAnalyticsCardinality("unique_services", 3L, null);

        assertSame(cardinality, cardinality.reduce(List.of(cardinality), null));
        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> cardinality.reduce(List.of(cardinality, cardinality), null)
        );
        assertTrue(exception.getMessage().contains("already globally reduced"));
    }
}
