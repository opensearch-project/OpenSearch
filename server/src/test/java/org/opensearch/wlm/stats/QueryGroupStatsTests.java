/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.ResourceType;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.HashMap;
import java.util.Map;

public class QueryGroupStatsTests extends AbstractWireSerializingTestCase<QueryGroupStats> {

    @Override
    protected Writeable.Reader<QueryGroupStats> instanceReader() {
        return QueryGroupStats::new;
    }

    @Override
    protected QueryGroupStats createTestInstance() {
        Map<String, QueryGroupStats.QueryGroupStatsHolder> stats = new HashMap<>();
        stats.put(
            randomAlphaOfLength(10),
            new QueryGroupStats.QueryGroupStatsHolder(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(
                    ResourceType.CPU,
                    new QueryGroupStats.ResourceStats(
                        randomDoubleBetween(0.0, 0.90, false),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                )
            )
        );
        return new QueryGroupStats(stats);
    }
}
