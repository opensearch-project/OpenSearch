/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.collect.MapBuilder;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.search.backpressure.trackers.CpuUsageTracker;
import org.opensearch.search.backpressure.trackers.ElapsedTimeTracker;
import org.opensearch.search.backpressure.trackers.HeapUsageTracker;
import org.opensearch.search.backpressure.trackers.ResourceUsageTracker;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class SearchBackpressureStatsTests extends AbstractWireSerializingTestCase<SearchBackpressureStats> {
    @Override
    protected Writeable.Reader<SearchBackpressureStats> instanceReader() {
        return SearchBackpressureStats::new;
    }

    @Override
    protected SearchBackpressureStats createTestInstance() {
        return new SearchBackpressureStats(
            new MapBuilder<String, ResourceUsageTracker.Stats>().put(
                CpuUsageTracker.NAME,
                new CpuUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong())
            )
                .put(
                    HeapUsageTracker.NAME,
                    new HeapUsageTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
                )
                .put(ElapsedTimeTracker.NAME, new ElapsedTimeTracker.Stats(randomNonNegativeLong(), randomNonNegativeLong()))
                .immutableMap(),
            CancellationStatsTests.randomInstance(),
            randomBoolean(),
            randomBoolean()
        );
    }
}
