/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.backpressure.stats;

import org.opensearch.common.io.stream.Writeable;
import org.opensearch.search.backpressure.settings.SearchBackpressureMode;
import org.opensearch.test.AbstractWireSerializingTestCase;

public class SearchBackpressureStatsTests extends AbstractWireSerializingTestCase<SearchBackpressureStats> {
    @Override
    protected Writeable.Reader<SearchBackpressureStats> instanceReader() {
        return SearchBackpressureStats::new;
    }

    @Override
    protected SearchBackpressureStats createTestInstance() {
        return randomInstance();
    }

    public static SearchBackpressureStats randomInstance() {
        return new SearchBackpressureStats(
            SearchTaskStatsTests.randomInstance(),
            SearchShardTaskStatsTests.randomInstance(),
            randomFrom(SearchBackpressureMode.DISABLED, SearchBackpressureMode.MONITOR_ONLY, SearchBackpressureMode.ENFORCED)
        );
    }
}
