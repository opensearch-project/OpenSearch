/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.search.aggregations.metrics.CardinalityWithRequestBreakerIT;
import org.opensearch.test.OpenSearchIntegTestCase;

/*
* Creating a separate class for Concurrent Search use-case as parameterization doesn't work with
* Circuit Breaker causing flaky tests. ref: https://github.com/opensearch-project/OpenSearch/issues/10154
* */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST)
public class ConcurrentSearchCardinalityWithRequestBreakerIT extends CardinalityWithRequestBreakerIT {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.CONCURRENT_SEGMENT_SEARCH, "true").build();
    }
}
