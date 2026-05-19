/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Set;

/**
 * Runs all promotion tests from {@link DataFormatAwareReplicationPromotionIT} with Lucene as
 * a secondary data format alongside Parquet. Validates that real Lucene segment files survive
 * the replication and promotion paths.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class DataFormatAwareReplicationPromotionWithLuceneIT extends DataFormatAwareReplicationPromotionIT {

    @Override
    protected Settings dfaIndexSettings(int replicaCount) {
        // Override to add lucene as a secondary data format.
        return Settings.builder()
            .put(super.dfaIndexSettings(replicaCount))
            .putList("index.composite.secondary_data_formats", java.util.List.of("lucene"))
            .build();
    }

    @Override
    protected boolean hasLuceneSecondary() {
        return true;
    }

    @Override
    protected Set<String> expectedFormats() {
        return Set.of("parquet", "lucene");
    }
}
