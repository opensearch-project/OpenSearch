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
 * Runs all recovery tests from {@link DataFormatAwareRemoteStoreRecoveryIT} with Lucene as
 * a secondary data format. Validates that Lucene segment files are correctly persisted,
 * restored, and survive node restart / shard relocation alongside Parquet files.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
@org.apache.lucene.tests.util.LuceneTestCase.SuppressTempFileChecks(bugUrl = "parquet native dir cleanup")
public class DataFormatAwareRemoteStoreRecoveryWithLuceneIT extends DataFormatAwareRemoteStoreRecoveryIT {

    @Override
    protected Settings dfaIndexSettings(int replicaCount) {
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
