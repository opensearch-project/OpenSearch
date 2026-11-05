/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.arrow;

import java.util.HashMap;
import java.util.Map;
import org.opensearch.OpenSearchParseException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.test.OpenSearchTestCase;

public class ArrowIngestionConfigTests extends OpenSearchTestCase {

    private static IndexMetadata indexMetadata(int numShards) {
        return IndexMetadata.builder("test-index")
                .settings(settings(Version.CURRENT))
                .numberOfShards(numShards)
                .numberOfReplicas(0)
                .build();
    }

    private static Map<String, Object> baseParams() {
        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "arrow_ipc");
        params.put(ArrowIngestionConfig.SHARDING_KEY_PROP_KEY, "id");
        return params;
    }

    public void testParsesRequiredFieldsAndDerivesNumShards() {
        ArrowIngestionConfig config = new ArrowIngestionConfig(baseParams(), indexMetadata(5));

        assertEquals("ARROW_IPC", config.getSourceType());
        assertEquals("id", config.getShardingKey());
        assertEquals(5, config.getNumShards());
        assertNull(config.getRowBound());
        assertEquals("", config.getIdColumn());
    }

    public void testMissingDatasetTypeThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIngestionConfig.SHARDING_KEY_PROP_KEY, "id");
        expectThrows(OpenSearchParseException.class, () -> new ArrowIngestionConfig(params, indexMetadata(1)));
    }

    public void testMissingShardingKeyThrows() {
        Map<String, Object> params = new HashMap<>();
        params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "arrow_ipc");
        expectThrows(OpenSearchParseException.class, () -> new ArrowIngestionConfig(params, indexMetadata(1)));
    }

    public void testRowBoundParsedWhenPresent() {
        Map<String, Object> params = baseParams();
        params.put(ArrowIngestionConfig.ROW_BOUND, "100");
        ArrowIngestionConfig config = new ArrowIngestionConfig(params, indexMetadata(1));
        assertEquals(Long.valueOf(100L), config.getRowBound());
    }

    public void testRowBoundBlankMeansUnbounded() {
        Map<String, Object> params = baseParams();
        params.put(ArrowIngestionConfig.ROW_BOUND, "  ");
        ArrowIngestionConfig config = new ArrowIngestionConfig(params, indexMetadata(1));
        assertNull(config.getRowBound());
    }

    public void testRowBoundNonNumericThrows() {
        Map<String, Object> params = baseParams();
        params.put(ArrowIngestionConfig.ROW_BOUND, "not-a-number");
        expectThrows(IllegalArgumentException.class, () -> new ArrowIngestionConfig(params, indexMetadata(1)));
    }

    public void testRowBoundZeroOrNegativeThrows() {
        Map<String, Object> zero = baseParams();
        zero.put(ArrowIngestionConfig.ROW_BOUND, "0");
        expectThrows(IllegalArgumentException.class, () -> new ArrowIngestionConfig(zero, indexMetadata(1)));

        Map<String, Object> negative = baseParams();
        negative.put(ArrowIngestionConfig.ROW_BOUND, "-5");
        expectThrows(IllegalArgumentException.class, () -> new ArrowIngestionConfig(negative, indexMetadata(1)));
    }

    public void testIdColumnTrimmed() {
        Map<String, Object> params = baseParams();
        params.put(ArrowIngestionConfig.ID_COLUMN, "  my_id  ");
        ArrowIngestionConfig config = new ArrowIngestionConfig(params, indexMetadata(1));
        assertEquals("my_id", config.getIdColumn());
    }

    public void testSourceTypeUpperCased() {
        Map<String, Object> params = baseParams();
        params.put(ArrowIngestionConfig.DATASET_TYPE_PROP_KEY, "MiXeD_CaSe");
        ArrowIngestionConfig config = new ArrowIngestionConfig(params, indexMetadata(1));
        assertEquals("MIXED_CASE", config.getSourceType());
    }
}
