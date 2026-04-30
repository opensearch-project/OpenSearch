/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

import static org.opensearch.composite.CompositeDataFormatPlugin.CLUSTER_INDEX_RESTRICT_COMPOSITE_DATAFORMAT_SETTING;

/**
 * Integration tests for {@link CompositeDataFormatPlugin#CLUSTER_INDEX_RESTRICT_COMPOSITE_DATAFORMAT_SETTING}
 * enforcement. The setting is {@code Property.Final}, so each test starts nodes with its own
 * settings bag.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class RestrictCompositeDataFormatOverrideIT extends OpenSearchIntegTestCase {

    private static final String INDEX_NAME = "test-composite-restrict";
    private static final String CLUSTER_DEFAULT_PRIMARY = "lucene";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(ParquetDataFormatPlugin.class, CompositeDataFormatPlugin.class, LucenePlugin.class, DataFusionPlugin.class);
    }

    private Settings nodeSettings(boolean restrict) {
        return Settings.builder()
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(CLUSTER_INDEX_RESTRICT_COMPOSITE_DATAFORMAT_SETTING.getKey(), restrict)
            .put(CompositeDataFormatPlugin.CLUSTER_DEFAULT_PRIMARY_DATA_FORMAT.getKey(), CLUSTER_DEFAULT_PRIMARY)
            .build();
    }

    public void testRejectsPrimaryOverrideWhenRestrictIsTrue() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(true));
        internalCluster().startDataOnlyNode(nodeSettings(true));

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .build();

        IllegalArgumentException thrown = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get()
        );
        String message = thrown.getMessage();
        assertTrue(
            "expected validation error to mention ["
                + CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey()
                + "] but was ["
                + message
                + "]",
            message.contains(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey())
        );
        assertTrue(
            "expected validation error to mention restrict setting but was [" + message + "]",
            message.contains(CLUSTER_INDEX_RESTRICT_COMPOSITE_DATAFORMAT_SETTING.getKey())
        );
    }

    public void testRejectsSecondaryOverrideWhenRestrictIsTrue() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(true));
        internalCluster().startDataOnlyNode(nodeSettings(true));

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .putList(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey(), "parquet")
            .build();

        IllegalArgumentException thrown = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get()
        );
        String message = thrown.getMessage();
        assertTrue(
            "expected validation error to mention ["
                + CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey()
                + "] but was ["
                + message
                + "]",
            message.contains(CompositeDataFormatPlugin.SECONDARY_DATA_FORMATS.getKey())
        );
    }

    public void testAcceptsMatchingOverrideWhenRestrictIsTrue() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(true));
        internalCluster().startDataOnlyNode(nodeSettings(true));

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), CLUSTER_DEFAULT_PRIMARY)
            .build();

        CreateIndexResponse response = client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
        assertTrue(response.isAcknowledged());
        ensureGreen(INDEX_NAME);
    }

    public void testAllowsOverrideWhenRestrictIsFalse() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(false));
        internalCluster().startDataOnlyNode(nodeSettings(false));

        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet")
            .build();

        CreateIndexResponse response = client().admin().indices().prepareCreate(INDEX_NAME).setSettings(indexSettings).get();
        assertTrue(response.isAcknowledged());
        ensureGreen(INDEX_NAME);
    }

    public void testRejectsTemplateOverrideWhenRestrictIsTrue() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(true));
        internalCluster().startDataOnlyNode(nodeSettings(true));

        AcknowledgedResponse putTemplate = client().admin()
            .indices()
            .preparePutTemplate("restrict-composite-template")
            .setPatterns(Collections.singletonList(INDEX_NAME + "*"))
            .setSettings(Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet"))
            .setOrder(0)
            .get();
        assertTrue(putTemplate.isAcknowledged());

        IllegalArgumentException thrown = expectThrows(IllegalArgumentException.class, () -> createIndex(INDEX_NAME));
        assertTrue(thrown.getMessage().contains(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey()));
    }

    public void testAllowsTemplateOverrideWhenRestrictIsFalse() {
        internalCluster().startClusterManagerOnlyNode(nodeSettings(false));
        internalCluster().startDataOnlyNode(nodeSettings(false));

        AcknowledgedResponse putTemplate = client().admin()
            .indices()
            .preparePutTemplate("permissive-composite-template")
            .setPatterns(Collections.singletonList(INDEX_NAME + "*"))
            .setSettings(Settings.builder().put(CompositeDataFormatPlugin.PRIMARY_DATA_FORMAT.getKey(), "parquet"))
            .setOrder(0)
            .get();
        assertTrue(putTemplate.isAcknowledged());

        createIndex(INDEX_NAME);
        ensureGreen(INDEX_NAME);
    }
}
