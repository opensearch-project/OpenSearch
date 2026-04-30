/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.metadata;

import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;

import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_PLUGGABLE_DATAFORMAT_ENABLED_SETTING;
import static org.opensearch.indices.IndicesService.CLUSTER_DEFAULT_PLUGGABLE_DATAFORMAT_VALUE_SETTING;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ClusterDefaultPluggableDataFormatIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(MockCommitterEnginePlugin.class, MockParquetDataFormatPlugin.class);
    }

    @Override
    protected Settings featureFlagSettings() {
        Settings.Builder builder = Settings.builder();
        for (Setting<?> builtInFlag : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            builder.put(builtInFlag.getKey(), builtInFlag.getDefaultRaw(Settings.EMPTY));
        }
        builder.put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true);
        return builder.build();
    }

    public void testClusterDefaultStampedIntoNewIndexWhenNoOverride() {
        String indexName = "test-pluggable-cluster-default";

        setClusterDefaults(true, "parquet");
        createIndex(indexName);
        ensureGreen(indexName);

        Settings effective = getIndexSettings(indexName);
        assertTrue(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(effective));
        assertEquals("parquet", IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(effective));
    }

    public void testExplicitIndexSettingOverridesClusterDefault() {
        String indexName = "test-pluggable-request-override";

        setClusterDefaults(true, "parquet");
        createIndex(
            indexName,
            Settings.builder()
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), false)
                .put(IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), "lucene")
                .build()
        );
        ensureGreen(indexName);

        Settings effective = getIndexSettings(indexName);
        assertFalse(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(effective));
        assertEquals("lucene", IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(effective));
    }

    public void testClusterDefaultUpdateAppliesToNewIndicesOnly() {
        String indexBefore = "test-pluggable-before-update";
        String indexAfter = "test-pluggable-after-update";

        setClusterDefaults(true, "parquet");
        createIndex(indexBefore);
        ensureGreen(indexBefore);

        Settings before = getIndexSettings(indexBefore);
        assertTrue(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(before));
        assertEquals("parquet", IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(before));

        setClusterDefaults(false, "arrow");
        createIndex(indexAfter);
        ensureGreen(indexAfter);

        Settings after = getIndexSettings(indexAfter);
        assertFalse(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(after));
        assertEquals("arrow", IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(after));

        Settings beforeReread = getIndexSettings(indexBefore);
        assertTrue(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.get(beforeReread));
        assertEquals("parquet", IndexSettings.PLUGGABLE_DATAFORMAT_VALUE_SETTING.get(beforeReread));
    }

    private void setClusterDefaults(boolean enabled, String value) {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(
                Settings.builder()
                    .put(CLUSTER_DEFAULT_PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), enabled)
                    .put(CLUSTER_DEFAULT_PLUGGABLE_DATAFORMAT_VALUE_SETTING.getKey(), value)
            )
            .get();
    }

    private Settings getIndexSettings(String indexName) {
        GetSettingsResponse resp = client().admin().indices().prepareGetSettings(indexName).get();
        Settings s = resp.getIndexToSettings().get(indexName);
        assertNotNull(s);
        return s;
    }
}
