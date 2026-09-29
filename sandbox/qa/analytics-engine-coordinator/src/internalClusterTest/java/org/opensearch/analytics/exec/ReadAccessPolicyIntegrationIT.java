/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.exec;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.ReadAccessContext;
import org.opensearch.action.support.ReadAccessPolicy;
import org.opensearch.action.support.ReadAccessPolicyProvider;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.AccessPolicyProviderPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.ppl.TestPPLPlugin;
import org.opensearch.ppl.action.PPLRequest;
import org.opensearch.ppl.action.PPLResponse;
import org.opensearch.ppl.action.UnifiedPPLExecuteAction;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.Set;

/**
 * Verifies that query execution obtains any ReadAccessPolicy provided by the cluster and properly applies it.
 * In a full cluster, the ReadAccessPolicy is provided by the security plugin. In this case, this is provided by a
 * mock plugin.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class ReadAccessPolicyIntegrationIT extends OpenSearchIntegTestCase {

    private static final String RESTRICTED_INDEX = "analytics-policy-restricted";
    private static final String UNRESTRICTED_INDEX = "analytics-policy-unrestricted";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class,
            TestPPLPlugin.class,
            TestAccessPolicyProviderPlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        if (indexExists(RESTRICTED_INDEX) == false) {
            createAnalyticsIndex(RESTRICTED_INDEX);
            client().prepareIndex(RESTRICTED_INDEX).setSource("tenant", "blue", "category", "blue-visible").get();
            client().prepareIndex(RESTRICTED_INDEX).setSource("tenant", "red", "category", "red-hidden").get();
            refresh(RESTRICTED_INDEX);
            client().admin().indices().prepareFlush(RESTRICTED_INDEX).get();
        }
        if (indexExists(UNRESTRICTED_INDEX) == false) {
            createAnalyticsIndex(UNRESTRICTED_INDEX);
            client().prepareIndex(UNRESTRICTED_INDEX).setSource("tenant", "blue", "category", "blue-visible").get();
            client().prepareIndex(UNRESTRICTED_INDEX).setSource("tenant", "red", "category", "red-visible").get();
            refresh(UNRESTRICTED_INDEX);
            client().admin().indices().prepareFlush(UNRESTRICTED_INDEX).get();
        }
    }

    public void testPolicyRestrictsAnalyticsResults() {
        PPLResponse response = executePpl("source = " + RESTRICTED_INDEX + " | fields category | sort category");

        assertEquals(1, response.getRows().size());
        assertEquals("blue-visible", response.getRows().getFirst()[0]);
    }

    public void testPolicyLeavesUnrestrictedIndexUnchanged() {
        PPLResponse response = executePpl("source = " + UNRESTRICTED_INDEX + " | fields category | sort category");

        assertEquals(2, response.getRows().size());
        assertEquals("blue-visible", response.getRows().get(0)[0]);
        assertEquals("red-visible", response.getRows().get(1)[0]);
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    private void createAnalyticsIndex(String indexName) {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats")
            .build();

        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(settings)
            .setMapping(
                "tenant",
                "type=keyword,low_cardinality=true",
                "category",
                "type=keyword,low_cardinality=true"
            )
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    private PPLResponse executePpl(String query) {
        return client().execute(UnifiedPPLExecuteAction.INSTANCE, new PPLRequest(query)).actionGet();
    }

    public static class TestAccessPolicyProviderPlugin extends Plugin implements AccessPolicyProviderPlugin {
        @Override
        public ReadAccessPolicyProvider getReadAccessPolicyProvider() {
            return new TestReadAccessPolicyProvider();
        }
    }

    private static class TestReadAccessPolicyProvider implements ReadAccessPolicyProvider {
        @Override
        public ReadAccessPolicy getReadAccessPolicy(ReadAccessContext context) {
            if (context.concreteIndices().contains(RESTRICTED_INDEX)) {
                return new TestReadAccessPolicy(
                    new TestIndexGroup(Set.of(RESTRICTED_INDEX), Optional.of(QueryBuilders.termQuery("tenant", "blue")))
                );
            }
            return ReadAccessPolicy.unrestricted();
        }
    }

    private static class TestReadAccessPolicy implements ReadAccessPolicy {
        private final List<IndexGroup> indexGroups;

        TestReadAccessPolicy(IndexGroup indexGroup) {
            this.indexGroups = List.of(indexGroup);
        }

        @Override
        public boolean hasRestrictions() {
            return true;
        }

        @Override
        public Set<String> coveredConcreteIndices() {
            return indexGroups.getFirst().concreteIndices();
        }

        @Override
        public Optional<QueryBuilder> restrictionsForIndex(String concreteIndex) {
            for (IndexGroup indexGroup : indexGroups) {
                if (indexGroup.concreteIndices().contains(concreteIndex)) {
                    return indexGroup.restrictions();
                }
            }
            return Optional.empty();
        }

        @Override
        public Collection<IndexGroup> indexGroups() {
            return indexGroups;
        }
    }

    private static class TestIndexGroup implements ReadAccessPolicy.IndexGroup {
        private final Set<String> concreteIndices;
        private final Optional<QueryBuilder> restrictions;

        TestIndexGroup(Set<String> concreteIndices, Optional<QueryBuilder> restrictions) {
            this.concreteIndices = Set.copyOf(concreteIndices);
            this.restrictions = restrictions;
        }

        @Override
        public Set<String> concreteIndices() {
            return concreteIndices;
        }

        @Override
        public Optional<QueryBuilder> restrictions() {
            return restrictions;
        }
    }
}
