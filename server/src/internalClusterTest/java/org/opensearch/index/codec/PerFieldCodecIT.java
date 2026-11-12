/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.lucene104.Lucene104PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.tests.codecs.asserting.AssertingPostingsFormat;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.fuzzy.FuzzyFilterPostingsFormat;
import org.opensearch.index.codec.fuzzy.FuzzySetFactory;
import org.opensearch.index.codec.fuzzy.FuzzySetParameters;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.indices.IndicesService;
import org.opensearch.plugins.EnginePlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;

/**
 * Several plugins may contribute the formats for the fields they own to the same index, in contrast to
 * {@link CodecServiceFactory} which only one plugin per index could provide.
 */
@SuppressCodecs("*") // the per-field formats are asserted against the ones of the OpenSearch codecs
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class PerFieldCodecIT extends OpenSearchIntegTestCase {
    private static final String FIRST_FIELD = "first";
    private static final String SECOND_FIELD = "second";
    private static final String PLAIN_FIELD = "plain";
    /** Claimed by both {@link FirstEnginePlugin} and {@link ThirdEnginePlugin} */
    private static final String CONTESTED_FIELD = "contested";
    private static final String LEGACY_FACTORY_INDEX = "legacy-factory";

    /**
     * The plugin which owns the {@link #FIRST_FIELD} field only, encoded with its own postings format.
     */
    public static class FirstEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<PerFieldCodecBuilder> getPerFieldCodecBuilder(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(
                postingsFor(
                    Set.of(FIRST_FIELD, CONTESTED_FIELD),
                    new FuzzyFilterPostingsFormat(
                        new Lucene104PostingsFormat(),
                        new FuzzySetFactory(Map.of(FIRST_FIELD, new FuzzySetParameters(() -> 0.01)))
                    )
                )
            );
        }
    }

    /**
     * The plugin which owns the {@link #SECOND_FIELD} field only, encoded with a different postings format
     * than the one of {@link FirstEnginePlugin}.
     */
    public static class SecondEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<PerFieldCodecBuilder> getPerFieldCodecBuilder(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(postingsFor(Set.of(SECOND_FIELD), new AssertingPostingsFormat()));
        }
    }

    /**
     * The plugin which claims the {@link #CONTESTED_FIELD} field, which {@link FirstEnginePlugin} claims as well.
     */
    public static class ThirdEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<PerFieldCodecBuilder> getPerFieldCodecBuilder(MapperService mapperService, IndexSettings indexSettings) {
            return Optional.of(postingsFor(Set.of(CONTESTED_FIELD), new Lucene104PostingsFormat()));
        }
    }

    /**
     * A plugin which still supplies its own codec service the way the plugins do today (not applying the
     * contributions of the others), for the {@link #LEGACY_FACTORY_INDEX} index only.
     */
    public static class LegacyFactoryEnginePlugin extends Plugin implements EnginePlugin {
        @Override
        public Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
            if (LEGACY_FACTORY_INDEX.equals(indexSettings.getIndex().getName()) == false) {
                return Optional.empty();
            }

            return Optional.of(
                config -> new CodecService(
                    config.getMapperService(),
                    config.getIndexSettings(),
                    config.getLogger(),
                    config.getAdditionalCodecs()
                )
            );
        }
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        final List<Class<? extends Plugin>> plugins = new ArrayList<>(super.nodePlugins());
        plugins.add(FirstEnginePlugin.class);
        plugins.add(SecondEnginePlugin.class);
        plugins.add(ThirdEnginePlugin.class);
        plugins.add(LegacyFactoryEnginePlugin.class);
        return plugins;
    }

    public void testFieldsOfSeveralPluginsAreEncodedWithinSameIndex() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(FIRST_FIELD, "type=keyword", SECOND_FIELD, "type=keyword", PLAIN_FIELD, "type=keyword")
        );

        final Map<String, String> document = Map.of(FIRST_FIELD, "one", SECOND_FIELD, "two", PLAIN_FIELD, "three");
        client().prepareIndex("test").setId("1").setSource(document).get();
        flushAndRefresh("test");

        for (final Map.Entry<String, String> field : document.entrySet()) {
            final SearchResponse response = client().prepareSearch("test")
                .setQuery(QueryBuilders.termQuery(field.getKey(), field.getValue()))
                .get();
            assertHitCount(response, 1);
        }

        // every field is encoded with the format of the plugin which owns it, the fields no plugin owns are
        // encoded with the format of the codec the index is configured with
        assertPerFieldPostingsFormats(
            Map.of(
                FIRST_FIELD,
                FuzzyFilterPostingsFormat.FUZZY_FILTER_CODEC_NAME,
                SECOND_FIELD,
                new AssertingPostingsFormat().getName(),
                PLAIN_FIELD,
                new Lucene104PostingsFormat().getName()
            )
        );
    }

    public void testIndexWithFieldClaimedByTwoPluginsIsRejected() {
        final IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(CONTESTED_FIELD, "type=keyword")
                .get()
        );
        assertThat(ex.getMessage(), containsString("postings format for field [" + CONTESTED_FIELD + "]"));
        assertFalse(indexExists("test"));
    }

    /**
     * A conflicting claim is rejected with the mapping update which introduces it: the shard keeps
     * indexing, rather than failing at the next flush.
     */
    public void testMappingUpdateWithFieldClaimedByTwoPluginsIsRejected() throws Exception {
        assertAcked(
            prepareCreate("test").setSettings(Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0))
                .setMapping(FIRST_FIELD, "type=keyword")
        );
        ensureGreen("test");

        final IllegalArgumentException explicit = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping("test").setSource(CONTESTED_FIELD, "type=keyword").get()
        );
        assertThat(explicit.getMessage(), containsString("postings format for field [" + CONTESTED_FIELD + "]"));

        // the dynamic mapping update is rejected the same way, failing the document only
        final Exception dynamic = expectThrows(
            Exception.class,
            () -> client().prepareIndex("test").setId("1").setSource(Map.of(CONTESTED_FIELD, "value")).get()
        );
        assertThat(dynamic.getMessage(), containsString("postings format for field [" + CONTESTED_FIELD + "]"));

        final Map<String, Object> mapping = client().admin()
            .indices()
            .prepareGetMappings("test")
            .get()
            .mappings()
            .get("test")
            .sourceAsMap();
        assertThat(mapping.toString(), not(containsString(CONTESTED_FIELD)));

        client().prepareIndex("test").setId("2").setSource(Map.of(FIRST_FIELD, "one")).get();
        flushAndRefresh("test");
        ensureGreen("test");
        assertHitCount(client().prepareSearch("test").setQuery(QueryBuilders.termQuery(FIRST_FIELD, "one")).get(), 1);
    }

    /**
     * A plugin which has not migrated yet and would drop the formats of the others fails the index creation,
     * rather than its shards or, worse, encoding the fields of the other plugins with the default formats.
     */
    public void testIndexWithCodecServiceFactoryDroppingContributionsIsRejected() {
        final IllegalStateException ex = expectThrows(
            IllegalStateException.class,
            () -> prepareCreate(LEGACY_FACTORY_INDEX).setSettings(
                Settings.builder().put("index.number_of_shards", 1).put("index.number_of_replicas", 0)
            ).setMapping(FIRST_FIELD, "type=keyword").get()
        );
        assertThat(ex.getMessage(), containsString(LegacyFactoryEnginePlugin.class.getName()));
        assertThat(ex.getMessage(), containsString("CodecService(CodecServiceConfig)"));
        assertFalse(indexExists(LEGACY_FACTORY_INDEX));
    }

    private void assertPerFieldPostingsFormats(Map<String, String> expected) throws Exception {
        int shards = 0;

        for (final IndicesService indicesService : internalCluster().getInstances(IndicesService.class)) {
            final IndexService indexService = indicesService.indexService(resolveIndex("test"));
            if (indexService == null) {
                continue;
            }

            for (final IndexShard shard : indexService) {
                try (Engine.Searcher searcher = shard.acquireSearcher("test")) {
                    for (final LeafReaderContext leaf : searcher.getIndexReader().leaves()) {
                        final FieldInfos fieldInfos = leaf.reader().getFieldInfos();
                        for (final Map.Entry<String, String> field : expected.entrySet()) {
                            assertEquals(
                                "postings format of field [" + field.getKey() + "]",
                                field.getValue(),
                                fieldInfos.fieldInfo(field.getKey()).getAttribute(PerFieldPostingsFormat.PER_FIELD_FORMAT_KEY)
                            );
                        }
                    }
                }

                ++shards;
            }
        }

        assertEquals("no shards of the index were found", 1, shards);
    }

    private static PerFieldCodecBuilder postingsFor(Set<String> fields, PostingsFormat postingsFormat) {
        final PerFieldPostingsFormat perField = new PerFieldPostingsFormat() {
            @Override
            public PostingsFormat getPostingsFormatForField(String name) {
                return fields.contains(name) ? postingsFormat : null;
            }
        };

        return new PerFieldCodecBuilder() {
            @Override
            public Optional<PerFieldPostingsFormat> postingsFormat() {
                return Optional.of(perField);
            }
        };
    }
}
