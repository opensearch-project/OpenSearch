/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.test;

import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.SeedUtils;

import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.util.Accountable;
import org.opensearch.Version;
import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.indices.mapping.put.PutMappingRequest;
import org.opensearch.action.get.GetRequest;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.support.PlainActionFuture;
import org.opensearch.action.termvectors.MultiTermVectorsRequest;
import org.opensearch.action.termvectors.MultiTermVectorsResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Strings;
import org.opensearch.common.compress.CompressedXContent;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsModule;
import org.opensearch.common.util.BigArrays;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.Index;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.cache.bitset.BitsetFilterCache;
import org.opensearch.index.fielddata.IndexFieldDataCache;
import org.opensearch.index.fielddata.IndexFieldDataService;
import org.opensearch.index.mapper.MappedFieldType;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.index.shard.ShardId;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.IndicesModule;
import org.opensearch.indices.analysis.AnalysisModule;
import org.opensearch.indices.breaker.NoneCircuitBreakerService;
import org.opensearch.indices.fielddata.cache.IndicesFieldDataCache;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.plugins.ScriptPlugin;
import org.opensearch.plugins.SearchPlugin;
import org.opensearch.script.MockScriptEngine;
import org.opensearch.script.MockScriptService;
import org.opensearch.script.ScriptContext;
import org.opensearch.script.ScriptEngine;
import org.opensearch.script.ScriptModule;
import org.opensearch.script.ScriptService;
import org.opensearch.search.SearchModule;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;

import java.io.Closeable;
import java.io.IOException;
import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;
import java.lang.reflect.Proxy;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static java.util.stream.Collectors.toList;

public abstract class AbstractBuilderTestCase extends OpenSearchTestCase {

    public static final String TEXT_FIELD_NAME = "mapped_string";
    public static final String TEXT_ALIAS_FIELD_NAME = "mapped_string_alias";
    protected static final String KEYWORD_FIELD_NAME = "mapped_string_2";
    protected static final String INT_FIELD_NAME = "mapped_int";
    protected static final String INT_ALIAS_FIELD_NAME = "mapped_int_field_alias";
    protected static final String INT_RANGE_FIELD_NAME = "mapped_int_range";
    protected static final String DOUBLE_FIELD_NAME = "mapped_double";
    protected static final String BOOLEAN_FIELD_NAME = "mapped_boolean";
    protected static final String DATE_NANOS_FIELD_NAME = "mapped_date_nanos";
    protected static final String DATE_FIELD_NAME = "mapped_date";
    protected static final String DATE_ALIAS_FIELD_NAME = "mapped_date_alias";
    protected static final String DATE_RANGE_FIELD_NAME = "mapped_date_range";
    protected static final String OBJECT_FIELD_NAME = "mapped_object";
    protected static final String GEO_POINT_FIELD_NAME = "mapped_geo_point";
    protected static final String GEO_POINT_ALIAS_FIELD_NAME = "mapped_geo_point_alias";
    protected static final String GEO_SHAPE_FIELD_NAME = "mapped_geo_shape";
    protected static final String[] MAPPED_FIELD_NAMES = new String[] {
        TEXT_FIELD_NAME,
        TEXT_ALIAS_FIELD_NAME,
        INT_FIELD_NAME,
        INT_RANGE_FIELD_NAME,
        DOUBLE_FIELD_NAME,
        BOOLEAN_FIELD_NAME,
        DATE_NANOS_FIELD_NAME,
        DATE_FIELD_NAME,
        DATE_RANGE_FIELD_NAME,
        OBJECT_FIELD_NAME,
        GEO_POINT_FIELD_NAME,
        GEO_POINT_ALIAS_FIELD_NAME,
        GEO_SHAPE_FIELD_NAME };
    protected static final String[] MAPPED_LEAF_FIELD_NAMES = new String[] {
        TEXT_FIELD_NAME,
        TEXT_ALIAS_FIELD_NAME,
        INT_FIELD_NAME,
        INT_RANGE_FIELD_NAME,
        DOUBLE_FIELD_NAME,
        BOOLEAN_FIELD_NAME,
        DATE_NANOS_FIELD_NAME,
        DATE_FIELD_NAME,
        DATE_RANGE_FIELD_NAME,
        GEO_POINT_FIELD_NAME,
        GEO_POINT_ALIAS_FIELD_NAME };

    private static final Map<String, String> ALIAS_TO_CONCRETE_FIELD_NAME = new HashMap<>();
    static {
        ALIAS_TO_CONCRETE_FIELD_NAME.put(TEXT_ALIAS_FIELD_NAME, TEXT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(INT_ALIAS_FIELD_NAME, INT_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(DATE_ALIAS_FIELD_NAME, DATE_FIELD_NAME);
        ALIAS_TO_CONCRETE_FIELD_NAME.put(GEO_POINT_ALIAS_FIELD_NAME, GEO_POINT_FIELD_NAME);
    }

    private static ServiceHolder serviceHolder;
    private static ServiceHolder serviceHolderWithNoType;
    private static int queryNameId = 0;
    private static Settings nodeSettings;
    private static Index index;
    private static long nowInMillis;

    protected static Index getIndex() {
        return index;
    }

    @SuppressWarnings("deprecation") // dependencies in server for geo_shape field should be decoupled
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singletonList(TestGeoShapeFieldMapperPlugin.class);
    }

    protected void initializeAdditionalMappings(MapperService mapperService) throws IOException {}

    @BeforeClass
    public static void beforeClass() {
        nodeSettings = Settings.builder()
            .put("node.name", AbstractQueryTestCase.class.toString())
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .build();

        index = new Index(randomAlphaOfLengthBetween(1, 10), randomAlphaOfLength(10));
        nowInMillis = randomNonNegativeLong();
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        return serviceHolder.xContentRegistry;
    }

    protected NamedWriteableRegistry namedWriteableRegistry() {
        return serviceHolder.namedWriteableRegistry;
    }

    /**
     * make sure query names are unique by suffixing them with increasing counter
     */
    protected static String createUniqueRandomName() {
        String queryName = randomAlphaOfLengthBetween(1, 10) + queryNameId;
        queryNameId++;
        return queryName;
    }

    protected Settings createTestIndexSettings() {
        // we have to prefer CURRENT since with the range of versions we support it's rather unlikely to get the current actually.
        Version indexVersionCreated = randomBoolean() ? Version.CURRENT : VersionUtils.randomIndexCompatibleVersion(random());
        return Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, indexVersionCreated).build();
    }

    protected static IndexSettings indexSettings() {
        return serviceHolder.idxSettings;
    }

    protected static String expectedFieldName(String builderFieldName) {
        return ALIAS_TO_CONCRETE_FIELD_NAME.getOrDefault(builderFieldName, builderFieldName);
    }

    protected Iterable<MappedFieldType> getMapping() {
        return serviceHolder.mapperService.fieldTypes();
    }

    @AfterClass
    public static void afterClass() throws Exception {
        IOUtils.close(serviceHolder);
        IOUtils.close(serviceHolderWithNoType);
        serviceHolder = null;
        serviceHolderWithNoType = null;
    }

    @Before
    public void beforeTest() throws Exception {
        if (serviceHolder == null) {
            assert serviceHolderWithNoType == null;
            // we initialize the serviceHolder and serviceHolderWithNoType just once, but need some
            // calls to the randomness source during its setup. In order to not mix these calls with
            // the randomness source that is later used in the test method, we use the cluster-manager seed during
            // this setup
            long clusterManagerSeed = SeedUtils.parseSeed(RandomizedTest.getContext().getRunnerSeedAsString());
            RandomizedTest.getContext().runWithPrivateRandomness(clusterManagerSeed, (Callable<Void>) () -> {
                serviceHolder = new ServiceHolder(
                    nodeSettings,
                    createTestIndexSettings(),
                    getPlugins(),
                    nowInMillis,
                    AbstractBuilderTestCase.this,
                    true
                );
                serviceHolderWithNoType = new ServiceHolder(
                    nodeSettings,
                    createTestIndexSettings(),
                    getPlugins(),
                    nowInMillis,
                    AbstractBuilderTestCase.this,
                    false
                );
                return null;
            });
        }

        serviceHolder.clientInvocationHandler.delegate = this;
        serviceHolderWithNoType.clientInvocationHandler.delegate = this;
    }

    @After
    public void afterTest() {
        serviceHolder.clientInvocationHandler.delegate = null;
        serviceHolderWithNoType.clientInvocationHandler.delegate = null;
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected GetResponse executeGet(GetRequest getRequest) {
        throw new UnsupportedOperationException("this test can't handle GET requests");
    }

    /**
     * Override this to handle {@link Client#get(GetRequest)} calls from parsers / builders
     */
    protected MultiTermVectorsResponse executeMultiTermVectors(MultiTermVectorsRequest mtvRequest) {
        throw new UnsupportedOperationException("this test can't handle MultiTermVector requests");
    }

    /**
     * @return a new {@link QueryShardContext} with the provided searcher
     */
    protected static QueryShardContext createShardContext(IndexSearcher searcher) {
        return serviceHolder.createShardContext(searcher);
    }

    /**
     * @return a new {@link QueryShardContext} based on an index with no type registered
     */
    protected static QueryShardContext createShardContextWithNoType() {
        return serviceHolderWithNoType.createShardContext(null);
    }

    /**
     * @return a new {@link QueryShardContext} based on the base test index and queryParserService
     */
    protected static QueryShardContext createShardContext() {
        return createShardContext(null);
    }

    private static class ClientInvocationHandler implements InvocationHandler {
        AbstractBuilderTestCase delegate;

        @Override
        public Object invoke(Object proxy, Method method, Object[] args) throws Throwable {
            if (method.equals(Client.class.getMethod("get", GetRequest.class, ActionListener.class))) {
                GetResponse getResponse = delegate.executeGet((GetRequest) args[0]);
                ActionListener<GetResponse> listener = (ActionListener<GetResponse>) args[1];
                if (randomBoolean()) {
                    listener.onResponse(getResponse);
                } else {
                    new Thread(() -> listener.onResponse(getResponse)).start();
                }
                return null;
            } else if (method.equals(Client.class.getMethod("multiTermVectors", MultiTermVectorsRequest.class))) {
                return new PlainActionFuture<MultiTermVectorsResponse>() {
                    @Override
                    public MultiTermVectorsResponse get() throws InterruptedException, ExecutionException {
                        return delegate.executeMultiTermVectors((MultiTermVectorsRequest) args[0]);
                    }
                };
            } else if (method.equals(Object.class.getMethod("toString"))) {
                return "MockClient";
            }
            throw new UnsupportedOperationException("this test can't handle calls to: " + method);
        }

    }

    private static class ServiceHolder implements Closeable {
        private final IndexFieldDataService indexFieldDataService;
        private final SearchModule searchModule;
        private final NamedWriteableRegistry namedWriteableRegistry;
        private final NamedXContentRegistry xContentRegistry;
        private final ClientInvocationHandler clientInvocationHandler = new ClientInvocationHandler();
        private final IndexSettings idxSettings;
        private final SimilarityService similarityService;
        private final MapperService mapperService;
        private final BitsetFilterCache bitsetFilterCache;
        private final ScriptService scriptService;
        private final Client client;
        private final long nowInMillis;

        ServiceHolder(
            Settings nodeSettings,
            Settings indexSettings,
            Collection<Class<? extends Plugin>> plugins,
            long nowInMillis,
            AbstractBuilderTestCase testCase,
            boolean registerType
        ) throws IOException {
            this.nowInMillis = nowInMillis;
            Environment env = InternalSettingsPreparer.prepareEnvironment(nodeSettings, emptyMap(), null, () -> {
                throw new AssertionError("node.name must be set");
            });
            PluginsService pluginsService;
            pluginsService = new PluginsService(nodeSettings, null, env.modulesDir(), env.pluginsDir(), plugins);

            client = (Client) Proxy.newProxyInstance(Client.class.getClassLoader(), new Class[] { Client.class }, clientInvocationHandler);
            ScriptModule scriptModule = createScriptModule(pluginsService.filterPlugins(ScriptPlugin.class));
            List<Setting<?>> additionalSettings = pluginsService.getPluginSettings();
            SettingsModule settingsModule = new SettingsModule(
                nodeSettings,
                additionalSettings,
                pluginsService.getPluginSettingsFilter(),
                Collections.emptySet()
            );
            searchModule = new SearchModule(nodeSettings, pluginsService.filterPlugins(SearchPlugin.class));
            IndicesModule indicesModule = new IndicesModule(pluginsService.filterPlugins(MapperPlugin.class));
            List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
            entries.addAll(indicesModule.getNamedWriteables());
            entries.addAll(searchModule.getNamedWriteables());
            namedWriteableRegistry = new NamedWriteableRegistry(entries);
            xContentRegistry = new NamedXContentRegistry(
                Stream.of(searchModule.getNamedXContents().stream()).flatMap(Function.identity()).collect(toList())
            );
            IndexScopedSettings indexScopedSettings = settingsModule.getIndexScopedSettings();
            idxSettings = IndexSettingsModule.newIndexSettings(index, indexSettings, indexScopedSettings);
            AnalysisModule analysisModule = new AnalysisModule(TestEnvironment.newEnvironment(nodeSettings), emptyList());
            IndexAnalyzers indexAnalyzers = analysisModule.getAnalysisRegistry().build(idxSettings);
            scriptService = new MockScriptService(Settings.EMPTY, scriptModule.engines, scriptModule.contexts);
            similarityService = new SimilarityService(idxSettings, null, Collections.emptyMap());
            MapperRegistry mapperRegistry = indicesModule.getMapperRegistry();
            mapperService = new MapperService(
                idxSettings,
                indexAnalyzers,
                xContentRegistry,
                similarityService,
                mapperRegistry,
                () -> createShardContext(null),
                () -> false,
                null
            );
            IndicesFieldDataCache indicesFieldDataCache = new IndicesFieldDataCache(nodeSettings, new IndexFieldDataCache.Listener() {
            });
            indexFieldDataService = new IndexFieldDataService(
                idxSettings,
                indicesFieldDataCache,
                new NoneCircuitBreakerService(),
                mapperService
            );
            bitsetFilterCache = new BitsetFilterCache(idxSettings, new BitsetFilterCache.Listener() {
                @Override
                public void onCache(ShardId shardId, Accountable accountable) {

                }

                @Override
                public void onRemoval(ShardId shardId, Accountable accountable) {

                }
            });

            if (registerType) {
                mapperService.merge(
                    "_doc",
                    new CompressedXContent(
                        Strings.toString(
                            PutMappingRequest.simpleMapping(
                                TEXT_FIELD_NAME,
                                "type=text",
                                KEYWORD_FIELD_NAME,
                                "type=keyword",
                                TEXT_ALIAS_FIELD_NAME,
                                "type=alias,path=" + TEXT_FIELD_NAME,
                                INT_FIELD_NAME,
                                "type=integer",
                                INT_ALIAS_FIELD_NAME,
                                "type=alias,path=" + INT_FIELD_NAME,
                                INT_RANGE_FIELD_NAME,
                                "type=integer_range",
                                DOUBLE_FIELD_NAME,
                                "type=double",
                                BOOLEAN_FIELD_NAME,
                                "type=boolean",
                                DATE_NANOS_FIELD_NAME,
                                "type=date_nanos",
                                DATE_FIELD_NAME,
                                "type=date",
                                DATE_ALIAS_FIELD_NAME,
                                "type=alias,path=" + DATE_FIELD_NAME,
                                DATE_RANGE_FIELD_NAME,
                                "type=date_range",
                                OBJECT_FIELD_NAME,
                                "type=object",
                                GEO_POINT_FIELD_NAME,
                                "type=geo_point",
                                GEO_POINT_ALIAS_FIELD_NAME,
                                "type=alias,path=" + GEO_POINT_FIELD_NAME,
                                GEO_SHAPE_FIELD_NAME,
                                "type=geo_shape"
                            )
                        )
                    ),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
                // also add mappings for two inner field in the object field
                mapperService.merge(
                    "_doc",
                    new CompressedXContent(
                        "{\"properties\":{\""
                            + OBJECT_FIELD_NAME
                            + "\":{\"type\":\"object\","
                            + "\"properties\":{\""
                            + DATE_FIELD_NAME
                            + "\":{\"type\":\"date\"},\""
                            + INT_FIELD_NAME
                            + "\":{\"type\":\"integer\"}}}}}"
                    ),
                    MapperService.MergeReason.MAPPING_UPDATE
                );
                testCase.initializeAdditionalMappings(mapperService);
            }
        }

        public static Predicate<String> indexNameMatcher() {
            // Simplistic index name matcher used for testing
            return pattern -> Regex.simpleMatch(pattern, index.getName());
        }

        @Override
        public void close() throws IOException {}

        QueryShardContext createShardContext(IndexSearcher searcher) {
            return new QueryShardContext(
                0,
                idxSettings,
                BigArrays.NON_RECYCLING_INSTANCE,
                bitsetFilterCache,
                indexFieldDataService::getForField,
                mapperService,
                similarityService,
                scriptService,
                xContentRegistry,
                namedWriteableRegistry,
                this.client,
                searcher,
                () -> nowInMillis,
                null,
                indexNameMatcher(),
                () -> true,
                null
            );
        }

        ScriptModule createScriptModule(List<ScriptPlugin> scriptPlugins) {
            if (scriptPlugins == null || scriptPlugins.isEmpty()) {
                return new ScriptModule(Settings.EMPTY, singletonList(new ScriptPlugin() {
                    @Override
                    public ScriptEngine getScriptEngine(Settings settings, Collection<ScriptContext<?>> contexts) {
                        return new MockScriptEngine(MockScriptEngine.NAME, Collections.singletonMap("1", script -> "1"), emptyMap());
                    }
                }));
            }
            return new ScriptModule(Settings.EMPTY, scriptPlugins);
        }
    }
}
