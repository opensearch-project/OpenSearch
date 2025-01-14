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

import com.carrotsearch.randomizedtesting.annotations.TestGroup;
import com.carrotsearch.randomizedtesting.generators.RandomNumbers;
import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.search.Sort;
import org.apache.lucene.search.TotalHits;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.node.hotthreads.NodeHotThreads;
import org.opensearch.action.admin.cluster.node.info.NodesInfoResponse;
import org.opensearch.action.admin.cluster.repositories.put.PutRepositoryRequestBuilder;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.cluster.tasks.PendingClusterTasksResponse;
import org.opensearch.action.admin.indices.create.CreateIndexRequestBuilder;
import org.opensearch.action.admin.indices.exists.indices.IndicesExistsResponse;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.admin.indices.forcemerge.ForceMergeResponse;
import org.opensearch.action.admin.indices.get.GetIndexResponse;
import org.opensearch.action.admin.indices.refresh.RefreshResponse;
import org.opensearch.action.admin.indices.segments.IndexSegments;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.action.admin.indices.template.put.PutIndexTemplateRequestBuilder;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.ClearScrollResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.WriteRequest;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.AdminClient;
import org.opensearch.client.Client;
import org.opensearch.client.ClusterAdminClient;
import org.opensearch.client.Requests;
import org.opensearch.client.RestClient;
import org.opensearch.cluster.ClusterModule;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.OpenSearchNodeCommand;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.metadata.Context;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.cluster.routing.allocation.AwarenessReplicaBalance;
import org.opensearch.cluster.routing.allocation.DiskThresholdSettings;
import org.opensearch.cluster.routing.allocation.decider.AwarenessAllocationDecider;
import org.opensearch.cluster.routing.allocation.decider.EnableAllocationDecider;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.cluster.service.applicationtemplates.TestSystemTemplatesRepositoryPlugin;
import org.opensearch.common.Nullable;
import org.opensearch.common.Priority;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.FeatureFlagSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.smile.SmileXContent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.transport.TransportAddress;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.concurrency.OpenSearchRejectedExecutionException;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.env.Environment;
import org.opensearch.env.TestEnvironment;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.MergeSchedulerConfig;
import org.opensearch.index.MockEngineFactoryPlugin;
import org.opensearch.index.TieredMergePolicyProvider;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.engine.Segment;
import org.opensearch.index.mapper.CompletionFieldMapper;
import org.opensearch.index.mapper.MockFieldFilterPlugin;
import org.opensearch.index.remote.RemoteStoreEnums;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.index.remote.RemoteStorePathStrategy;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.Translog;
import org.opensearch.indices.IndicesQueryCache;
import org.opensearch.indices.IndicesRequestCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.RemoteStoreSettings;
import org.opensearch.indices.replication.common.ReplicationType;
import org.opensearch.indices.store.IndicesStore;
import org.opensearch.monitor.os.OsInfo;
import org.opensearch.node.NodeMocksPlugin;
import org.opensearch.node.remotestore.RemoteStoreNodeService;
import org.opensearch.plugins.NetworkPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.repositories.IndexId;
import org.opensearch.repositories.blobstore.BlobStoreRepository;
import org.opensearch.repositories.fs.FsRepository;
import org.opensearch.repositories.fs.ReloadableFsRepository;
import org.opensearch.script.MockScriptService;
import org.opensearch.search.MockSearchService;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchService;
import org.opensearch.telemetry.TelemetrySettings;
import org.opensearch.test.client.RandomizingClient;
import org.opensearch.test.disruption.NetworkDisruption;
import org.opensearch.test.disruption.ServiceDisruptionScheme;
import org.opensearch.test.store.MockFSIndexStore;
import org.opensearch.test.telemetry.MockTelemetryPlugin;
import org.opensearch.test.transport.MockTransportService;
import org.opensearch.transport.TransportInterceptor;
import org.opensearch.transport.TransportRequest;
import org.opensearch.transport.TransportRequestHandler;
import org.opensearch.transport.TransportService;
import org.hamcrest.Matchers;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Rule;

import java.io.IOException;
import java.lang.Runtime.Version;
import java.lang.annotation.Annotation;
import java.lang.annotation.ElementType;
import java.lang.annotation.Inherited;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import reactor.util.annotation.NonNull;

import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_REPLICAS;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.unit.TimeValue.timeValueMillis;
import static org.opensearch.core.common.util.CollectionUtils.eagerPartition;
import static org.opensearch.discovery.DiscoveryModule.DISCOVERY_SEED_PROVIDERS_SETTING;
import static org.opensearch.discovery.SettingsBasedSeedHostsProvider.DISCOVERY_SEED_HOSTS_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING;
import static org.opensearch.index.IndexSettings.INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.remote.RemoteStoreEnums.PathHashAlgorithm.FNV_1A_COMPOSITE_1;
import static org.opensearch.indices.IndicesService.CLUSTER_REPLICATION_TYPE_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeAttribute.REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.MIGRATION_DIRECTION_SETTING;
import static org.opensearch.node.remotestore.RemoteStoreNodeService.REMOTE_STORE_COMPATIBILITY_MODE_SETTING;
import static org.opensearch.test.XContentTestUtils.convertToMap;
import static org.opensearch.test.XContentTestUtils.differenceBetweenMapsIgnoringArrayOrder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoTimeout;
import static org.hamcrest.Matchers.emptyIterable;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

/**
 * {@link OpenSearchIntegTestCase} is an abstract base class to run integration
 * tests against a JVM private OpenSearch Cluster. The test class supports 2 different
 * cluster scopes.
 * <ul>
 * <li>{@link Scope#TEST} - uses a new cluster for each individual test method.</li>
 * <li>{@link Scope#SUITE} - uses a cluster shared across all test methods in the same suite</li>
 * </ul>
 * <p>
 * The most common test scope is {@link Scope#SUITE} which shares a cluster per test suite.
 * <p>
 * If the test methods need specific node settings or change persistent and/or transient cluster settings {@link Scope#TEST}
 * should be used. To configure a scope for the test cluster the {@link ClusterScope} annotation
 * should be used, here is an example:
 * <pre>
 *
 * {@literal @}NodeScope(scope=Scope.TEST) public class SomeIT extends OpenSearchIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * If no {@link ClusterScope} annotation is present on an integration test the default scope is {@link Scope#SUITE}
 * <p>
 * A test cluster creates a set of nodes in the background before the test starts. The number of nodes in the cluster is
 * determined at random and can change across tests. The {@link ClusterScope} allows configuring the initial number of nodes
 * that are created before the tests start.
 *  <pre>
 * {@literal @}NodeScope(scope=Scope.SUITE, numDataNodes=3)
 * public class SomeIT extends OpenSearchIntegTestCase {
 * public void testMethod() {}
 * }
 * </pre>
 * <p>
 * Note, the {@link OpenSearchIntegTestCase} uses randomized settings on a cluster and index level. For instance
 * each test might use different directory implementation for each test or will return a random client to one of the
 * nodes in the cluster for each call to {@link #client()}. Test failures might only be reproducible if the correct
 * system properties are passed to the test execution environment.
 * <p>
 * This class supports the following system properties (passed with -Dkey=value to the application)
 * <ul>
 * <li>-D{@value #TESTS_ENABLE_MOCK_MODULES} - a boolean value to enable or disable mock modules. This is
 * useful to test the system without asserting modules that to make sure they don't hide any bugs in production.</li>
 * <li> - a random seed used to initialize the index random context.
 * </ul>
 */
@LuceneTestCase.SuppressFileSystems("ExtrasFS") // doesn't work with potential multi data path from test cluster yet
public abstract class OpenSearchIntegTestCase extends OpenSearchTestCase {

    /**
     * Property that controls whether ThirdParty Integration tests are run (not the default).
     */
    public static final String SYSPROP_THIRDPARTY = "tests.thirdparty";

    /**
     * The lucene_default {@link Codec} is not added to the list as it internally maps to Asserting {@link Codec}.
     * The override to fetch the {@link CompletionFieldMapper.CompletionFieldType} postings format is not available for this codec.
     */
    public static final List<String> CODECS = List.of(
        CodecService.DEFAULT_CODEC,
        CodecService.LZ4,
        CodecService.BEST_COMPRESSION_CODEC,
        CodecService.ZLIB
    );

    /**
     * Annotation for third-party integration tests.
     * <p>
     * These are tests the require a third-party service in order to run. They
     * may require the user to manually configure an external process (such as rabbitmq),
     * or may additionally require some external configuration (e.g. AWS credentials)
     * via the {@code tests.config} system property.
     */
    @Inherited
    @Retention(RetentionPolicy.RUNTIME)
    @Target(ElementType.TYPE)
    @TestGroup(enabled = false, sysProperty = OpenSearchIntegTestCase.SYSPROP_THIRDPARTY)
    public @interface ThirdParty {
    }

    /** node names of the corresponding clusters will start with these prefixes */
    public static final String SUITE_CLUSTER_NODE_PREFIX = "node_s";
    public static final String TEST_CLUSTER_NODE_PREFIX = "node_t";

    /**
     * Key used to eventually switch to using an external cluster and provide its transport addresses
     */
    public static final String TESTS_CLUSTER = "tests.cluster";

    /**
     * Key used to retrieve the index random seed from the index settings on a running node.
     * The value of this seed can be used to initialize a random context for a specific index.
     * It's set once per test via a generic index template.
     */
    public static final Setting<Long> INDEX_TEST_SEED_SETTING = Setting.longSetting(
        "index.tests.seed",
        0,
        Long.MIN_VALUE,
        Property.IndexScope
    );

    /**
     * A boolean value to enable or disable mock modules. This is useful to test the
     * system without asserting modules that to make sure they don't hide any bugs in
     * production.
     *
     * @see OpenSearchIntegTestCase
     */
    public static final String TESTS_ENABLE_MOCK_MODULES = "tests.enable_mock_modules";

    private static final boolean MOCK_MODULES_ENABLED = "true".equals(System.getProperty(TESTS_ENABLE_MOCK_MODULES, "true"));

    @Rule
    public static OpenSearchTestClusterRule testClusterRule = new OpenSearchTestClusterRule();

    /**
     * Threshold at which indexing switches from frequently async to frequently bulk.
     */
    private static final int FREQUENT_BULK_THRESHOLD = 300;

    /**
     * Threshold at which bulk indexing will always be used.
     */
    private static final int ALWAYS_BULK_THRESHOLD = 3000;

    /**
     * Maximum number of async operations that indexRandom will kick off at one time.
     */
    private static final int MAX_IN_FLIGHT_ASYNC_INDEXES = 150;

    /**
     * Maximum number of documents in a single bulk index request.
     */
    private static final int MAX_BULK_INDEX_REQUEST_SIZE = 1000;

    /**
     * Default minimum number of shards for an index
     */
    protected static final int DEFAULT_MIN_NUM_SHARDS = 1;

    /**
     * Default maximum number of shards for an index
     */
    protected static final int DEFAULT_MAX_NUM_SHARDS = 10;

    /**
     * Key to provide the cluster name
     */
    public static final String TESTS_CLUSTER_NAME = "tests.clustername";

    protected static final String REMOTE_BACKED_STORAGE_REPOSITORY_NAME = "test-remote-store-repo";

    private static Boolean prefixModeVerificationEnable;

    private static Boolean translogPathFixedPrefix;

    private static Boolean segmentsPathFixedPrefix;

    protected static Boolean snapshotShardPathFixedPrefix;

    private Path remoteStoreRepositoryPath;

    private ReplicationType randomReplicationType;

    private String randomStorageType;

    @BeforeClass
    public static void beforeClass() throws Exception {
        prefixModeVerificationEnable = randomBoolean();
        translogPathFixedPrefix = randomBoolean();
        segmentsPathFixedPrefix = randomBoolean();
        snapshotShardPathFixedPrefix = randomBoolean();
        testClusterRule.beforeClass();
    }

    @Override
    protected final boolean enableWarningsCheck() {
        // In an integ test it doesn't make sense to keep track of warnings: if the cluster is external the warnings are in another jvm,
        // if the cluster is internal the deprecation logger is shared across all nodes
        return false;
    }

    /**
     * Creates a randomized index template. This template is used to pass in randomized settings on a
     * per index basis. Allows to enable/disable the randomization for number of shards and replicas
     */
    protected void randomIndexTemplate() {

        // TODO move settings for random directory etc here into the index based randomized settings.
        if (cluster().size() > 0) {
            Settings.Builder randomSettingsBuilder = setRandomIndexSettings(random(), Settings.builder());
            if (isInternalCluster()) {
                // this is only used by mock plugins and if the cluster is not internal we just can't set it
                randomSettingsBuilder.put(INDEX_TEST_SEED_SETTING.getKey(), random().nextLong());
            }

            randomSettingsBuilder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards()).put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas());

            // if the test class is annotated with SuppressCodecs("*"), it means don't use lucene's codec randomization
            // otherwise, use it, it has assertions and so on that can find bugs.
            SuppressCodecs annotation = getClass().getAnnotation(SuppressCodecs.class);
            if (annotation != null && annotation.value().length == 1 && "*".equals(annotation.value()[0])) {
                randomSettingsBuilder.put("index.codec", randomFrom(CODECS));
            } else {
                randomSettingsBuilder.put("index.codec", CodecService.LUCENE_DEFAULT_CODEC);
            }

            for (String setting : randomSettingsBuilder.keys()) {
                assertThat("non index. prefix setting set on index template, its a node setting...", setting, startsWith("index."));
            }
            // always default delayed allocation to 0 to make sure we have tests are not delayed
            randomSettingsBuilder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
            if (randomBoolean()) {
                randomSettingsBuilder.put(IndexModule.INDEX_QUERY_CACHE_ENABLED_SETTING.getKey(), randomBoolean());
            }
            PutIndexTemplateRequestBuilder putTemplate = client().admin()
                .indices()
                .preparePutTemplate("random_index_template")
                .setPatterns(Collections.singletonList("*"))
                .setOrder(0)
                .setSettings(randomSettingsBuilder);
            assertAcked(putTemplate.execute().actionGet());
        }
    }

    protected Settings.Builder setRandomIndexSettings(Random random, Settings.Builder builder) {
        setRandomIndexMergeSettings(random, builder);
        setRandomIndexTranslogSettings(random, builder);

        if (random.nextBoolean()) {
            builder.put(MergeSchedulerConfig.AUTO_THROTTLE_SETTING.getKey(), false);
        }

        if (random.nextBoolean()) {
            builder.put(IndicesRequestCache.INDEX_CACHE_REQUEST_ENABLED_SETTING.getKey(), random.nextBoolean());
        }

        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_CHECK_ON_STARTUP.getKey(), randomFrom(random, "false", "checksum", "true"));
        }

        if (random.nextBoolean()) {
            // keep this low so we don't stall tests
            builder.put(
                UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(),
                RandomNumbers.randomIntBetween(random, 1, 15) + "ms"
            );
        }

        if (random.nextBoolean()) {
            builder.put(Store.FORCE_RAM_TERM_DICT.getKey(), true);
        }

        return builder;
    }

    private static Settings.Builder setRandomIndexMergeSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(
                TieredMergePolicyProvider.INDEX_COMPOUND_FORMAT_SETTING.getKey(),
                (random.nextBoolean() ? random.nextDouble() : random.nextBoolean()).toString()
            );
        }
        switch (random.nextInt(4)) {
            case 3:
                final int maxThreadCount = RandomNumbers.randomIntBetween(random, 1, 4);
                final int maxMergeCount = RandomNumbers.randomIntBetween(random, maxThreadCount, maxThreadCount + 4);
                builder.put(MergeSchedulerConfig.MAX_MERGE_COUNT_SETTING.getKey(), maxMergeCount);
                builder.put(MergeSchedulerConfig.MAX_THREAD_COUNT_SETTING.getKey(), maxThreadCount);
                break;
        }

        return builder;
    }

    private static Settings.Builder setRandomIndexTranslogSettings(Random random, Settings.Builder builder) {
        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(),
                new ByteSizeValue(RandomNumbers.randomIntBetween(random, 1, 300), ByteSizeUnit.MB)
            );
        }
        if (random.nextBoolean()) {
            builder.put(IndexSettings.INDEX_TRANSLOG_FLUSH_THRESHOLD_SIZE_SETTING.getKey(), new ByteSizeValue(1, ByteSizeUnit.PB)); // just
                                                                                                                                    // don't
                                                                                                                                    // flush
        }
        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_DURABILITY_SETTING.getKey(),
                RandomPicks.randomFrom(random, Translog.Durability.values())
            );
        }

        if (random.nextBoolean()) {
            builder.put(
                IndexSettings.INDEX_TRANSLOG_SYNC_INTERVAL_SETTING.getKey(),
                RandomNumbers.randomIntBetween(random, 100, 5000),
                TimeUnit.MILLISECONDS
            );
        }

        return builder;
    }

    /**
     * @return An exclude set of index templates that will not be removed in between tests.
     */
    protected Set<String> excludeTemplates() {
        return Collections.emptySet();
    }

    protected void beforeIndexDeletion() throws Exception {
        cluster().beforeIndexDeletion();
    }

    public static TestCluster cluster() {
        return testClusterRule.cluster();
    }

    public static boolean isInternalCluster() {
        return testClusterRule.isInternalCluster();
    }

    public static InternalTestCluster internalCluster() {
        return testClusterRule.internalCluster().orElseThrow(() -> new UnsupportedOperationException("current test cluster is immutable"));
    }

    public ClusterService clusterService() {
        return internalCluster().clusterService();
    }

    public static Client client() {
        return client(null);
    }

    public static Client client(@Nullable String node) {
        return testClusterRule.clientForNode(node);
    }

    public static Client dataNodeClient() {
        Client client = internalCluster().dataNodeClient();
        if (frequently()) {
            client = new RandomizingClient(client, random());
        }
        return client;
    }

    public static Iterable<Client> clients() {
        return cluster().getClients();
    }

    protected int minimumNumberOfShards() {
        return DEFAULT_MIN_NUM_SHARDS;
    }

    protected int maximumNumberOfShards() {
        return DEFAULT_MAX_NUM_SHARDS;
    }

    protected int numberOfShards() {
        return between(minimumNumberOfShards(), maximumNumberOfShards());
    }

    protected int minimumNumberOfReplicas() {
        return 0;
    }

    protected int maximumNumberOfReplicas() {
        // use either 0 or 1 replica, yet a higher amount when possible, but only rarely
        int maxNumReplicas = Math.max(0, cluster().numDataNodes() - 1);
        return frequently() ? Math.min(1, maxNumReplicas) : maxNumReplicas;
    }

    protected int numberOfReplicas() {
        return between(minimumNumberOfReplicas(), maximumNumberOfReplicas());
    }

    public void setDisruptionScheme(ServiceDisruptionScheme scheme) {
        internalCluster().setDisruptionScheme(scheme);
    }

    /**
     * Creates a disruption that isolates the current cluster-manager node from all other nodes in the cluster.
     *
     * @param disruptionType type of disruption to create
     * @return disruption
     */
    protected static NetworkDisruption isolateClusterManagerDisruption(NetworkDisruption.NetworkLinkDisruptionType disruptionType) {
        final String clusterManagerNode = internalCluster().getClusterManagerName();
        return new NetworkDisruption(
            new NetworkDisruption.TwoPartitions(
                Collections.singleton(clusterManagerNode),
                Arrays.stream(internalCluster().getNodeNames())
                    .filter(name -> name.equals(clusterManagerNode) == false)
                    .collect(Collectors.toSet())
            ),
            disruptionType
        );
    }

    /**
     * Returns a settings object used in {@link #createIndex(String...)} and {@link #prepareCreate(String)} and friends.
     * This method can be overwritten by subclasses to set defaults for the indices that are created by the test.
     * By default it returns a settings object that sets a random number of shards. Number of shards and replicas
     * can be controlled through specific methods.
     */
    public Settings indexSettings() {
        Settings.Builder builder = Settings.builder();
        int numberOfShards = numberOfShards();
        if (numberOfShards > 0) {
            builder.put(SETTING_NUMBER_OF_SHARDS, numberOfShards).build();
        }
        int numberOfReplicas = numberOfReplicas();
        if (numberOfReplicas >= 0) {
            builder.put(SETTING_NUMBER_OF_REPLICAS, numberOfReplicas).build();
        }
        // 30% of the time
        if (randomInt(9) < 3) {
            final String dataPath = randomAlphaOfLength(10);
            logger.info("using custom data_path for index: [{}]", dataPath);
            builder.put(IndexMetadata.SETTING_DATA_PATH, dataPath);
        }
        // always default delayed allocation to 0 to make sure we have tests are not delayed
        builder.put(UnassignedInfo.INDEX_DELAYED_NODE_LEFT_TIMEOUT_SETTING.getKey(), 0);
        if (randomBoolean()) {
            builder.put(IndexSettings.INDEX_SOFT_DELETES_RETENTION_OPERATIONS_SETTING.getKey(), between(0, 1000));
        }
        if (randomBoolean()) {
            builder.put(
                INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.getKey(),
                timeValueMillis(
                    randomLongBetween(
                        0,
                        randomBoolean() ? 1000 : INDEX_SOFT_DELETES_RETENTION_LEASE_PERIOD_SETTING.get(Settings.EMPTY).millis()
                    )
                ).getStringRep()
            );
        }

        if (randomBoolean()) {
            builder.put(INDEX_DOC_ID_FUZZY_SET_ENABLED_SETTING.getKey(), true);
            builder.put(INDEX_DOC_ID_FUZZY_SET_FALSE_POSITIVE_PROBABILITY_SETTING.getKey(), randomDoubleBetween(0.01, 0.50, true));
        }

        return builder.build();
    }

    /**
     * Setting all feature flag settings at base IT, which can be overridden later by individual
     * IT classes.
     *
     * @return Feature flag settings.
     */
    protected Settings featureFlagSettings() {
        Settings.Builder featureSettings = Settings.builder();
        for (Setting builtInFlag : FeatureFlagSettings.BUILT_IN_FEATURE_FLAGS) {
            featureSettings.put(builtInFlag.getKey(), builtInFlag.getDefaultRaw(Settings.EMPTY));
        }
        // Enabling Telemetry setting by default
        featureSettings.put(FeatureFlags.TELEMETRY_SETTING.getKey(), true);
        featureSettings.put(FeatureFlags.APPLICATION_BASED_CONFIGURATION_TEMPLATES_SETTING.getKey(), true);

        return featureSettings.build();
    }

    /**
     * Represent if it needs to trigger remote state restore or not.
     * For tests with remote store enabled domain, it will be overridden to true.
     *
     * @return if needs to perform remote state restore or not
     */
    protected boolean triggerRemoteStateRestore() {
        return false;
    }

    /**
     * For tests with remote cluster state, it will reset the cluster and cluster state will be
     * restored from remote.
     */
    protected void performRemoteStoreTestAction() {
        if (triggerRemoteStateRestore()) {
            String clusterUUIDBefore = clusterService().state().metadata().clusterUUID();
            internalCluster().resetCluster();
            String clusterUUIDAfter = clusterService().state().metadata().clusterUUID();
            // assertion that UUID is changed post restore.
            assertFalse(clusterUUIDBefore.equals(clusterUUIDAfter));
        }
    }

    /**
     * Creates one or more indices and asserts that the indices are acknowledged. If one of the indices
     * already exists this method will fail and wipe all the indices created so far.
     */
    public final void createIndex(String... names) {

        List<String> created = new ArrayList<>();
        for (String name : names) {
            boolean success = false;
            try {
                assertAcked(prepareCreate(name));
                created.add(name);
                success = true;
            } finally {
                if (!success && !created.isEmpty()) {
                    cluster().wipeIndices(created.toArray(new String[0]));
                }
            }
        }
    }

    /**
     * creates an index with the given setting
     */
    public final void createIndex(String name, Settings indexSettings) {
        assertAcked(prepareCreate(name).setSettings(indexSettings));
    }

    /**
     * creates an index with the given setting
     */
    public final void createIndex(String name, Context context) {
        assertAcked(prepareCreate(name).setContext(context));
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     */
    public final CreateIndexRequestBuilder prepareCreate(String index) {
        return prepareCreate(index, -1);
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public final CreateIndexRequestBuilder prepareCreate(String index, int numNodes) {
        return prepareCreate(index, numNodes, Settings.builder());
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}, augmented
     * by the given builder
     */
    public CreateIndexRequestBuilder prepareCreate(String index, Settings.Builder settingsBuilder) {
        return prepareCreate(index, -1, settingsBuilder);
    }

    /**
     * Creates a new {@link CreateIndexRequestBuilder} with the settings obtained from {@link #indexSettings()}.
     * The index that is created with this builder will only be allowed to allocate on the number of nodes passed to this
     * method.
     * <p>
     * This method uses allocation deciders to filter out certain nodes to allocate the created index on. It defines allocation
     * rules based on <code>index.routing.allocation.exclude._name</code>.
     * </p>
     */
    public CreateIndexRequestBuilder prepareCreate(String index, int numNodes, Settings.Builder settingsBuilder) {
        Settings.Builder builder = Settings.builder().put(indexSettings()).put(settingsBuilder.build());

        if (numNodes > 0) {
            internalCluster().ensureAtLeastNumDataNodes(numNodes);
            getExcludeSettings(numNodes, builder);
        }
        return client().admin().indices().prepareCreate(index).setSettings(builder.build());
    }

    private Settings.Builder getExcludeSettings(int num, Settings.Builder builder) {
        String exclude = String.join(",", internalCluster().allDataNodesButN(num));
        builder.put("index.routing.allocation.exclude._name", exclude);
        return builder;
    }

    /**
     * Waits until all nodes have no pending tasks.
     */
    public void waitNoPendingTasksOnAll() throws Exception {
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
        assertBusy(() -> {
            for (Client client : clients()) {
                ClusterHealthResponse clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
                PendingClusterTasksResponse pendingTasks = client.admin().cluster().preparePendingClusterTasks().setLocal(true).get();
                assertThat("client " + client + " still has pending tasks " + pendingTasks, pendingTasks, Matchers.emptyIterable());
                clusterHealth = client.admin().cluster().prepareHealth().setLocal(true).get();
                assertThat("client " + client + " still has in flight fetch", clusterHealth.getNumberOfInFlightFetch(), equalTo(0));
            }
        });
        assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForEvents(Priority.LANGUID).get());
    }

    /** Ensures the result counts are as expected, and logs the results if different */
    public void assertResultsAndLogOnFailure(long expectedResults, SearchResponse searchResponse) {
        final TotalHits totalHits = searchResponse.getHits().getTotalHits();
        if (totalHits.value != expectedResults || totalHits.relation != TotalHits.Relation.EQUAL_TO) {
            StringBuilder sb = new StringBuilder("search result contains [");
            String value = Long.toString(totalHits.value) + (totalHits.relation == TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO ? "+" : "");
            sb.append(value).append("] results. expected [").append(expectedResults).append("]");
            String failMsg = sb.toString();
            for (SearchHit hit : searchResponse.getHits().getHits()) {
                sb.append("\n-> _index: [").append(hit.getIndex()).append("] id [").append(hit.getId()).append("]");
            }
            logger.warn("{}", sb);
            fail(failMsg);
        }
    }

    /**
     * Restricts the given index to be allocated on <code>n</code> nodes using the allocation deciders.
     * Yet if the shards can't be allocated on any other node shards for this index will remain allocated on
     * more than <code>n</code> nodes.
     */
    public void allowNodes(String index, int n) {
        assert index != null;
        internalCluster().ensureAtLeastNumDataNodes(n);
        Settings.Builder builder = Settings.builder();
        if (n > 0) {
            getExcludeSettings(n, builder);
        }
        Settings build = builder.build();
        if (!build.isEmpty()) {
            logger.debug("allowNodes: updating [{}]'s setting to [{}]", index, build.toDelimitedString(';'));
            client().admin().indices().prepareUpdateSettings(index).setSettings(build).execute().actionGet();
        }
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     */
    public ClusterHealthStatus ensureGreen(String... indices) {
        return ensureGreen(TimeValue.timeValueSeconds(30), indices);
    }

    /**
     * Ensures the cluster has a green state via the cluster health API. This method will also wait for relocations.
     * It is useful to ensure that all action on the cluster have finished and all shards that were currently relocating
     * are now allocated and started.
     *
     * @param timeout time out value to set on {@link ClusterHealthRequest}
     */
    public ClusterHealthStatus ensureGreen(TimeValue timeout, String... indices) {
        return ensureColor(ClusterHealthStatus.GREEN, timeout, false, indices);
    }

    public ClusterHealthStatus ensureGreen(TimeValue timeout, boolean waitForNoRelocatingShards, String... indices) {
        return ensureColor(ClusterHealthStatus.GREEN, timeout, waitForNoRelocatingShards, false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API.
     */
    public ClusterHealthStatus ensureYellow(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), false, indices);
    }

    /**
     * Ensures the cluster has a red state via the cluster health API.
     */
    public ClusterHealthStatus ensureRed(String... indices) {
        return ensureColor(ClusterHealthStatus.RED, TimeValue.timeValueSeconds(30), false, indices);
    }

    /**
     * Ensures the cluster has a yellow state via the cluster health API and ensures the that cluster has no initializing shards
     * for the given indices
     */
    public ClusterHealthStatus ensureYellowAndNoInitializingShards(String... indices) {
        return ensureColor(ClusterHealthStatus.YELLOW, TimeValue.timeValueSeconds(30), true, indices);
    }

    private ClusterHealthStatus ensureColor(
        ClusterHealthStatus clusterHealthStatus,
        TimeValue timeout,
        boolean waitForNoInitializingShards,
        String... indices
    ) {
        return ensureColor(clusterHealthStatus, timeout, true, waitForNoInitializingShards, indices);
    }

    private ClusterHealthStatus ensureColor(
        ClusterHealthStatus clusterHealthStatus,
        TimeValue timeout,
        boolean waitForNoRelocatingShards,
        boolean waitForNoInitializingShards,
        String... indices
    ) {
        String color = clusterHealthStatus.name().toLowerCase(Locale.ROOT);
        String method = "ensure" + Strings.capitalize(color);

        ClusterHealthRequest healthRequest = Requests.clusterHealthRequest(indices)
            .timeout(timeout)
            .waitForStatus(clusterHealthStatus)
            .waitForEvents(Priority.LANGUID)
            .waitForNoRelocatingShards(waitForNoRelocatingShards)
            .waitForNoInitializingShards(waitForNoInitializingShards)
            // We currently often use ensureGreen or ensureYellow to check whether the cluster is back in a good state after shutting down
            // a node. If the node that is stopped is the cluster-manager node, another node will become cluster-manager and publish a
            // cluster state where it is cluster-manager but where the node that was stopped hasn't been removed yet from the cluster state.
            // It will only subsequently publish a second state where the old cluster-manager is removed.
            // If the ensureGreen/ensureYellow is timed just right, it will get to execute before the second cluster state update removes
            // the old cluster-manager and the condition ensureGreen / ensureYellow will trivially hold if it held before the node was
            // shut down. The following "waitForNodes" condition ensures that the node has been removed by the cluster-manager
            // so that the health check applies to the set of nodes we expect to be part of the cluster.
            .waitForNodes(Integer.toString(cluster().size()));

        ClusterHealthResponse actionGet = client().admin().cluster().health(healthRequest).actionGet();
        if (actionGet.isTimedOut()) {
            final String hotThreads = client().admin()
                .cluster()
                .prepareNodesHotThreads()
                .setThreads(99999)
                .setIgnoreIdleThreads(false)
                .get()
                .getNodes()
                .stream()
                .map(NodeHotThreads::getHotThreads)
                .collect(Collectors.joining("\n"));
            logger.info(
                "{} timed out, cluster state:\n{}\npending tasks:\n{}\nhot threads:\n{}\n",
                method,
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get(),
                hotThreads
            );
            fail("timed out waiting for " + color + " state");
        }
        assertThat(
            "Expected at least " + clusterHealthStatus + " but got " + actionGet.getStatus(),
            actionGet.getStatus().value(),
            lessThanOrEqualTo(clusterHealthStatus.value())
        );
        logger.debug("indices {} are {}", indices.length == 0 ? "[_all]" : indices, color);
        return actionGet.getStatus();
    }

    /**
     * Waits for all relocating shards to become active using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation() {
        return waitForRelocation(null);
    }

    /**
     * Waits for all relocating shards to become active and the cluster has reached the given health status
     * using the cluster health API.
     */
    public ClusterHealthStatus waitForRelocation(ClusterHealthStatus status) {
        ClusterHealthRequest request = Requests.clusterHealthRequest().waitForNoRelocatingShards(true).waitForEvents(Priority.LANGUID);
        if (status != null) {
            request.waitForStatus(status);
        }
        ClusterHealthResponse actionGet = client().admin().cluster().health(request).actionGet();
        if (actionGet.isTimedOut()) {
            logger.info(
                "waitForRelocation timed out (status={}), cluster state:\n{}\n{}",
                status,
                client().admin().cluster().prepareState().get().getState(),
                client().admin().cluster().preparePendingClusterTasks().get()
            );
            assertThat("timed out waiting for relocation", actionGet.isTimedOut(), equalTo(false));
        }
        if (status != null) {
            assertThat(actionGet.getStatus(), equalTo(status));
        }
        return actionGet.getStatus();
    }

    /**
     * Waits until at least a give number of document is visible for searchers
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link BackgroundIndexer}. It will be first checked for documents indexed.
     *                This saves on unneeded searches.
     */
    public void waitForDocs(final long numDocs, final BackgroundIndexer indexer) throws Exception {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        final long maxWaitTimeMs = Math.max(90 * 1000, 200 * numDocs);

        assertBusy(() -> {
            long lastKnownCount = indexer.totalIndexedDocs();

            if (lastKnownCount >= numDocs) {
                try {
                    long count = client().prepareSearch()
                        .setTrackTotalHits(true)
                        .setSize(0)
                        .setQuery(matchAllQuery())
                        .get()
                        .getHits()
                        .getTotalHits().value;

                    if (count == lastKnownCount) {
                        // no progress - try to refresh for the next time
                        client().admin().indices().prepareRefresh().get();
                    }
                    lastKnownCount = count;
                } catch (Exception e) { // count now acts like search and barfs if all shards failed...
                    logger.debug("failed to executed count", e);
                    throw e;
                }
            }

            if (logger.isDebugEnabled()) {
                if (lastKnownCount < numDocs) {
                    logger.debug("[{}] docs indexed. waiting for [{}]", lastKnownCount, numDocs);
                } else {
                    logger.debug("[{}] docs visible for search (needed [{}])", lastKnownCount, numDocs);
                }
            }

            assertThat(lastKnownCount, greaterThanOrEqualTo(numDocs));
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Waits until at least a give number of document is indexed by indexer
     *
     * @param numDocs number of documents to wait for
     * @param indexer a {@link BackgroundIndexer}. It will be first checked for documents indexed.
     *                This saves on unneeded searches.
     */
    public void waitForIndexed(final long numDocs, final BackgroundIndexer indexer) throws Exception {
        // indexing threads can wait for up to ~1m before retrying when they first try to index into a shard which is not STARTED.
        final long maxWaitTimeMs = Math.max(90 * 1000, 200 * numDocs);

        assertBusy(() -> {
            long lastKnownCount = indexer.totalIndexedDocs();
            assertThat(lastKnownCount, greaterThanOrEqualTo(numDocs));
        }, maxWaitTimeMs, TimeUnit.MILLISECONDS);
    }

    /**
     * Prints the current cluster state as debug logging.
     */
    public void logClusterState() {
        logger.debug(
            "cluster state:\n{}\n{}",
            client().admin().cluster().prepareState().get().getState(),
            client().admin().cluster().preparePendingClusterTasks().get()
        );
    }

    protected void ensureClusterSizeConsistency() {
        if (cluster() != null && cluster().size() > 0) { // if static init fails the cluster can be null
            logger.trace("Check consistency for [{}] nodes", cluster().size());
            assertNoTimeout(client().admin().cluster().prepareHealth().setWaitForNodes(Integer.toString(cluster().size())).get());
        }
    }

    /**
     * Verifies that all nodes that have the same version of the cluster state as cluster-manager have same cluster state
     */
    protected void ensureClusterStateConsistency() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            final NamedWriteableRegistry namedWriteableRegistry = cluster().getNamedWriteableRegistry();
            final Client clusterManagerClient = client();
            ClusterState clusterManagerClusterState = clusterManagerClient.admin().cluster().prepareState().all().get().getState();
            byte[] masterClusterStateBytes = ClusterState.Builder.toBytes(clusterManagerClusterState);
            // remove local node reference
            clusterManagerClusterState = ClusterState.Builder.fromBytes(masterClusterStateBytes, null, namedWriteableRegistry);
            Map<String, Object> clusterManagerStateMap = convertToMap(clusterManagerClusterState);
            int clusterManagerClusterStateSize = clusterManagerClusterState.toString().length();
            String clusterManagerId = clusterManagerClusterState.nodes().getClusterManagerNodeId();
            for (Client client : cluster().getClients()) {
                ClusterState localClusterState = client.admin().cluster().prepareState().all().setLocal(true).get().getState();
                byte[] localClusterStateBytes = ClusterState.Builder.toBytes(localClusterState);
                // remove local node reference
                localClusterState = ClusterState.Builder.fromBytes(localClusterStateBytes, null, namedWriteableRegistry);
                final Map<String, Object> localStateMap = convertToMap(localClusterState);
                final int localClusterStateSize = localClusterState.toString().length();
                // Check that the non-cluster-manager node has the same version of the cluster state as the cluster-manager and
                // that the cluster-manager node matches the cluster-manager (otherwise there is no requirement for the cluster state to
                // match)
                if (clusterManagerClusterState.version() == localClusterState.version()
                    && clusterManagerId.equals(localClusterState.nodes().getClusterManagerNodeId())) {
                    try {
                        assertEquals(
                            "cluster state UUID does not match",
                            clusterManagerClusterState.stateUUID(),
                            localClusterState.stateUUID()
                        );
                        // We cannot compare serialization bytes since serialization order of maps is not guaranteed
                        // We also cannot compare byte array size because CompressedXContent's DeflateCompressor uses
                        // a synced flush that can affect the size of the compressed byte array
                        // (see: DeflateCompressedXContentTests#testDifferentCompressedRepresentation for an example)
                        // instead we compare the string length of cluster state - they should be the same
                        assertEquals("cluster state size does not match", clusterManagerClusterStateSize, localClusterStateSize);
                        // Compare JSON serialization
                        assertNull(
                            "cluster state JSON serialization does not match",
                            differenceBetweenMapsIgnoringArrayOrder(clusterManagerStateMap, localStateMap)
                        );
                    } catch (final AssertionError error) {
                        logger.error(
                            "Cluster state from cluster-manager:\n{}\nLocal cluster state:\n{}",
                            clusterManagerClusterState.toString(),
                            localClusterState.toString()
                        );
                        throw error;
                    }
                }
            }
        }

    }

    protected void ensureClusterStateCanBeReadByNodeTool() throws IOException {
        if (cluster() != null && cluster().size() > 0) {
            final Client clusterManagerClient = client();
            Metadata metadata = clusterManagerClient.admin().cluster().prepareState().all().get().getState().metadata();
            final Map<String, String> serializationParams = new HashMap<>(2);
            serializationParams.put("binary", "true");
            serializationParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
            final ToXContent.Params serializationFormatParams = new ToXContent.MapParams(serializationParams);

            // when comparing XContent output, do not use binary format
            final Map<String, String> compareParams = new HashMap<>(2);
            compareParams.put(Metadata.CONTEXT_MODE_PARAM, Metadata.CONTEXT_MODE_GATEWAY);
            final ToXContent.Params compareFormatParams = new ToXContent.MapParams(compareParams);

            {
                Metadata metadataWithoutIndices = Metadata.builder(metadata).removeAllIndices().build();

                XContentBuilder builder = SmileXContent.contentBuilder();
                builder.startObject();
                metadataWithoutIndices.toXContent(builder, serializationFormatParams);
                builder.endObject();
                final BytesReference originalBytes = BytesReference.bytes(builder);

                XContentBuilder compareBuilder = SmileXContent.contentBuilder();
                compareBuilder.startObject();
                metadataWithoutIndices.toXContent(compareBuilder, compareFormatParams);
                compareBuilder.endObject();
                final BytesReference compareOriginalBytes = BytesReference.bytes(compareBuilder);

                final Metadata loadedMetadata;
                try (
                    XContentParser parser = createParser(
                        OpenSearchNodeCommand.namedXContentRegistry,
                        SmileXContent.smileXContent,
                        originalBytes
                    )
                ) {
                    loadedMetadata = Metadata.fromXContent(parser);
                }
                builder = SmileXContent.contentBuilder();
                builder.startObject();
                loadedMetadata.toXContent(builder, compareFormatParams);
                builder.endObject();
                final BytesReference parsedBytes = BytesReference.bytes(builder);

                assertNull(
                    "cluster state XContent serialization does not match, expected "
                        + XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE)
                        + " but got "
                        + XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE),
                    differenceBetweenMapsIgnoringArrayOrder(
                        XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE).v2(),
                        XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE).v2()
                    )
                );
            }

            for (IndexMetadata indexMetadata : metadata) {
                XContentBuilder builder = SmileXContent.contentBuilder();
                builder.startObject();
                indexMetadata.toXContent(builder, serializationFormatParams);
                builder.endObject();
                final BytesReference originalBytes = BytesReference.bytes(builder);

                XContentBuilder compareBuilder = SmileXContent.contentBuilder();
                compareBuilder.startObject();
                indexMetadata.toXContent(compareBuilder, compareFormatParams);
                compareBuilder.endObject();
                final BytesReference compareOriginalBytes = BytesReference.bytes(compareBuilder);

                final IndexMetadata loadedIndexMetadata;
                try (
                    XContentParser parser = createParser(
                        OpenSearchNodeCommand.namedXContentRegistry,
                        SmileXContent.smileXContent,
                        originalBytes
                    )
                ) {
                    loadedIndexMetadata = IndexMetadata.fromXContent(parser);
                }
                builder = SmileXContent.contentBuilder();
                builder.startObject();
                loadedIndexMetadata.toXContent(builder, compareFormatParams);
                builder.endObject();
                final BytesReference parsedBytes = BytesReference.bytes(builder);

                assertNull(
                    "cluster state XContent serialization does not match, expected "
                        + XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE)
                        + " but got "
                        + XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE),
                    differenceBetweenMapsIgnoringArrayOrder(
                        XContentHelper.convertToMap(compareOriginalBytes, false, XContentType.SMILE).v2(),
                        XContentHelper.convertToMap(parsedBytes, false, XContentType.SMILE).v2()
                    )
                );
            }
        }
    }

    /**
     * Ensures the cluster is in a searchable state for the given indices. This means a searchable copy of each
     * shard is available on the cluster.
     */
    protected ClusterHealthStatus ensureSearchable(String... indices) {
        // this is just a temporary thing but it's easier to change if it is encapsulated.
        return ensureGreen(indices);
    }

    protected void ensureStableCluster(int nodeCount) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30));
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue) {
        ensureStableCluster(nodeCount, timeValue, false, null);
    }

    protected void ensureStableCluster(int nodeCount, @Nullable String viaNode) {
        ensureStableCluster(nodeCount, TimeValue.timeValueSeconds(30), false, viaNode);
    }

    protected void ensureStableCluster(int nodeCount, TimeValue timeValue, boolean local, @Nullable String viaNode) {
        if (viaNode == null) {
            viaNode = randomFrom(internalCluster().getNodeNames());
        }
        logger.debug("ensuring cluster is stable with [{}] nodes. access node: [{}]. timeout: [{}]", nodeCount, viaNode, timeValue);
        ClusterHealthResponse clusterHealthResponse = client(viaNode).admin()
            .cluster()
            .prepareHealth()
            .setWaitForEvents(Priority.LANGUID)
            .setWaitForNodes(Integer.toString(nodeCount))
            .setTimeout(timeValue)
            .setLocal(local)
            .setWaitForNoRelocatingShards(true)
            .get();
        if (clusterHealthResponse.isTimedOut()) {
            ClusterStateResponse stateResponse = client(viaNode).admin().cluster().prepareState().get();
            fail(
                "failed to reach a stable cluster of ["
                    + nodeCount
                    + "] nodes. Tried via ["
                    + viaNode
                    + "]. last cluster state:\n"
                    + stateResponse.getState()
            );
        }
        assertThat(clusterHealthResponse.isTimedOut(), is(false));
        ensureFullyConnectedCluster();
    }

    /**
     * Ensures that all nodes in the cluster are connected to each other.
     * <p>
     * Some network disruptions may leave nodes that are not the cluster-manager disconnected from each other.
     * {@link org.opensearch.cluster.NodeConnectionsService} will eventually reconnect but it's
     * handy to be able to ensure this happens faster
     */
    protected void ensureFullyConnectedCluster() {
        NetworkDisruption.ensureFullyConnectedCluster(internalCluster());
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    @Deprecated
    protected final IndexResponse index(String index, String type, XContentBuilder source) {
        return client().prepareIndex(index).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   client().prepareIndex(index, type).setSource(source).execute().actionGet();
     * </pre>
     */
    protected final IndexResponse index(String index, String type, String id, Map<String, Object> source) {
        return client().prepareIndex(index).setId(id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    @Deprecated
    protected final IndexResponse index(String index, String type, String id, XContentBuilder source) {
        return client().prepareIndex(index).setId(id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     */
    @Deprecated
    protected final IndexResponse index(String index, String type, String id, Object... source) {
        return client().prepareIndex(index).setId(id).setSource(source).execute().actionGet();
    }

    /**
     * Syntactic sugar for:
     * <pre>
     *   return client().prepareIndex(index, type, id).setSource(source).execute().actionGet();
     * </pre>
     * <p>
     * where source is a JSON String.
     */
    @Deprecated
    protected final IndexResponse index(String index, String type, String id, String source) {
        return client().prepareIndex(index).setId(id).setSource(source, MediaTypeRegistry.JSON).execute().actionGet();
    }

    /**
     * Waits for relocations and refreshes all indices in the cluster.
     *
     * @see #waitForRelocation()
     */
    protected final RefreshResponse refresh(String... indices) {
        waitForRelocation();
        // TODO RANDOMIZE with flush?
        RefreshResponse actionGet = client().admin()
            .indices()
            .prepareRefresh(indices)
            .setIndicesOptions(IndicesOptions.STRICT_EXPAND_OPEN_HIDDEN_FORBID_CLOSED)
            .execute()
            .actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Flushes and refreshes all indices in the cluster
     */
    protected final void flushAndRefresh(String... indices) {
        flush(indices);
        refresh(indices);
    }

    /**
     * Flush some or all indices in the cluster.
     */
    protected final FlushResponse flush(String... indices) {
        waitForRelocation();
        FlushResponse actionGet = client().admin().indices().prepareFlush(indices).execute().actionGet();
        for (DefaultShardOperationFailedException failure : actionGet.getShardFailures()) {
            assertThat("unexpected flush failure " + failure.reason(), failure.status(), equalTo(RestStatus.SERVICE_UNAVAILABLE));
        }
        return actionGet;
    }

    /**
     * Waits for all relocations and force merge all indices in the cluster to 1 segment.
     */
    protected ForceMergeResponse forceMerge() {
        waitForRelocation();
        ForceMergeResponse actionGet = client().admin().indices().prepareForceMerge().setMaxNumSegments(1).execute().actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    protected ForceMergeResponse forceMerge(int maxNumSegments) {
        waitForRelocation();
        ForceMergeResponse actionGet = client().admin()
            .indices()
            .prepareForceMerge()
            .setMaxNumSegments(maxNumSegments)
            .execute()
            .actionGet();
        assertNoFailures(actionGet);
        return actionGet;
    }

    /**
     * Returns <code>true</code> iff the given index exists otherwise <code>false</code>
     */
    protected static boolean indexExists(String index) {
        IndicesExistsResponse actionGet = client().admin().indices().prepareExists(index).execute().actionGet();
        return actionGet.isExists();
    }

    /**
     * Syntactic sugar for enabling allocation for <code>indices</code>
     */
    protected final void enableAllocation(String... indices) {
        client().admin()
            .indices()
            .prepareUpdateSettings(indices)
            .setSettings(Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "all"))
            .get();
    }

    /**
     * Syntactic sugar for disabling allocation for <code>indices</code>
     */
    protected final void disableAllocation(String... indices) {
        client().admin()
            .indices()
            .prepareUpdateSettings(indices)
            .setSettings(Settings.builder().put(EnableAllocationDecider.INDEX_ROUTING_ALLOCATION_ENABLE_SETTING.getKey(), "none"))
            .get();
    }

    /**
     * Returns a random admin client. This client can be a node pointing to any of the nodes in the cluster.
     */
    protected AdminClient admin() {
        return client().admin();
    }

    /**
     * Returns a random cluster admin client. This client can be pointing to any of the nodes in the cluster.
     */
    protected ClusterAdminClient clusterAdmin() {
        return admin().cluster();
    }

    /**
     * Convenience method that forwards to {@link #indexRandom(boolean, List)}.
     */
    public void indexRandom(boolean forceRefresh, IndexRequestBuilder... builders) throws InterruptedException {
        indexRandom(forceRefresh, Arrays.asList(builders));
    }

    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, IndexRequestBuilder... builders) throws InterruptedException {
        indexRandom(forceRefresh, dummyDocuments, Arrays.asList(builders));
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh if {@code true} all involved indices are refreshed
     *   once the documents are indexed. Additionally if {@code true} some
     *   empty dummy documents are may be randomly inserted into the document
     *   list and deleted once all documents are indexed. This is useful to
     *   produce deleted documents on the server side.
     * @param builders     the documents to index.
     * @see #indexRandom(boolean, boolean, java.util.List)
     */
    public void indexRandom(boolean forceRefresh, List<IndexRequestBuilder> builders) throws InterruptedException {
        indexRandom(forceRefresh, forceRefresh, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh   if {@code true} all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments if {@code true} some empty dummy documents may be randomly inserted into the document list and deleted once
     *                       all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param builders       the documents to index.
     */
    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, List<IndexRequestBuilder> builders) throws InterruptedException {
        indexRandom(forceRefresh, dummyDocuments, true, builders);
    }

    /**
     * Indexes the given {@link IndexRequestBuilder} instances randomly. It shuffles the given builders and either
     * indexes them in a blocking or async fashion. This is very useful to catch problems that relate to internal document
     * ids or index segment creations. Some features might have bug when a given document is the first or the last in a
     * segment or if only one document is in a segment etc. This method prevents issues like this by randomizing the index
     * layout.
     *
     * @param forceRefresh   if {@code true} all involved indices are refreshed once the documents are indexed.
     * @param dummyDocuments if {@code true} some empty dummy documents may be randomly inserted into the document list and deleted once
     *                       all documents are indexed. This is useful to produce deleted documents on the server side.
     * @param maybeFlush     if {@code true} this method may randomly execute full flushes after index operations.
     * @param builders       the documents to index.
     */
    public void indexRandom(boolean forceRefresh, boolean dummyDocuments, boolean maybeFlush, List<IndexRequestBuilder> builders)
        throws InterruptedException {
        Random random = random();
        Set<String> indices = new HashSet<>();
        for (IndexRequestBuilder builder : builders) {
            indices.add(builder.request().index());
        }
        Set<List<String>> bogusIds = new HashSet<>(); // (index, type, id)
        if (random.nextBoolean() && !builders.isEmpty() && dummyDocuments) {
            builders = new ArrayList<>(builders);
            // inject some bogus docs
            final int numBogusDocs = scaledRandomIntBetween(1, builders.size() * 2);
            final int unicodeLen = between(1, 10);
            for (int i = 0; i < numBogusDocs; i++) {
                String id = "bogus_doc_" + randomRealisticUnicodeOfLength(unicodeLen) + dummmyDocIdGenerator.incrementAndGet();
                String index = RandomPicks.randomFrom(random, indices);
                bogusIds.add(Arrays.asList(index, id));
                // We configure a routing key in case the mapping requires it
                builders.add(client().prepareIndex().setIndex(index).setId(id).setSource("{}", MediaTypeRegistry.JSON).setRouting(id));
            }
        }
        Collections.shuffle(builders, random());
        final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Exception>> errors = new CopyOnWriteArrayList<>();
        List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
        // If you are indexing just a few documents then frequently do it one at a time. If many then frequently in bulk.
        final String[] indicesArray = indices.toArray(new String[] {});
        if (builders.size() < FREQUENT_BULK_THRESHOLD ? frequently() : builders.size() < ALWAYS_BULK_THRESHOLD ? rarely() : false) {
            if (frequently()) {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), true, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute(
                        new PayloadLatchedActionListener<>(indexRequestBuilder, newLatch(inFlightAsyncOperations), errors)
                    );
                    postIndexAsyncActions(indicesArray, inFlightAsyncOperations, maybeFlush);
                }
            } else {
                logger.info("Index [{}] docs async: [{}] bulk: [{}]", builders.size(), false, false);
                for (IndexRequestBuilder indexRequestBuilder : builders) {
                    indexRequestBuilder.execute().actionGet();
                    postIndexAsyncActions(indicesArray, inFlightAsyncOperations, maybeFlush);
                }
            }
        } else {
            List<List<IndexRequestBuilder>> partition = eagerPartition(
                builders,
                Math.min(MAX_BULK_INDEX_REQUEST_SIZE, Math.max(1, (int) (builders.size() * randomDouble())))
            );
            logger.info("Index [{}] docs async: [{}] bulk: [{}] partitions [{}]", builders.size(), false, true, partition.size());
            for (List<IndexRequestBuilder> segmented : partition) {
                BulkRequestBuilder bulkBuilder = client().prepareBulk();
                for (IndexRequestBuilder indexRequestBuilder : segmented) {
                    indexRequestBuilder.setRefreshPolicy(WriteRequest.RefreshPolicy.NONE);
                    bulkBuilder.add(indexRequestBuilder);
                }
                BulkResponse actionGet = bulkBuilder.execute().actionGet();
                assertThat(actionGet.hasFailures() ? actionGet.buildFailureMessage() : "", actionGet.hasFailures(), equalTo(false));
            }
        }
        for (CountDownLatch operation : inFlightAsyncOperations) {
            operation.await();
        }
        final List<Exception> actualErrors = new ArrayList<>();
        for (Tuple<IndexRequestBuilder, Exception> tuple : errors) {
            Throwable t = ExceptionsHelper.unwrapCause(tuple.v2());
            if (t instanceof OpenSearchRejectedExecutionException) {
                logger.debug("Error indexing doc: " + t.getMessage() + ", reindexing.");
                tuple.v1().execute().actionGet(); // re-index if rejected
            } else {
                actualErrors.add(tuple.v2());
            }
        }
        assertThat(actualErrors, emptyIterable());

        if (!bogusIds.isEmpty()) {
            // delete the bogus types again - it might trigger merges or at least holes in the segments and enforces deleted docs!
            for (List<String> doc : bogusIds) {
                assertEquals(
                    "failed to delete a dummy doc [" + doc.get(0) + "][" + doc.get(1) + "]",
                    DocWriteResponse.Result.DELETED,
                    client().prepareDelete(doc.get(0), doc.get(1)).setRouting(doc.get(1)).get().getResult()
                );
            }
        }
        if (forceRefresh) {
            assertNoFailures(
                client().admin().indices().prepareRefresh(indicesArray).setIndicesOptions(IndicesOptions.lenientExpandOpen()).get()
            );
        }
        if (dummyDocuments) {
            indexRandomForMultipleSlices(indicesArray);
        }
        if (forceRefresh) {
            waitForReplication();
        }
    }

    /*
     * This method ingests bogus documents for the given indices such that multiple slices
     * are formed. This is useful for testing with the concurrent search use-case as it creates
     * multiple slices based on segment count.
     * @param indices         the indices in which bogus documents should be ingested
     * */
    protected void indexRandomForMultipleSlices(String... indices) throws InterruptedException {
        Set<List<String>> bogusIds = new HashSet<>();
        int refreshCount = randomIntBetween(2, 3);
        for (String index : indices) {
            int numDocs = getNumShards(index).totalNumShards * randomIntBetween(2, 10);
            while (refreshCount-- > 0) {
                final CopyOnWriteArrayList<Tuple<IndexRequestBuilder, Exception>> errors = new CopyOnWriteArrayList<>();
                List<CountDownLatch> inFlightAsyncOperations = new ArrayList<>();
                for (int i = 0; i < numDocs; i++) {
                    String id = "bogus_doc_" + randomRealisticUnicodeOfLength(between(1, 10)) + dummmyDocIdGenerator.incrementAndGet();
                    IndexRequestBuilder indexRequestBuilder = client().prepareIndex()
                        .setIndex(index)
                        .setId(id)
                        .setSource("{}", MediaTypeRegistry.JSON)
                        .setRouting(id);
                    indexRequestBuilder.execute(
                        new PayloadLatchedActionListener<>(indexRequestBuilder, newLatch(inFlightAsyncOperations), errors)
                    );
                    bogusIds.add(Arrays.asList(index, id));
                }
                for (CountDownLatch operation : inFlightAsyncOperations) {
                    operation.await();
                }
                final List<Exception> actualErrors = new ArrayList<>();
                for (Tuple<IndexRequestBuilder, Exception> tuple : errors) {
                    Throwable t = ExceptionsHelper.unwrapCause(tuple.v2());
                    if (t instanceof OpenSearchRejectedExecutionException) {
                        logger.debug("Error indexing doc: " + t.getMessage() + ", reindexing.");
                        tuple.v1().execute().actionGet(); // re-index if rejected
                    } else {
                        actualErrors.add(tuple.v2());
                    }
                }
                assertThat(actualErrors, emptyIterable());
                refresh(index);
            }
        }
        for (List<String> doc : bogusIds) {
            assertEquals(
                "failed to delete a dummy doc [" + doc.get(0) + "][" + doc.get(1) + "]",
                DocWriteResponse.Result.DELETED,
                client().prepareDelete(doc.get(0), doc.get(1)).setRouting(doc.get(1)).get().getResult()
            );
        }
        // refresh is called to make sure the bogus docs doesn't affect the search results
        refresh();
    }

    private final AtomicInteger dummmyDocIdGenerator = new AtomicInteger();

    /** Disables an index block for the specified index */
    public static void disableIndexBlock(String index, String block) {
        Settings settings = Settings.builder().put(block, false).build();
        client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
    }

    /** Enables an index block for the specified index */
    public static void enableIndexBlock(String index, String block) {
        if (IndexMetadata.APIBlock.fromSetting(block) == IndexMetadata.APIBlock.READ_ONLY_ALLOW_DELETE || randomBoolean()) {
            // the read-only-allow-delete block isn't supported by the add block API so we must use the update settings API here.
            Settings settings = Settings.builder().put(block, true).build();
            client().admin().indices().prepareUpdateSettings(index).setSettings(settings).get();
        } else {
            client().admin().indices().prepareAddBlock(IndexMetadata.APIBlock.fromSetting(block), index).get();
        }
    }

    /** Sets or unsets the cluster read_only mode **/
    public static void setClusterReadOnly(boolean value) {
        Settings settings = value
            ? Settings.builder().put(Metadata.SETTING_READ_ONLY_SETTING.getKey(), value).build()
            : Settings.builder().putNull(Metadata.SETTING_READ_ONLY_SETTING.getKey()).build();
        assertAcked(client().admin().cluster().prepareUpdateSettings().setTransientSettings(settings).get());
    }

    private static CountDownLatch newLatch(List<CountDownLatch> latches) {
        CountDownLatch l = new CountDownLatch(1);
        latches.add(l);
        return l;
    }

    /**
     * Maybe refresh, force merge, or flush then always make sure there aren't too many in flight async operations.
     */
    private void postIndexAsyncActions(String[] indices, List<CountDownLatch> inFlightAsyncOperations, boolean maybeFlush)
        throws InterruptedException {
        if (rarely()) {
            if (rarely()) {
                client().admin()
                    .indices()
                    .prepareRefresh(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
            } else if (maybeFlush && rarely()) {
                client().admin()
                    .indices()
                    .prepareFlush(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
            } else if (rarely()) {
                client().admin()
                    .indices()
                    .prepareForceMerge(indices)
                    .setIndicesOptions(IndicesOptions.lenientExpandOpen())
                    .setMaxNumSegments(between(1, 10))
                    .setFlush(maybeFlush && randomBoolean())
                    .execute(new LatchedActionListener<>(newLatch(inFlightAsyncOperations)));
            }
        }
        while (inFlightAsyncOperations.size() > MAX_IN_FLIGHT_ASYNC_INDEXES) {
            int waitFor = between(0, inFlightAsyncOperations.size() - 1);
            inFlightAsyncOperations.remove(waitFor).await();
        }
    }

    /**
     * The scope of a test cluster used together with
     * {@link OpenSearchIntegTestCase.ClusterScope} annotations on {@link OpenSearchIntegTestCase} subclasses.
     */
    public enum Scope {
        /**
         * A cluster shared across all method in a single test suite
         */
        SUITE,
        /**
         * A test exclusive test cluster
         */
        TEST
    }

    /**
     * Defines a cluster scope for a {@link OpenSearchIntegTestCase} subclass.
     * By default if no {@link ClusterScope} annotation is present {@link OpenSearchIntegTestCase.Scope#SUITE} is used
     * together with randomly chosen settings like number of nodes etc.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Target({ ElementType.TYPE })
    public @interface ClusterScope {
        /**
         * Returns the scope. {@link OpenSearchIntegTestCase.Scope#SUITE} is default.
         */
        Scope scope() default Scope.SUITE;

        /**
         * Returns the number of nodes in the cluster. Default is {@code -1} which means
         * a random number of nodes is used, where the minimum and maximum number of nodes
         * are either the specified ones or the default ones if not specified.
         */
        int numDataNodes() default -1;

        /**
         * Returns the minimum number of data nodes in the cluster. Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int minNumDataNodes() default -1;

        /**
         * Returns the maximum number of data nodes in the cluster.  Default is {@code -1}.
         * Ignored when {@link ClusterScope#numDataNodes()} is set.
         */
        int maxNumDataNodes() default -1;

        /**
         * Indicates whether the cluster can have dedicated cluster-manager nodes. If {@code false} means data nodes will serve as cluster-manager nodes
         * and there will be no dedicated cluster-manager (and data) nodes. Default is {@code false} which means
         * dedicated cluster-manager nodes will be randomly used.
         */
        boolean supportsDedicatedMasters() default true;

        /**
         * Indicates whether the cluster automatically manages cluster bootstrapping. If set to {@code false} then the
         * tests must manage these things explicitly.
         */
        boolean autoManageMasterNodes() default true;

        /**
         * Returns the number of client nodes in the cluster. Default is {@link InternalTestCluster#DEFAULT_NUM_CLIENT_NODES}, a
         * negative value means that the number of client nodes will be randomized.
         */
        int numClientNodes() default InternalTestCluster.DEFAULT_NUM_CLIENT_NODES;
    }

    private class LatchedActionListener<Response> implements ActionListener<Response> {
        private final CountDownLatch latch;

        LatchedActionListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public final void onResponse(Response response) {
            latch.countDown();
        }

        @Override
        public final void onFailure(Exception t) {
            try {
                logger.info("Action Failed", t);
                addError(t);
            } finally {
                latch.countDown();
            }
        }

        protected void addError(Exception e) {}

    }

    private class PayloadLatchedActionListener<Response, T> extends LatchedActionListener<Response> {
        private final CopyOnWriteArrayList<Tuple<T, Exception>> errors;
        private final T builder;

        PayloadLatchedActionListener(T builder, CountDownLatch latch, CopyOnWriteArrayList<Tuple<T, Exception>> errors) {
            super(latch);
            this.errors = errors;
            this.builder = builder;
        }

        @Override
        protected void addError(Exception e) {
            errors.add(new Tuple<>(builder, e));
        }

    }

    /**
     * Clears the given scroll Ids
     */
    public void clearScroll(String... scrollIds) {
        ClearScrollResponse clearResponse = client().prepareClearScroll().setScrollIds(Arrays.asList(scrollIds)).get();
        assertThat(clearResponse.isSucceeded(), equalTo(true));
    }

    static <A extends Annotation> A getAnnotation(Class<?> clazz, Class<A> annotationClass) {
        if (clazz == Object.class || clazz == OpenSearchIntegTestCase.class) {
            return null;
        }
        A annotation = clazz.getAnnotation(annotationClass);
        if (annotation != null) {
            return annotation;
        }
        return getAnnotation(clazz.getSuperclass(), annotationClass);
    }

    private boolean getSupportsDedicatedClusterManagers() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.supportsDedicatedMasters();
    }

    private boolean getAutoManageClusterManagerNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? true : annotation.autoManageMasterNodes();
    }

    private int getNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? -1 : annotation.numDataNodes();
    }

    private int getMinNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.minNumDataNodes() == -1
            ? InternalTestCluster.DEFAULT_MIN_NUM_DATA_NODES
            : annotation.minNumDataNodes();
    }

    private int getMaxNumDataNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null || annotation.maxNumDataNodes() == -1
            ? InternalTestCluster.DEFAULT_MAX_NUM_DATA_NODES
            : annotation.maxNumDataNodes();
    }

    private int getNumClientNodes() {
        ClusterScope annotation = getAnnotation(this.getClass(), ClusterScope.class);
        return annotation == null ? InternalTestCluster.DEFAULT_NUM_CLIENT_NODES : annotation.numClientNodes();
    }

    /**
     * This method is used to obtain settings for the {@code N}th node in the cluster.
     * Nodes in this cluster are associated with an ordinal number such that nodes can
     * be started with specific configurations. This method might be called multiple
     * times with the same ordinal and is expected to return the same value for each invocation.
     * In other words subclasses must ensure this method is idempotent.
     */
    protected Settings nodeSettings(int nodeOrdinal) {
        final Settings featureFlagSettings = featureFlagSettings();
        Settings.Builder builder = Settings.builder()
            // Default the watermarks to absurdly low to prevent the tests
            // from failing on nodes without enough disk space
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_LOW_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_HIGH_DISK_WATERMARK_SETTING.getKey(), "1b")
            .put(DiskThresholdSettings.CLUSTER_ROUTING_ALLOCATION_DISK_FLOOD_STAGE_WATERMARK_SETTING.getKey(), "1b")
            // by default we never cache below 10k docs in a segment,
            // bypass this limit so that caching gets some testing in
            // integration tests that usually create few documents
            .put(IndicesQueryCache.INDICES_QUERIES_CACHE_ALL_SEGMENTS_SETTING.getKey(), nodeOrdinal % 2 == 0)
            // wait short time for other active shards before actually deleting, default 30s not needed in tests
            .put(IndicesStore.INDICES_STORE_DELETE_SHARD_TIMEOUT.getKey(), new TimeValue(1, TimeUnit.SECONDS))
            // randomly enable low-level search cancellation to make sure it does not alter results
            .put(SearchService.LOW_LEVEL_CANCELLATION_SETTING.getKey(), randomBoolean())
            .putList(DISCOVERY_SEED_HOSTS_SETTING.getKey()) // empty list disables a port scan for other nodes
            .putList(DISCOVERY_SEED_PROVIDERS_SETTING.getKey(), "file")
            // By default, for tests we will put the target slice count of 2. This will increase the probability of having multiple slices
            // when tests are run with concurrent segment search enabled
            .put(SearchService.CONCURRENT_SEGMENT_SEARCH_TARGET_MAX_SLICE_COUNT_KEY, 2)
            .put(featureFlagSettings());

        // Enable tracer only when Telemetry Setting is enabled
        if (featureFlagSettings().getAsBoolean(FeatureFlags.TELEMETRY_SETTING.getKey(), false)) {
            builder.put(TelemetrySettings.TRACER_FEATURE_ENABLED_SETTING.getKey(), true);
            builder.put(TelemetrySettings.TRACER_ENABLED_SETTING.getKey(), true);
        }

        // Randomly set a Replication Strategy and storage type for the node. Both Replication Strategy and Storage Type can still be
        // manually overridden by subclass if needed.
        if (useRandomReplicationStrategy()) {
            if (randomReplicationType.equals(ReplicationType.SEGMENT) && randomStorageType.equals("REMOTE_STORE")) {
                logger.info("Randomly using Replication Strategy as {} and Storage Type as {}.", randomReplicationType, randomStorageType);
                if (remoteStoreRepositoryPath == null) {
                    remoteStoreRepositoryPath = randomRepoPath().toAbsolutePath();
                }
                builder.put(remoteStoreClusterSettings(REMOTE_BACKED_STORAGE_REPOSITORY_NAME, remoteStoreRepositoryPath));
            } else {
                logger.info("Randomly using Replication Strategy as {} and Storage Type as {}.", randomReplicationType, randomStorageType);
                builder.put(CLUSTER_REPLICATION_TYPE_SETTING.getKey(), randomReplicationType);
            }
        }
        return builder.build();
    }

    /**
     * Used for selecting random replication strategy, either DOCUMENT or SEGMENT.
     * This method must be overridden by subclass to use random replication strategy.
     * Should be used only on test classes where replication strategy is not critical for tests.
     */
    protected boolean useRandomReplicationStrategy() {
        return false;
    }

    protected Path nodeConfigPath(int nodeOrdinal) {
        return null;
    }

    /**
     * Returns a collection of plugins that should be loaded on each node.
     */
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.emptyList();
    }

    private ExternalTestCluster buildExternalCluster(String clusterAddresses, String clusterName) throws IOException {
        String[] stringAddresses = clusterAddresses.split(",");
        TransportAddress[] transportAddresses = new TransportAddress[stringAddresses.length];
        int i = 0;
        for (String stringAddress : stringAddresses) {
            URL url = new URL("http://" + stringAddress);
            InetAddress inetAddress = InetAddress.getByName(url.getHost());
            transportAddresses[i++] = new TransportAddress(new InetSocketAddress(inetAddress, url.getPort()));
        }
        return new ExternalTestCluster(
            createTempDir(),
            externalClusterClientSettings(),
            getClientWrapper(),
            clusterName,
            nodePlugins(),
            transportAddresses
        );
    }

    protected Settings externalClusterClientSettings() {
        return Settings.EMPTY;
    }

    protected boolean ignoreExternalCluster() {
        return false;
    }

    protected TestCluster buildTestCluster(Scope scope, long seed) throws IOException {
        if (useRandomReplicationStrategy()) {
            randomReplicationType = randomBoolean() ? ReplicationType.DOCUMENT : ReplicationType.SEGMENT;
            if (randomReplicationType.equals(ReplicationType.SEGMENT)) {
                randomStorageType = randomBoolean() ? "REMOTE_STORE" : "LOCAL";
            } else {
                randomStorageType = "LOCAL";
            }
        }
        String clusterAddresses = System.getProperty(TESTS_CLUSTER);
        if (Strings.hasLength(clusterAddresses) && ignoreExternalCluster() == false) {
            if (scope == Scope.TEST) {
                throw new IllegalArgumentException("Cannot run TEST scope test with " + TESTS_CLUSTER);
            }
            String clusterName = System.getProperty(TESTS_CLUSTER_NAME);
            if (Strings.isNullOrEmpty(clusterName)) {
                throw new IllegalArgumentException("Missing tests.clustername system property");
            }
            return buildExternalCluster(clusterAddresses, clusterName);
        }

        final String nodePrefix;
        switch (scope) {
            case TEST:
                nodePrefix = TEST_CLUSTER_NODE_PREFIX;
                break;
            case SUITE:
                nodePrefix = SUITE_CLUSTER_NODE_PREFIX;
                break;
            default:
                throw new OpenSearchException("Scope not supported: " + scope);
        }

        boolean supportsDedicatedClusterManagers = getSupportsDedicatedClusterManagers();
        int numDataNodes = getNumDataNodes();
        int minNumDataNodes;
        int maxNumDataNodes;
        if (numDataNodes >= 0) {
            minNumDataNodes = maxNumDataNodes = numDataNodes;
        } else {
            minNumDataNodes = getMinNumDataNodes();
            maxNumDataNodes = getMaxNumDataNodes();
        }
        Collection<Class<? extends Plugin>> mockPlugins = getMockPlugins();
        final NodeConfigurationSource nodeConfigurationSource = getNodeConfigSource();
        if (addMockTransportService()) {
            ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>(mockPlugins);
            // add both mock plugins - local and tcp if they are not there
            // we do this in case somebody overrides getMockPlugins and misses to call super
            if (mockPlugins.contains(getTestTransportPlugin()) == false) {
                mocks.add(getTestTransportPlugin());
            }
            mockPlugins = mocks;
        }
        return new InternalTestCluster(
            seed,
            createTempDir(),
            supportsDedicatedClusterManagers,
            getAutoManageClusterManagerNodes(),
            minNumDataNodes,
            maxNumDataNodes,
            InternalTestCluster.clusterName(scope.name(), seed) + "-cluster",
            nodeConfigurationSource,
            getNumClientNodes(),
            nodePrefix,
            mockPlugins,
            getClientWrapper(),
            forbidPrivateIndexSettings()
        );
    }

    private NodeConfigurationSource getNodeConfigSource() {
        Settings.Builder initialNodeSettings = Settings.builder();
        if (addMockTransportService()) {
            initialNodeSettings.put(NetworkModule.TRANSPORT_TYPE_KEY, getTestTransportType());
        }
        return new NodeConfigurationSource() {
            @Override
            public Settings nodeSettings(int nodeOrdinal) {
                return Settings.builder()
                    .put(initialNodeSettings.build())
                    .put(OpenSearchIntegTestCase.this.nodeSettings(nodeOrdinal))
                    .build();
            }

            @Override
            public Path nodeConfigPath(int nodeOrdinal) {
                return OpenSearchIntegTestCase.this.nodeConfigPath(nodeOrdinal);
            }

            @Override
            public Collection<Class<? extends Plugin>> nodePlugins() {
                return OpenSearchIntegTestCase.this.nodePlugins();
            }
        };
    }

    /**
     * Iff this returns true mock transport implementations are used for the test runs. Otherwise not mock transport impls are used.
     * The default is {@code true}.
     */
    protected boolean addMockTransportService() {
        return true;
    }

    protected boolean addMockIndexStorePlugin() {
        return true;
    }

    /** Returns {@code true} iff this test cluster should use a dummy http transport */
    protected boolean addMockHttpTransport() {
        return true;
    }

    /**
     * Returns {@code true} if this test cluster can use a mock internal engine. Defaults to true.
     */
    protected boolean addMockInternalEngine() {
        return true;
    }

    /** Returns {@code true} iff this test cluster should use a dummy geo_shape field mapper */
    protected boolean addMockGeoShapeFieldMapper() {
        return true;
    }

    /**
     * Returns {@code true} if this test cluster should have tracing enabled with MockTelemetryPlugin
     * Disabling this for now as the existing way of strict check do not support multiple nodes internal cluster.
     * @return boolean.
     */
    protected boolean addMockTelemetryPlugin() {
        return true;
    }

    /**
     * Returns a function that allows to wrap / filter all clients that are exposed by the test cluster. This is useful
     * for debugging or request / response pre and post processing. It also allows to intercept all calls done by the test
     * framework. By default this method returns an identity function {@link Function#identity()}.
     */
    protected Function<Client, Client> getClientWrapper() {
        return Function.identity();
    }

    /** Return the mock plugins the cluster should use */
    protected Collection<Class<? extends Plugin>> getMockPlugins() {
        final ArrayList<Class<? extends Plugin>> mocks = new ArrayList<>();
        if (MOCK_MODULES_ENABLED && randomBoolean()) { // sometimes run without those completely
            if (randomBoolean() && addMockTransportService()) {
                mocks.add(MockTransportService.TestPlugin.class);
            }
            if (randomBoolean() && addMockIndexStorePlugin()) {
                mocks.add(MockFSIndexStore.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(NodeMocksPlugin.class);
            }
            if (addMockInternalEngine() && randomBoolean()) {
                mocks.add(MockEngineFactoryPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockSearchService.TestPlugin.class);
            }
            if (randomBoolean()) {
                mocks.add(MockFieldFilterPlugin.class);
            }
        }
        if (addMockTransportService()) {
            mocks.add(getTestTransportPlugin());
        }
        if (addMockHttpTransport()) {
            mocks.add(MockHttpTransport.TestPlugin.class);
        }
        mocks.add(TestSeedPlugin.class);
        mocks.add(AssertActionNamePlugin.class);
        mocks.add(MockScriptService.TestPlugin.class);
        if (addMockGeoShapeFieldMapper()) {
            mocks.add(TestGeoShapeFieldMapperPlugin.class);
        }
        if (addMockTelemetryPlugin()) {
            mocks.add(MockTelemetryPlugin.class);
        }
        mocks.add(TestSystemTemplatesRepositoryPlugin.class);
        return Collections.unmodifiableList(mocks);
    }

    public static final class TestSeedPlugin extends Plugin {
        @Override
        public List<Setting<?>> getSettings() {
            return Collections.singletonList(INDEX_TEST_SEED_SETTING);
        }
    }

    public static final class AssertActionNamePlugin extends Plugin implements NetworkPlugin {
        @Override
        public List<TransportInterceptor> getTransportInterceptors(
            NamedWriteableRegistry namedWriteableRegistry,
            ThreadContext threadContext
        ) {
            return Arrays.asList(new TransportInterceptor() {
                @Override
                public <T extends TransportRequest> TransportRequestHandler<T> interceptHandler(
                    String action,
                    String executor,
                    boolean forceExecution,
                    TransportRequestHandler<T> actualHandler
                ) {
                    if (TransportService.isValidActionName(action) == false) {
                        throw new IllegalArgumentException(
                            "invalid action name [" + action + "] must start with one of: " + TransportService.VALID_ACTION_PREFIXES
                        );
                    }
                    return actualHandler;
                }
            });
        }
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public Path randomRepoPath() {
        return testClusterRule.internalCluster()
            .map(c -> randomRepoPath(c.getDefaultSettings()))
            .orElseThrow(() -> new UnsupportedOperationException("unsupported cluster type"));
    }

    /**
     * Returns path to a random directory that can be used to create a temporary file system repo
     */
    public static Path randomRepoPath(Settings settings) {
        Environment environment = TestEnvironment.newEnvironment(settings);
        Path[] repoFiles = environment.repoFiles();
        assert repoFiles.length > 0;
        Path path;
        do {
            path = repoFiles[0].resolve(randomAlphaOfLength(10));
        } while (Files.exists(path));
        return path;
    }

    protected NumShards getNumShards(String index) {
        Metadata metadata = client().admin().cluster().prepareState().get().getState().metadata();
        assertThat(metadata.hasIndex(index), equalTo(true));
        int numShards = Integer.valueOf(metadata.index(index).getSettings().get(SETTING_NUMBER_OF_SHARDS));
        int numReplicas = Integer.valueOf(metadata.index(index).getSettings().get(SETTING_NUMBER_OF_REPLICAS));
        return new NumShards(numShards, numReplicas);
    }

    /**
     * Asserts that all shards are allocated on nodes matching the given node pattern.
     */
    public Set<String> assertAllShardsOnNodes(String index, String... pattern) {
        Set<String> nodes = new HashSet<>();
        ClusterState clusterState = client().admin().cluster().prepareState().execute().actionGet().getState();
        for (IndexRoutingTable indexRoutingTable : clusterState.routingTable()) {
            for (IndexShardRoutingTable indexShardRoutingTable : indexRoutingTable) {
                for (ShardRouting shardRouting : indexShardRoutingTable) {
                    if (shardRouting.currentNodeId() != null && index.equals(shardRouting.getIndexName())) {
                        String name = clusterState.nodes().get(shardRouting.currentNodeId()).getName();
                        nodes.add(name);
                        assertThat("Allocated on new node: " + name, Regex.simpleMatch(pattern, name), is(true));
                    }
                }
            }
        }
        return nodes;
    }

    /**
     * Asserts that all segments are sorted with the provided {@link Sort}.
     */
    public void assertSortedSegments(String indexName, Sort expectedIndexSort) {
        IndicesSegmentResponse segmentResponse = client().admin().indices().prepareSegments(indexName).execute().actionGet();
        IndexSegments indexSegments = segmentResponse.getIndices().get(indexName);
        for (IndexShardSegments indexShardSegments : indexSegments.getShards().values()) {
            for (ShardSegments shardSegments : indexShardSegments.getShards()) {
                for (Segment segment : shardSegments) {
                    assertThat(expectedIndexSort, equalTo(segment.getSegmentSort()));
                }
            }
        }
    }

    protected static class NumShards {
        public final int numPrimaries;
        public final int numReplicas;
        public final int totalNumShards;
        public final int dataCopies;

        private NumShards(int numPrimaries, int numReplicas) {
            this.numPrimaries = numPrimaries;
            this.numReplicas = numReplicas;
            this.dataCopies = numReplicas + 1;
            this.totalNumShards = numPrimaries * dataCopies;
        }
    }

    @AfterClass
    public static void afterClass() throws Exception {
        testClusterRule.afterClass();
    }

    /**
     * Compute a routing key that will route documents to the <code>shard</code>-th shard
     * of the provided index.
     */
    protected String routingKeyForShard(String index, int shard) {
        return internalCluster().routingKeyForShard(resolveIndex(index), shard, random());
    }

    @Override
    protected NamedXContentRegistry xContentRegistry() {
        if (isInternalCluster() && cluster().size() > 0) {
            // If it's internal cluster - using existing registry in case plugin registered custom data
            return internalCluster().getInstance(NamedXContentRegistry.class);
        } else {
            // If it's external cluster - fall back to the standard set
            return new NamedXContentRegistry(ClusterModule.getNamedXWriteables());
        }
    }

    protected boolean forbidPrivateIndexSettings() {
        return true;
    }

    /**
     * Returns an instance of {@link RestClient} pointing to the current test cluster.
     * Creates a new client if the method is invoked for the first time in the context of the current test scope.
     * The returned client gets automatically closed when needed, it shouldn't be closed as part of tests otherwise
     * it cannot be reused by other tests anymore.
     */
    protected static RestClient getRestClient() {
        return testClusterRule.getRestClient();
    }

    /**
     * This method is executed iff the test is annotated with {@link SuiteScopeTestCase}
     * before the first test of this class is executed.
     *
     * @see SuiteScopeTestCase
     */
    protected void setupSuiteScopeCluster() throws Exception {}

    /**
     * If a test is annotated with {@link SuiteScopeTestCase}
     * the checks and modifications that are applied to the used test cluster are only done after all tests
     * of this class are executed. This also has the side-effect of a suite level setup method {@link #setupSuiteScopeCluster()}
     * that is executed in a separate test instance. Variables that need to be accessible across test instances must be static.
     */
    @Retention(RetentionPolicy.RUNTIME)
    @Inherited
    @Target(ElementType.TYPE)
    public @interface SuiteScopeTestCase {
    }

    public static Index resolveIndex(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        String uuid = getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_INDEX_UUID);
        return new Index(index, uuid);
    }

    public static String resolveCustomDataPath(String index) {
        GetIndexResponse getIndexResponse = client().admin().indices().prepareGetIndex().setIndices(index).get();
        assertTrue("index " + index + " not found", getIndexResponse.getSettings().containsKey(index));
        return getIndexResponse.getSettings().get(index).get(IndexMetadata.SETTING_DATA_PATH);
    }

    public static boolean inFipsJvm() {
        return Boolean.parseBoolean(System.getProperty(FIPS_SYSPROP));
    }

    /**
     * On Debian 8 the "memory" subsystem is not mounted by default
     * when cgroups are enabled, and this confuses many versions of
     * Java prior to Java 15.  Tests that rely on machine memory
     * being accurately determined will not work on such setups,
     * and can use this method for selective muting.
     * See https://github.com/elastic/elasticsearch/issues/67089
     * and https://github.com/elastic/elasticsearch/issues/66885
     */
    protected boolean willSufferDebian8MemoryProblem() {
        final NodesInfoResponse response = client().admin().cluster().prepareNodesInfo().execute().actionGet();
        final boolean anyDebian8Nodes = response.getNodes()
            .stream()
            .anyMatch(ni -> ni.getInfo(OsInfo.class).getPrettyName().equals("Debian GNU/Linux 8 (jessie)"));
        boolean java15Plus = Runtime.version().compareTo(Version.parse("15")) >= 0;
        return anyDebian8Nodes && java15Plus == false;
    }

    public void manageReplicaBalanceSetting(boolean apply) {
        Settings settings;
        if (apply) {
            settings = Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values", "a, b")
                .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
                .build();
        } else {
            settings = Settings.builder()
                .putNull(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey())
                .putNull(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values")
                .putNull(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey())
                .build();
        }
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    public void manageReplicaSettingForDefaultReplica(boolean apply) {
        Settings settings;
        if (apply) {
            settings = Settings.builder()
                .put(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey(), "zone")
                .put(
                    AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values",
                    "a, b, c"
                )
                .put(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey(), true)
                .put(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey(), 2)
                .build();
        } else {
            settings = Settings.builder()
                .putNull(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_ATTRIBUTE_SETTING.getKey())
                .putNull(AwarenessAllocationDecider.CLUSTER_ROUTING_ALLOCATION_AWARENESS_FORCE_GROUP_SETTING.getKey() + "zone.values")
                .putNull(AwarenessReplicaBalance.CLUSTER_ROUTING_ALLOCATION_AWARENESS_BALANCE_SETTING.getKey())
                .putNull(Metadata.DEFAULT_REPLICA_COUNT_SETTING.getKey())
                .build();
        }
        ClusterUpdateSettingsRequest updateSettingsRequest = new ClusterUpdateSettingsRequest();
        updateSettingsRequest.persistentSettings(settings);
        assertAcked(client().admin().cluster().updateSettings(updateSettingsRequest).actionGet());
    }

    protected String primaryNodeName(String indexName) {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(indexName).shard(0).primaryShard().currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }

    protected String primaryNodeName(String indexName, int shardId) {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(indexName).shard(shardId).primaryShard().currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }

    protected String replicaNodeName(String indexName) {
        ClusterState clusterState = client().admin().cluster().prepareState().get().getState();
        String nodeId = clusterState.getRoutingTable().index(indexName).shard(0).replicaShards().get(0).currentNodeId();
        return clusterState.getRoutingNodes().node(nodeId).node().getName();
    }

    protected ClusterState getClusterState() {
        return client(internalCluster().getClusterManagerName()).admin().cluster().prepareState().get().getState();
    }

    /**
     * Refreshes the indices in the cluster and waits until active/started replica shards
     * are caught up with primary shard only when Segment Replication is enabled.
     * This doesn't wait for inactive/non-started replica shards to become active/started.
     */
    protected RefreshResponse refreshAndWaitForReplication(String... indices) {
        RefreshResponse refreshResponse = refresh(indices);
        waitForReplication();
        return refreshResponse;
    }

    public boolean isMigratingToRemoteStore() {
        ClusterSettings clusterSettings = clusterService().getClusterSettings();
        boolean isMixedMode = clusterSettings.get(REMOTE_STORE_COMPATIBILITY_MODE_SETTING)
            .equals(RemoteStoreNodeService.CompatibilityMode.MIXED);
        boolean isRemoteStoreMigrationDirection = clusterSettings.get(MIGRATION_DIRECTION_SETTING)
            .equals(RemoteStoreNodeService.Direction.REMOTE_STORE);
        return (isMixedMode && isRemoteStoreMigrationDirection);
    }

    /**
     * Waits until active/started replica shards are caught up with primary shard only when Segment Replication is enabled.
     * This doesn't wait for inactive/non-started replica shards to become active/started.
     */
    protected void waitForReplication(String... indices) {
        if (indices.length == 0) {
            indices = getClusterState().routingTable().indicesRouting().keySet().toArray(String[]::new);
        }
        try {
            for (String index : indices) {
                if (isSegmentReplicationEnabledForIndex(index)) {
                    if (isInternalCluster()) {
                        IndexRoutingTable indexRoutingTable = getClusterState().routingTable().index(index);
                        if (indexRoutingTable != null) {
                            assertBusy(() -> {
                                for (IndexShardRoutingTable shardRoutingTable : indexRoutingTable) {
                                    final ShardRouting primaryRouting = shardRoutingTable.primaryShard();
                                    if (primaryRouting.state().toString().equals("STARTED")) {
                                        if (isSegmentReplicationEnabledForIndex(index)) {
                                            final List<ShardRouting> replicaRouting = shardRoutingTable.replicaShards();
                                            final IndexShard primaryShard = getIndexShard(primaryRouting, index);
                                            for (ShardRouting replica : replicaRouting) {
                                                if (replica.state().toString().equals("STARTED")) {
                                                    IndexShard replicaShard = getIndexShard(replica, index);
                                                    if (replicaShard.indexSettings().isSegRepEnabledOrRemoteNode()) {
                                                        assertEquals(
                                                            "replica shards haven't caught up with primary",
                                                            getLatestSegmentInfoVersion(primaryShard),
                                                            getLatestSegmentInfoVersion(replicaShard)
                                                        );
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }, 30, TimeUnit.SECONDS);
                        }
                    } else {
                        throw new IllegalStateException(
                            "Segment Replication is not supported for testing tests using External Test Cluster"
                        );
                    }
                }
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Checks if Segment Replication is enabled on Index.
     */
    protected boolean isSegmentReplicationEnabledForIndex(String index) {
        return clusterService().state().getMetadata().isSegmentReplicationEnabled(index) || isMigratingToRemoteStore();
    }

    protected IndexShard getIndexShard(ShardRouting routing, String indexName) {
        return getIndexShard(getClusterState().nodes().get(routing.currentNodeId()).getName(), routing.shardId(), indexName);
    }

    /**
     * Fetch IndexShard by shardId, multiple shards per node allowed.
     */
    protected IndexShard getIndexShard(String node, ShardId shardId, String indexName) {
        final Index index = resolveIndex(indexName);
        IndicesService indicesService = internalCluster().getInstance(IndicesService.class, node);
        IndexService indexService = indicesService.indexServiceSafe(index);
        final Optional<Integer> id = indexService.shardIds().stream().filter(sid -> sid.equals(shardId.id())).findFirst();
        return indexService.getShard(id.get());
    }

    /**
     * Fetch latest segment info snapshot version of an index.
     */
    protected long getLatestSegmentInfoVersion(IndexShard shard) {
        try (final GatedCloseable<SegmentInfos> snapshot = shard.getSegmentInfosSnapshot()) {
            return snapshot.get().version;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    protected void createRepository(String repoName, String type, Settings.Builder settings, String timeout) {
        logger.info("--> creating repository [{}] [{}]", repoName, type);
        putRepository(clusterAdmin(), repoName, type, timeout, settings);
    }

    protected void createRepository(String repoName, String type, Settings.Builder settings) {
        logger.info("--> creating repository [{}] [{}]", repoName, type);
        putRepository(clusterAdmin(), repoName, type, null, settings);
    }

    protected void updateRepository(String repoName, String type, Settings.Builder settings) {
        logger.info("--> updating repository [{}] [{}]", repoName, type);
        putRepository(clusterAdmin(), repoName, type, null, settings);
    }

    public Settings getNodeSettings() {
        InternalTestCluster internalTestCluster = internalCluster();
        ClusterService clusterService = internalTestCluster.getInstance(ClusterService.class, internalTestCluster.getClusterManagerName());
        return clusterService.getSettings();
    }

    public static void putRepository(ClusterAdminClient adminClient, String repoName, String type, Settings.Builder settings) {
        assertAcked(putRepositoryRequestBuilder(adminClient, repoName, type, true, settings, null, false));
    }

    public static void putRepository(
        ClusterAdminClient adminClient,
        String repoName,
        String type,
        String timeout,
        Settings.Builder settings
    ) {
        assertAcked(putRepositoryRequestBuilder(adminClient, repoName, type, true, settings, timeout, false));
    }

    public static void putRepository(
        ClusterAdminClient adminClient,
        String repoName,
        String type,
        boolean verify,
        Settings.Builder settings
    ) {
        assertAcked(putRepositoryRequestBuilder(adminClient, repoName, type, verify, settings, null, false));
    }

    public static void putRepositoryWithNoSettingOverrides(
        ClusterAdminClient adminClient,
        String repoName,
        String type,
        boolean verify,
        Settings.Builder settings
    ) {
        assertAcked(putRepositoryRequestBuilder(adminClient, repoName, type, verify, settings, null, true));
    }

    public static void putRepository(
        ClusterAdminClient adminClient,
        String repoName,
        String type,
        Settings.Builder settings,
        ActionListener<AcknowledgedResponse> listener
    ) {
        putRepositoryRequestBuilder(adminClient, repoName, type, true, settings, null, false).execute(listener);
    }

    public static PutRepositoryRequestBuilder putRepositoryRequestBuilder(
        ClusterAdminClient adminClient,
        String repoName,
        String type,
        boolean verify,
        Settings.Builder settings,
        String timeout,
        boolean finalSettings
    ) {
        PutRepositoryRequestBuilder builder = adminClient.preparePutRepository(repoName).setType(type).setVerify(verify);
        if (timeout != null) {
            builder.setTimeout(timeout);
        }
        if (finalSettings == false) {
            settings.put(BlobStoreRepository.SHARD_PATH_TYPE.getKey(), randomFrom(PathType.values()));
        }
        builder.setSettings(settings);
        return builder;
    }

    public static Settings remoteStoreClusterSettings(String name, Path path) {
        return remoteStoreClusterSettings(name, path, name, path);
    }

    public static Settings remoteStoreClusterSettings(
        String segmentRepoName,
        Path segmentRepoPath,
        String segmentRepoType,
        String translogRepoName,
        Path translogRepoPath,
        String translogRepoType
    ) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(
            buildRemoteStoreNodeAttributes(
                segmentRepoName,
                segmentRepoPath,
                segmentRepoType,
                translogRepoName,
                translogRepoPath,
                translogRepoType,
                false
            )
        );
        return settingsBuilder.build();
    }

    public static Settings remoteStoreClusterSettings(
        String segmentRepoName,
        Path segmentRepoPath,
        String segmentRepoType,
        String translogRepoName,
        Path translogRepoPath,
        String translogRepoType,
        String routingTableRepoName,
        Path routingTableRepoPath,
        String routingTableRepoType
    ) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(
            buildRemoteStoreNodeAttributes(
                segmentRepoName,
                segmentRepoPath,
                segmentRepoType,
                translogRepoName,
                translogRepoPath,
                translogRepoType,
                routingTableRepoName,
                routingTableRepoPath,
                routingTableRepoType,
                false
            )
        );
        return settingsBuilder.build();
    }

    public static Settings remoteStoreClusterSettings(
        String segmentRepoName,
        Path segmentRepoPath,
        String translogRepoName,
        Path translogRepoPath
    ) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(buildRemoteStoreNodeAttributes(segmentRepoName, segmentRepoPath, translogRepoName, translogRepoPath, false));
        return settingsBuilder.build();
    }

    public static Settings remoteStoreClusterSettings(
        String segmentRepoName,
        Path segmentRepoPath,
        String translogRepoName,
        Path translogRepoPath,
        String remoteRoutingTableRepoName,
        Path remoteRoutingTableRepoPath
    ) {
        Settings.Builder settingsBuilder = Settings.builder();
        settingsBuilder.put(
            buildRemoteStoreNodeAttributes(
                segmentRepoName,
                segmentRepoPath,
                translogRepoName,
                translogRepoPath,
                remoteRoutingTableRepoName,
                remoteRoutingTableRepoPath,
                false
            )
        );
        return settingsBuilder.build();
    }

    public static Settings buildRemoteStoreNodeAttributes(
        String segmentRepoName,
        Path segmentRepoPath,
        String translogRepoName,
        Path translogRepoPath,
        boolean withRateLimiterAttributes
    ) {
        return buildRemoteStoreNodeAttributes(
            segmentRepoName,
            segmentRepoPath,
            ReloadableFsRepository.TYPE,
            translogRepoName,
            translogRepoPath,
            ReloadableFsRepository.TYPE,
            withRateLimiterAttributes
        );
    }

    public static Settings buildRemoteStoreNodeAttributes(
        String segmentRepoName,
        Path segmentRepoPath,
        String translogRepoName,
        Path translogRepoPath,
        String remoteRoutingTableRepoName,
        Path remoteRoutingTableRepoPath,
        boolean withRateLimiterAttributes
    ) {
        return buildRemoteStoreNodeAttributes(
            segmentRepoName,
            segmentRepoPath,
            ReloadableFsRepository.TYPE,
            translogRepoName,
            translogRepoPath,
            ReloadableFsRepository.TYPE,
            remoteRoutingTableRepoName,
            remoteRoutingTableRepoPath,
            FsRepository.TYPE,
            withRateLimiterAttributes
        );
    }

    public static Settings buildRemoteStateNodeAttributes(String stateRepoName, Path stateRepoPath, String stateRepoType) {
        String stateRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            stateRepoName
        );
        String stateRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            stateRepoName
        );
        String prefixModeVerificationSuffix = BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey();
        Settings.Builder settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, stateRepoName)
            .put(stateRepoTypeAttributeKey, stateRepoType)
            .put(stateRepoSettingsAttributeKeyPrefix + "location", stateRepoPath)
            .put(stateRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable);
        return settings.build();
    }

    private static Settings buildRemoteStoreNodeAttributes(
        String segmentRepoName,
        Path segmentRepoPath,
        String segmentRepoType,
        String translogRepoName,
        Path translogRepoPath,
        String translogRepoType,
        boolean withRateLimiterAttributes
    ) {
        return buildRemoteStoreNodeAttributes(
            segmentRepoName,
            segmentRepoPath,
            segmentRepoType,
            translogRepoName,
            translogRepoPath,
            translogRepoType,
            null,
            null,
            null,
            withRateLimiterAttributes
        );
    }

    protected static Settings buildRemoteStoreNodeAttributes(
        String segmentRepoName,
        Path segmentRepoPath,
        String segmentRepoType,
        String translogRepoName,
        Path translogRepoPath,
        String translogRepoType,
        String routingTableRepoName,
        Path routingTableRepoPath,
        String routingTableRepoType,
        boolean withRateLimiterAttributes
    ) {
        String segmentRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            segmentRepoName
        );
        String segmentRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            segmentRepoName
        );
        String translogRepoTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            translogRepoName
        );
        String translogRepoSettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            translogRepoName
        );
        String routingTableRepoAttributeKey = null, routingTableRepoSettingsAttributeKeyPrefix = null;
        if (routingTableRepoName != null) {
            routingTableRepoAttributeKey = String.format(
                Locale.getDefault(),
                "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
                routingTableRepoName
            );
            routingTableRepoSettingsAttributeKeyPrefix = String.format(
                Locale.getDefault(),
                "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
                routingTableRepoName
            );
        }

        String prefixModeVerificationSuffix = BlobStoreRepository.PREFIX_MODE_VERIFICATION_SETTING.getKey();

        Settings.Builder settings = Settings.builder()
            .put("node.attr." + REMOTE_STORE_SEGMENT_REPOSITORY_NAME_ATTRIBUTE_KEY, segmentRepoName)
            .put(segmentRepoTypeAttributeKey, segmentRepoType)
            .put(segmentRepoSettingsAttributeKeyPrefix + "location", segmentRepoPath)
            .put(segmentRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable)
            .put("node.attr." + REMOTE_STORE_TRANSLOG_REPOSITORY_NAME_ATTRIBUTE_KEY, translogRepoName)
            .put(translogRepoTypeAttributeKey, translogRepoType)
            .put(translogRepoSettingsAttributeKeyPrefix + "location", translogRepoPath)
            .put(translogRepoSettingsAttributeKeyPrefix + prefixModeVerificationSuffix, prefixModeVerificationEnable)
            .put(buildRemoteStateNodeAttributes(segmentRepoName, segmentRepoPath, segmentRepoType));
        if (routingTableRepoName != null) {
            settings.put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, routingTableRepoName)
                .put(routingTableRepoAttributeKey, routingTableRepoType)
                .put(routingTableRepoSettingsAttributeKeyPrefix + "location", routingTableRepoPath);
        }

        if (withRateLimiterAttributes) {
            settings.put(segmentRepoSettingsAttributeKeyPrefix + "compress", randomBoolean())
                .put(segmentRepoSettingsAttributeKeyPrefix + "chunk_size", 200, ByteSizeUnit.BYTES);
        }
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PATH_TYPE_SETTING.getKey(), randomFrom(PathType.values()));
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_METADATA.getKey(), randomBoolean());
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_PINNED_TIMESTAMP_ENABLED.getKey(), randomBoolean());
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_SEGMENTS_PATH_PREFIX.getKey(), translogPathFixedPrefix ? "a" : "");
        settings.put(RemoteStoreSettings.CLUSTER_REMOTE_STORE_TRANSLOG_PATH_PREFIX.getKey(), segmentsPathFixedPrefix ? "b" : "");
        settings.put(BlobStoreRepository.SNAPSHOT_SHARD_PATH_PREFIX_SETTING.getKey(), snapshotShardPathFixedPrefix ? "c" : "");
        return settings.build();
    }

    protected Settings buildRemotePublicationNodeAttributes(
        @NonNull String remoteStateRepoName,
        @NonNull String remoteStateRepoType,
        @NonNull String routingTableRepoName,
        @NonNull String routingTableRepoType
    ) {
        String remoteStateRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            remoteStateRepoName
        );
        String routingTableRepositoryTypeAttributeKey = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_TYPE_ATTRIBUTE_KEY_FORMAT,
            routingTableRepoName
        );
        String remoteStateRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            remoteStateRepoName
        );
        String routingTableRepositorySettingsAttributeKeyPrefix = String.format(
            Locale.getDefault(),
            "node.attr." + REMOTE_STORE_REPOSITORY_SETTINGS_ATTRIBUTE_KEY_PREFIX,
            routingTableRepoName
        );

        return Settings.builder()
            .put("node.attr." + REMOTE_STORE_CLUSTER_STATE_REPOSITORY_NAME_ATTRIBUTE_KEY, remoteStateRepoName)
            .put("node.attr." + REMOTE_STORE_ROUTING_TABLE_REPOSITORY_NAME_ATTRIBUTE_KEY, routingTableRepoName)
            .put(remoteStateRepositoryTypeAttributeKey, remoteStateRepoType)
            .put(routingTableRepositoryTypeAttributeKey, routingTableRepoType)
            .put(remoteStateRepositorySettingsAttributeKeyPrefix + "location", randomRepoPath().toAbsolutePath())
            .put(routingTableRepositorySettingsAttributeKeyPrefix + "location", randomRepoPath().toAbsolutePath())
            .build();
    }

    public static String resolvePath(IndexId indexId, String shardId) {
        PathType pathType = PathType.fromCode(indexId.getShardPathType());
        RemoteStorePathStrategy.SnapshotShardPathInput shardPathInput = new RemoteStorePathStrategy.SnapshotShardPathInput.Builder()
            .basePath(BlobPath.cleanPath())
            .indexUUID(indexId.getId())
            .shardId(shardId)
            .build();
        RemoteStoreEnums.PathHashAlgorithm pathHashAlgorithm = pathType != PathType.FIXED ? FNV_1A_COMPOSITE_1 : null;
        BlobPath blobPath = pathType.path(shardPathInput, pathHashAlgorithm);
        return blobPath.buildAsString();
    }
}
