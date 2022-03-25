/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sweeper;

import org.opensearch.jobscheduler.JobSchedulerSettings;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobParser;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.apache.lucene.index.Term;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.action.ActionFuture;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.OpenSearchAllocationTestCase;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.IndexRoutingTable;
import org.opensearch.cluster.routing.RoutingTable;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.allocation.AllocationService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.index.Index;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.mapper.ParseContext;
import org.opensearch.index.mapper.ParsedDocument;
import org.opensearch.index.shard.ShardId;
import org.opensearch.test.ClusterServiceUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;
import org.mockito.Mockito;
import org.mockito.stubbing.OngoingStubbing;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

@SuppressWarnings({"unchecked", "rawtypes"})
public class JobSweeperTests extends OpenSearchAllocationTestCase {

    private Client client;
    private ClusterService clusterService;
    private NamedXContentRegistry xContentRegistry;
    private ThreadPool threadPool;
    private JobScheduler scheduler;
    private Settings settings;
    private ScheduledJobParser jobParser;
    private ScheduledJobRunner jobRunner;

    private JobSweeper sweeper;

    private DiscoveryNode discoveryNode;

    private Double jitterLimit = 0.95;

    @Before
    public void setup() throws IOException {
        this.client = Mockito.mock(Client.class);
        this.threadPool = Mockito.mock(ThreadPool.class);
        this.scheduler = Mockito.mock(JobScheduler.class);
        this.jobRunner = Mockito.mock(ScheduledJobRunner.class);
        this.jobParser = Mockito.mock(ScheduledJobParser.class);

        // NamedXContentRegistry.Entry xContentRegistryEntry = new NamedXContentRegistry.Entry(ScheduledJobParameter.class,
        //         new ParseField("JOB_TYPE"), this.jobParser);
        List<NamedXContentRegistry.Entry> namedXContentRegistryEntries = new ArrayList<>();
        // namedXContentRegistryEntries.add(xContentRegistryEntry);
        this.xContentRegistry = new NamedXContentRegistry(namedXContentRegistryEntries);

        this.settings = Settings.builder().build();

        this.discoveryNode = new DiscoveryNode("node", OpenSearchTestCase.buildNewFakeTransportAddress(), Version.CURRENT);

        Set<Setting<?>> settingSet = new HashSet<>();
        settingSet.addAll(ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        settingSet.add(JobSchedulerSettings.REQUEST_TIMEOUT);
        settingSet.add(JobSchedulerSettings.SWEEP_PERIOD);
        settingSet.add(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT);
        settingSet.add(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS);
        settingSet.add(JobSchedulerSettings.SWEEP_PAGE_SIZE);
        settingSet.add(JobSchedulerSettings.JITTER_LIMIT);

        ClusterSettings clusterSettings = new ClusterSettings(this.settings, settingSet);
        ClusterService originClusterService = ClusterServiceUtils.createClusterService(this.threadPool, discoveryNode, clusterSettings);
        this.clusterService = Mockito.spy(originClusterService);

        ScheduledJobProvider jobProvider = new ScheduledJobProvider("JOB_TYPE", "job-index-name",
                this.jobParser, this.jobRunner);
        Map<String, ScheduledJobProvider> jobProviderMap = new HashMap<>();
        jobProviderMap.put("index-name", jobProvider);

        sweeper = new JobSweeper(settings, this.client, this.clusterService, this.threadPool, xContentRegistry,
                jobProviderMap, scheduler, new LockService(client, clusterService));
    }

    public void testAfterStart() {
        this.sweeper.afterStart();
        Mockito.verify(this.threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testInitBackgroundSweep() {
        Scheduler.Cancellable cancellable = Mockito.mock(Scheduler.Cancellable.class);
        Mockito.when(this.threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);

        this.sweeper.initBackgroundSweep();
        Mockito.verify(this.threadPool).scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());

        this.sweeper.initBackgroundSweep();
        Mockito.verify(cancellable).cancel();
        Mockito.verify(this.threadPool, Mockito.times(2))
                .scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString());
    }

    public void testBeforeStop() {
        Scheduler.Cancellable cancellable = Mockito.mock(Scheduler.Cancellable.class);

        this.sweeper.beforeStop();
        Mockito.verify(cancellable, Mockito.times(0)).cancel();

        Mockito.when(this.threadPool.scheduleWithFixedDelay(Mockito.any(), Mockito.any(), Mockito.anyString())).thenReturn(cancellable);
        this.sweeper.initBackgroundSweep();
        this.sweeper.beforeStop();
        Mockito.verify(cancellable).cancel();
    }

    public void testBeforeClose() {
        this.sweeper.beforeClose(); // nothing to verify
    }

    public void testPostIndex() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Index index = this.getIndexOperation();
        Engine.IndexResult indexResult = new Engine.IndexResult(1L, 1L, 1L, true);

        Metadata metadata = Metadata.builder()
                .put(createIndexMetadata("index-name", 1, 3))
                .build();
        RoutingTable routingTable = new RoutingTable.Builder()
                .add(new IndexRoutingTable.Builder(metadata.index("index-name").getIndex())
                        .initializeAsNew(metadata.index("index-name")).build())
                .build();
        ClusterState clusterState = ClusterState.builder(new ClusterName("cluster-name"))
                .metadata(metadata)
                .routingTable(routingTable)
                .build();

        clusterState = this.addNodesToCluter(clusterState, 2);
        clusterState = this.initializeAllShards(clusterState);

        OngoingStubbing stubbing = null;
        Iterator<DiscoveryNode> iter = clusterState.getNodes().iterator();
        while(iter.hasNext()) {
            if(stubbing == null) {
                stubbing = Mockito.when(this.clusterService.localNode()).thenReturn(iter.next());
            }else {
                stubbing = stubbing.thenReturn(iter.next());
            }
        }

        Mockito.when(this.clusterService.state()).thenReturn(clusterState);
        JobSweeper testSweeper = Mockito.spy(this.sweeper);
        Mockito.doNothing().when(testSweeper).sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class),
                Mockito.any(JobDocVersion.class));
        for (int i = 0; i<clusterState.getNodes().getSize(); i++) {
            testSweeper.postIndex(shardId, index, indexResult);
        }

        Mockito.verify(testSweeper).sweep(Mockito.any(), Mockito.anyString(), Mockito.any(BytesReference.class),
                Mockito.any(JobDocVersion.class));
    }

    public void testPostIndex_indexFailed() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Index index = this.getIndexOperation();
        Engine.IndexResult indexResult = new Engine.IndexResult(new IOException("exception"), 1L);

        this.sweeper.postIndex(shardId, index, indexResult);

        Mockito.verify(this.clusterService, Mockito.times(0)).localNode();
    }

    public void testPostDelete() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Delete delete = this.getDeleteOperation("doc-id");
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(1L, 1L, 1L, true);

        Set<String> jobIdSet = new HashSet<>();
        jobIdSet.add("doc-id");
        Mockito.when(this.scheduler.getScheduledJobIds("index-name")).thenReturn(jobIdSet);

        ActionFuture<DeleteResponse> actionFuture = Mockito.mock(ActionFuture.class);
        Mockito.when(this.client.delete(Mockito.any())).thenReturn(actionFuture);
        DeleteResponse response = new DeleteResponse(new ShardId(new Index("name","uuid"), 0), "id", 1L, 2L, 3L, true);
        Mockito.when(actionFuture.actionGet()).thenReturn(response);

        this.sweeper.postDelete(shardId, delete, deleteResult);

        Mockito.verify(this.scheduler).deschedule("index-name", "doc-id");
    }

    public void testPostDelete_deletionFailed() {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);
        Engine.Delete delete = this.getDeleteOperation("doc-id");
        Engine.DeleteResult deleteResult = new Engine.DeleteResult(new IOException("exception"), 1L, 1L);

        this.sweeper.postDelete(shardId, delete, deleteResult);

        Mockito.verify(this.scheduler, Mockito.times(0)).deschedule("index-name", "doc-id");
    }

    public void testSweep() throws IOException {
        ShardId shardId = new ShardId(new Index("index-name", IndexMetadata.INDEX_UUID_NA_VALUE), 1);

        this.sweeper.sweep(shardId, "id", this.getTestJsonSource(), new JobDocVersion(1L, 1L, 2L));
        Mockito.verify(this.scheduler, Mockito.times(0)).schedule(Mockito.anyString(), Mockito.anyString(),
                Mockito.any(), Mockito.any(), Mockito.any(JobDocVersion.class), Mockito.any(Double.class));

        ScheduledJobParameter mockJobParameter = Mockito.mock(ScheduledJobParameter.class);
        Mockito.when(mockJobParameter.isEnabled()).thenReturn(true);
        Mockito.when(this.jobParser.parse(Mockito.any(), Mockito.anyString(), Mockito.any(JobDocVersion.class)))
            .thenReturn(mockJobParameter);

        this.sweeper.sweep(shardId, "id", this.getTestJsonSource(), new JobDocVersion(1L, 1L, 2L));
        Mockito.verify(this.scheduler).schedule(Mockito.anyString(), Mockito.anyString(), Mockito.any(), Mockito.any(),
                Mockito.any(JobDocVersion.class), Mockito.any(Double.class));
    }

    private ClusterState addNodesToCluter(ClusterState clusterState, int nodeCount) {
        DiscoveryNodes.Builder nodeBuilder = DiscoveryNodes.builder();
        for (int i = 1; i<=nodeCount; i++) {
            nodeBuilder.add(OpenSearchAllocationTestCase.newNode("node-" + i));
        }

        return ClusterState.builder(clusterState).nodes(nodeBuilder).build();
    }

    private ClusterState initializeAllShards(ClusterState clusterState) {
        AllocationService allocationService = createAllocationService(Settings.builder()
                .put("cluster.routing.allocation.node_concurrent_recoveries", Integer.MAX_VALUE)
                .put("cluster.routing.allocation.node_initial_parimaries_recoveries", Integer.MAX_VALUE)
                .build());
        clusterState = allocationService.reroute(clusterState, "reroute");
        clusterState = allocationService.applyStartedShards(clusterState, clusterState.getRoutingNodes()
                .shardsWithState("index-name", ShardRoutingState.INITIALIZING)); // start primary shard
        clusterState = allocationService.applyStartedShards(clusterState, clusterState.getRoutingNodes()
                .shardsWithState("index-name", ShardRoutingState.INITIALIZING)); // start replica shards
        return clusterState;
    }

    private Engine.Index getIndexOperation() {
        String docId = "doc-id";
        long primaryTerm = 1L;
        List<ParseContext.Document> docs = new ArrayList<>();
        docs.add(new ParseContext.Document());
        BytesReference source = this.getTestJsonSource();

        Term uid = new Term(
            "id_field",
            new BytesRef(docId.getBytes(Charset.defaultCharset()), 0, docId.getBytes(Charset.defaultCharset()).length)
        );
        ParsedDocument parsedDocument = new ParsedDocument(null, null, docId, "_doc", null, docs, source, null, null);

        return new Engine.Index(uid, primaryTerm, parsedDocument);
    }

    private Engine.Delete getDeleteOperation(String docId) {
        Term uid = new Term(
            "id_field",
            new BytesRef(docId.getBytes(Charset.defaultCharset()), 0, docId.getBytes(Charset.defaultCharset()).length)
        );
        return new Engine.Delete("_doc", docId, uid, 1L);
    }

    private BytesReference getTestJsonSource() {
        return new BytesArray("{\n" +
                "\t\"id\": \"id\",\n" +
                "\t\"name\": \"name\",\n" +
                "\t\"version\": 3,\n" +
                "\t\"enabled\": true,\n" +
                "\t\"schedule\": {\n" +
                "\t\t\"cron\": {\n" +
                "\t\t\t\"expression\": \"* * * * *\",\n" +
                "\t\t\t\"timezone\": \"PST8PDT\"\n" +
                "\t\t}\n" +
                "\t},\n" +
                "\t\"sample_param\": \"sample parameter\",\n" +
                "\t\"enable_time\": 1550105987448,\n" +
                "\t\"last_update_time\": 1550105987448\n" +
                "}");
    }

    private IndexMetadata.Builder createIndexMetadata(String indexName, int replicaNumber, int shardNumber) {
        Settings defaultSettings = Settings.builder().put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT).build();
        return new IndexMetadata.Builder(indexName)
                .settings(defaultSettings)
                .numberOfReplicas(replicaNumber)
                .numberOfShards(shardNumber);
    }
}
