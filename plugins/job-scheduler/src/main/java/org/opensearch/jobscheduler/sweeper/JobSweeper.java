/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.jobscheduler.sweeper;

import org.opensearch.jobscheduler.JobSchedulerSettings;
import org.opensearch.jobscheduler.ScheduledJobProvider;
import org.opensearch.jobscheduler.scheduler.JobScheduler;
import org.opensearch.jobscheduler.spi.LockModel;
import org.opensearch.jobscheduler.spi.ScheduledJobParameter;
import org.opensearch.jobscheduler.spi.ScheduledJobRunner;
import org.opensearch.jobscheduler.spi.JobDocVersion;
import org.opensearch.jobscheduler.spi.utils.LockService;
import org.opensearch.jobscheduler.utils.VisibleForTesting;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.OpenSearchException;
import org.opensearch.action.ActionListener;
import org.opensearch.action.bulk.BackoffPolicy;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.client.Client;
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.routing.IndexShardRoutingTable;
import org.opensearch.cluster.routing.Murmur3HashFunction;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.component.LifecycleListener;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.common.util.set.Sets;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.shard.IndexingOperationListener;
import org.opensearch.index.shard.ShardId;
import org.opensearch.rest.RestStatus;
import org.opensearch.search.SearchHit;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.search.sort.FieldSortBuilder;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * Sweeper component that handles job indexing and cluster changes.
 */
public class JobSweeper extends LifecycleListener implements IndexingOperationListener, ClusterStateListener {
    private static final Logger log = LogManager.getLogger(JobSweeper.class);

    private Client client;
    private ClusterService clusterService;
    private ThreadPool threadPool;
    private Map<String, ScheduledJobProvider> indexToProviders;
    private NamedXContentRegistry xContentRegistry;

    private Scheduler.Cancellable scheduledFullSweep;
    private ExecutorService fullSweepExecutor;
    private ConcurrentHashMap<ShardId, ConcurrentHashMap<String, JobDocVersion>> sweptJobs;
    private JobScheduler scheduler;
    private LockService lockService;

    private volatile long lastFullSweepTimeNano;

    private volatile TimeValue sweepPeriod;
    private volatile Integer sweepPageMaxSize;
    private volatile TimeValue sweepSearchTimeout;
    private volatile TimeValue sweepSearchBackoffMillis;
    private volatile Integer sweepSearchBackoffRetryCount;
    private volatile BackoffPolicy sweepSearchBackoff;
    private volatile Double jitterLimit;

    public JobSweeper(Settings settings, Client client, ClusterService clusterService, ThreadPool threadPool,
                      NamedXContentRegistry registry, Map<String, ScheduledJobProvider> indexToProviders, JobScheduler scheduler,
                      LockService lockService) {
        this.client = client;
        this.clusterService = clusterService;
        this.threadPool = threadPool;
        this.xContentRegistry = registry;
        this.indexToProviders = indexToProviders;
        this.scheduler = scheduler;
        this.lockService = lockService;

        this.lastFullSweepTimeNano = System.nanoTime();
        this.loadSettings(settings);
        this.addConfigListeners();

        this.fullSweepExecutor = Executors.newSingleThreadExecutor(
                OpenSearchExecutors.daemonThreadFactory("opendistro_job_sweeper"));
        this.sweptJobs = new ConcurrentHashMap<>();
    }

    private void loadSettings(Settings settings) {
        this.sweepPeriod = JobSchedulerSettings.SWEEP_PERIOD.get(settings);
        this.sweepPageMaxSize = JobSchedulerSettings.SWEEP_PAGE_SIZE.get(settings);
        this.sweepSearchTimeout = JobSchedulerSettings.REQUEST_TIMEOUT.get(settings);
        this.sweepSearchBackoffMillis = JobSchedulerSettings.SWEEP_BACKOFF_MILLIS.get(settings);
        this.sweepSearchBackoffRetryCount = JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT.get(settings);
        this.jitterLimit = JobSchedulerSettings.JITTER_LIMIT.get(settings);
        this.sweepSearchBackoff = this.updateRetryPolicy();
    }

    private void addConfigListeners() {
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_PERIOD,
                timeValue -> {
                    sweepPeriod = timeValue;
                    log.debug("Reinitializing background full sweep with period: {}", this.sweepPeriod.getMinutes());
                    initBackgroundSweep();
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_PAGE_SIZE,
                intValue -> {
                    sweepPageMaxSize = intValue;
                    log.debug("Setting background sweep page size: {}", this.sweepPageMaxSize);
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.REQUEST_TIMEOUT,
                timeValue -> {
                    this.sweepSearchTimeout = timeValue;
                    log.debug("Setting background sweep search timeout: {}", this.sweepSearchTimeout.getMinutes());
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_BACKOFF_MILLIS,
                timeValue -> {
                    this.sweepSearchBackoffMillis = timeValue;
                    this.sweepSearchBackoff = this.updateRetryPolicy();
                    log.debug("Setting background sweep search backoff: {}", this.sweepSearchBackoffMillis.getMillis());
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.SWEEP_BACKOFF_RETRY_COUNT,
                intValue -> {
                    this.sweepSearchBackoffRetryCount = intValue;
                    this.sweepSearchBackoff = this.updateRetryPolicy();
                    log.debug("Setting background sweep search backoff retry count: {}", this.sweepSearchBackoffRetryCount);
                });
        clusterService.getClusterSettings().addSettingsUpdateConsumer(JobSchedulerSettings.JITTER_LIMIT,
                doubleValue -> {
                    this.jitterLimit = doubleValue;
                    log.debug("Setting background sweep jitter limit: {}", this.jitterLimit);
                });
    }

    private BackoffPolicy updateRetryPolicy() {
        return BackoffPolicy.exponentialBackoff(this.sweepSearchBackoffMillis, this.sweepSearchBackoffRetryCount);
    }

    @Override
    public void afterStart() {
        this.initBackgroundSweep();
    }

    @Override
    public void beforeStop() {
        if (this.scheduledFullSweep != null) {
            this.scheduledFullSweep.cancel();
        }
    }

    @Override
    public void beforeClose() {
        this.fullSweepExecutor.shutdown();
    }

    @Override
    public void postIndex(ShardId shardId, Engine.Index index, Engine.IndexResult result) {
        if (result.getResultType().equals(Engine.Result.Type.FAILURE)) {
            log.info("Indexing failed for job {} on index {}", index.id(), shardId.getIndexName());
            return;
        }

        String localNodeId = clusterService.localNode().getId();
        IndexShardRoutingTable routingTable = clusterService.state().routingTable().shardRoutingTable(shardId);
        List<String> shardNodeIds = new ArrayList<>();
        for (ShardRouting shardRouting : routingTable) {
            if (shardRouting.active()) {
                shardNodeIds.add(shardRouting.currentNodeId());
            }
        }

        ShardNodes shardNodes = new ShardNodes(localNodeId, shardNodeIds);
        if (shardNodes.isOwningNode(index.id())) {
            this.sweep(shardId, index.id(), index.source(), new JobDocVersion(result.getTerm(), result.getSeqNo(), result.getVersion()));
        }
    }

    @Override
    public void postDelete(ShardId shardId, Engine.Delete delete, Engine.DeleteResult result) {
        if (result.getResultType() == Engine.Result.Type.FAILURE) {
            ConcurrentHashMap<String, JobDocVersion> shardJobs = this.sweptJobs.containsKey(shardId) ?
                    this.sweptJobs.get(shardId) : new ConcurrentHashMap<>();
            JobDocVersion version = shardJobs.get(delete.id());
            log.debug("Deletion failed for scheduled job {}. Continuing with current version {}", delete.id(), version);
            return;
        }

        if (this.scheduler.getScheduledJobIds(shardId.getIndexName()).contains(delete.id())) {
            log.info("Descheduling job {} on index {}", delete.id(), shardId.getIndexName());
            this.scheduler.deschedule(shardId.getIndexName(), delete.id());
            lockService.deleteLock(LockModel.generateLockId(shardId.getIndexName(), delete.id()), ActionListener.wrap(
                    deleted -> log.debug("Deleted lock: {}", deleted),
                    exception -> log.debug("Failed to delete lock", exception)
            ));
        }
    }

    @VisibleForTesting
    void sweep(ShardId shardId, String docId, BytesReference jobSource, JobDocVersion jobDocVersion) {
        ConcurrentHashMap<String, JobDocVersion> jobVersionMap;
        if (this.sweptJobs.containsKey(shardId)) {
            jobVersionMap = this.sweptJobs.get(shardId);
        } else {
            jobVersionMap = new ConcurrentHashMap<>();
            this.sweptJobs.put(shardId, jobVersionMap);
        }
        jobVersionMap.compute(docId, (id, currentJobDocVersion) -> {
            if (jobDocVersion.compareTo(currentJobDocVersion) <= 0) {
                log.debug("Skipping job {}, new version {} <= current version {}", docId, jobDocVersion, currentJobDocVersion);
                return currentJobDocVersion;
            }

            if (this.scheduler.getScheduledJobIds(shardId.getIndexName()).contains(docId)) {
                this.scheduler.deschedule(shardId.getIndexName(), docId);
            }
            if (jobSource != null) {
                try {
                    ScheduledJobProvider provider = this.indexToProviders.get(shardId.getIndexName());
                    XContentParser parser = XContentHelper.createParser(this.xContentRegistry, LoggingDeprecationHandler.INSTANCE,
                            jobSource, XContentType.JSON);
                    ScheduledJobParameter jobParameter = provider.getJobParser().parse(parser, docId, jobDocVersion);
                    if (jobParameter == null) {
                        // allow parser to return null, which means this is not a scheduled job document.
                        return null;
                    }
                    ScheduledJobRunner jobRunner = this.indexToProviders.get(shardId.getIndexName()).getJobRunner();
                    if (jobParameter.isEnabled()) {
                        this.scheduler.schedule(shardId.getIndexName(), docId, jobParameter, jobRunner, jobDocVersion, jitterLimit);
                    }
                    return jobDocVersion;
                } catch (Exception e) {
                    log.warn("Unable to parse job {}, error message: {}", docId, e.getMessage());
                    return currentJobDocVersion;
                }
            } else {
                return null;
            }
        });
    }

    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        for (String indexName : indexToProviders.keySet()) {
            if (event.indexRoutingTableChanged(indexName)) {
                this.fullSweepExecutor.submit(() -> this.sweepIndex(indexName));
            }
        }
    }

    @VisibleForTesting
    void initBackgroundSweep() {
        if (scheduledFullSweep != null) {
            this.scheduledFullSweep.cancel();
        }

        Runnable scheduledSweep = () -> {
            log.info("Running full sweep");
            TimeValue elapsedTime = getFullSweepElapsedTime();
            long delta = this.sweepPeriod.millis() - elapsedTime.millis();
            if (delta < 20L) {
                this.fullSweepExecutor.submit(this::sweepAllJobIndices);
            }
        };
        this.scheduledFullSweep = this.threadPool.scheduleWithFixedDelay(scheduledSweep, sweepPeriod, ThreadPool.Names.SAME);
    }

    private TimeValue getFullSweepElapsedTime() {
        return TimeValue.timeValueNanos(System.nanoTime() - this.lastFullSweepTimeNano);
    }

    private Map<ShardId, List<ShardRouting>> getLocalShards(ClusterState clusterState, String localNodeId, String indexName) {
        List<ShardRouting> allShards = clusterState.routingTable().allShards(indexName);
        // group shards by shard id
        Map<ShardId, List<ShardRouting>> shards = allShards.stream().filter(ShardRouting::active)
                .collect(Collectors.groupingBy(ShardRouting::shardId,
                        Collectors.mapping(shardRouting -> shardRouting, Collectors.toList())));
        // filter out shards not on local node
        return shards.entrySet().stream().filter((entry) -> entry.getValue().stream()
                .filter((shardRouting -> shardRouting.currentNodeId().equals(localNodeId))).count() > 0)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private void sweepAllJobIndices() {
        for (String indexName : this.indexToProviders.keySet()) {
            this.sweepIndex(indexName);
        }
        this.lastFullSweepTimeNano = System.nanoTime();
    }

    private void sweepIndex(String indexName) {
        ClusterState clusterState = this.clusterService.state();
        if (!clusterState.routingTable().hasIndex(indexName)) {
            // deschedule jobs for this index
            for (ShardId shardId : this.sweptJobs.keySet()) {
                if (shardId.getIndexName().equals(indexName) && this.sweptJobs.containsKey(shardId)) {
                    log.info("Descheduling jobs, shard {} index {} as the index is removed.", shardId.getId(), indexName);
                    this.scheduler.bulkDeschedule(shardId.getIndexName(), this.sweptJobs.get(shardId).keySet());
                }
            }
            return;
        }
        String localNodeId = clusterState.getNodes().getLocalNodeId();
        Map<ShardId, List<ShardRouting>> localShards = this.getLocalShards(clusterState, localNodeId, indexName);

        // deschedule jobs in removed shards
        Iterator<Map.Entry<ShardId, ConcurrentHashMap<String, JobDocVersion>>> sweptJobIter = this.sweptJobs.entrySet().iterator();
        while (sweptJobIter.hasNext()) {
            Map.Entry<ShardId, ConcurrentHashMap<String, JobDocVersion>> entry = sweptJobIter.next();
            if (entry.getKey().getIndexName().equals(indexName) && !localShards.containsKey(entry.getKey())) {
                log.info("Descheduling jobs of shard {} index {} as the shard is removed from this node.",
                        entry.getKey().getId(), indexName);
                //shard is removed, deschedule jobs of this shard
                this.scheduler.bulkDeschedule(indexName, entry.getValue().keySet());
                sweptJobIter.remove();
            }
        }

        // sweep each local shard
        for (Map.Entry<ShardId, List<ShardRouting>> shard : localShards.entrySet()) {
            try {
                List<ShardRouting> shardRoutingList = shard.getValue();
                List<String> shardNodeIds = shardRoutingList.stream().map(ShardRouting::currentNodeId).collect(Collectors.toList());
                sweepShard(shard.getKey(), new ShardNodes(localNodeId, shardNodeIds), null);
            } catch (Exception e) {
                log.info("Error while sweeping shard {}, error message: {}", shard.getKey(), e.getMessage());
            }
        }
    }

    private void sweepShard(ShardId shardId, ShardNodes shardNodes, String startAfter) {
        ConcurrentHashMap<String, JobDocVersion> currentJobs = this.sweptJobs.containsKey(shardId) ?
                this.sweptJobs.get(shardId) : new ConcurrentHashMap<>();

        for (String jobId : currentJobs.keySet()) {
            if (!shardNodes.isOwningNode(jobId)) {
                this.scheduler.deschedule(shardId.getIndexName(), jobId);
                currentJobs.remove(jobId);
            }
        }

        String searchAfter = startAfter == null ? "" : startAfter;
        while (searchAfter != null) {
            SearchRequest jobSearchRequest = new SearchRequest()
                    .indices(shardId.getIndexName())
                    .preference("_shards:" + shardId.id() + "|_only_local")
                    .source(new SearchSourceBuilder()
                            .version(true)
                            .seqNoAndPrimaryTerm(true)
                            .sort(new FieldSortBuilder("_id").unmappedType("keyword").missing("_last"))
                            .searchAfter(new String[]{searchAfter})
                            .size(this.sweepPageMaxSize)
                            .query(QueryBuilders.matchAllQuery()));

            SearchResponse response = this.retry((searchRequest) -> this.client.search(searchRequest),
                    jobSearchRequest, this.sweepSearchBackoff).actionGet(this.sweepSearchTimeout);
            if (response.status() != RestStatus.OK) {
                log.error("Error sweeping shard {}, failed querying jobs on this shard", shardId);
                return;
            }
            for (SearchHit hit : response.getHits()) {
                String jobId = hit.getId();
                if (shardNodes.isOwningNode(jobId)) {
                    this.sweep(shardId, jobId, hit.getSourceRef(), new JobDocVersion(hit.getPrimaryTerm(), hit.getSeqNo(),
                            hit.getVersion()));
                }
            }
            if (response.getHits() == null || response.getHits().getHits().length < 1) {
                searchAfter = null;
            } else {
                SearchHit lastHit = response.getHits().getHits()[response.getHits().getHits().length - 1];
                searchAfter = lastHit.getId();
            }
        }
    }

    private <T, R> R retry(Function<T, R> function, T param, BackoffPolicy backoffPolicy) {
        Set<RestStatus> retryalbeStatus = Sets.newHashSet(RestStatus.BAD_GATEWAY, RestStatus.GATEWAY_TIMEOUT,
                RestStatus.SERVICE_UNAVAILABLE);
        Iterator<TimeValue> iter = backoffPolicy.iterator();
        do {
            try {
                return function.apply(param);
            } catch (OpenSearchException e) {
                if (iter.hasNext() && retryalbeStatus.contains(e.status())) {
                    try {
                        Thread.sleep(iter.next().millis());
                    } catch (InterruptedException ex) {
                        throw e;
                    }
                } else {
                    throw e;
                }
            }
        } while (true);
    }

    private static class ShardNodes {
        private static final int VIRTUAL_NODE_COUNT = 100;

        String localNodeId;
        Collection<String> activeShardNodeIds;
        private TreeMap<Integer, String> circle;

        ShardNodes(String localNodeId, Collection<String> activeShardNodeIds) {
            this.localNodeId = localNodeId;
            this.activeShardNodeIds = activeShardNodeIds;
            this.circle = new TreeMap<>();
            for (String node : activeShardNodeIds) {
                for (int i = 0; i < VIRTUAL_NODE_COUNT; i++) {
                    this.circle.put(Murmur3HashFunction.hash(node + i), node);
                }
            }
        }

        boolean isOwningNode(String jobId) {
            if (this.circle.isEmpty()) {
                return false;
            }
            int jobHashCode = Murmur3HashFunction.hash(jobId);
            String nodeId = this.circle.higherEntry(jobHashCode) == null ? this.circle.firstEntry().getValue()
                    : this.circle.higherEntry(jobHashCode).getValue();
            return this.localNodeId.equals(nodeId);
        }
    }
}
