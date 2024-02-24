/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.insights.core.service;

import org.opensearch.client.Client;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.inject.Inject;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.core.action.ActionListener;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataAction;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataRequest;
import org.opensearch.plugin.insights.rules.action.top_queries.SearchMetadataResponse;
import org.opensearch.plugin.insights.rules.model.MetricType;
import org.opensearch.plugin.insights.rules.model.SearchQueryRecord;
import org.opensearch.plugin.insights.rules.model.SearchTaskMetadata;
import org.opensearch.plugin.insights.settings.QueryInsightsSettings;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.threadpool.ThreadPool;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * Service responsible for gathering, analyzing, storing and exporting
 * information related to search queries
 *
 * @opensearch.internal
 */
public class QueryInsightsService extends AbstractLifecycleComponent {
    /**
     * The internal OpenSearch thread pool that execute async processing and exporting tasks
     */
    private final ThreadPool threadPool;
    private final Client client;

    public final ClusterService clusterService;

    /**
     * Services to capture top n queries for different metric types
     */
    private final Map<MetricType, TopQueriesService> topQueriesServices;

    /**
     * Flags for enabling insight data collection for different metric types
     */
    private final Map<MetricType, Boolean> enableCollect;

    /**
     * The internal thread-safe queue to ingest the search query data and subsequently forward to processors
     */
    private final LinkedBlockingQueue<SearchQueryRecord> queryRecordsQueue;

    // TODO Move these to top queries service and change to private
    public final LinkedBlockingQueue<SearchTaskMetadata> taskRecordsQueue = new LinkedBlockingQueue<>();

    public final ConcurrentHashMap<Long, AtomicInteger> taskStatusMap = new ConcurrentHashMap<>();

    /**
     * Holds a reference to delayed operation {@link Scheduler.Cancellable} so it can be cancelled when
     * the service closed concurrently.
     */
    protected volatile Scheduler.Cancellable scheduledFuture;

    /**
     * Constructor of the QueryInsightsService
     *
     * @param threadPool     The OpenSearch thread pool to run async tasks
     */
    @Inject
    public QueryInsightsService(final ThreadPool threadPool, final Client client, final ClusterService clusterService) {
        enableCollect = new HashMap<>();
        queryRecordsQueue = new LinkedBlockingQueue<>(QueryInsightsSettings.QUERY_RECORD_QUEUE_CAPACITY);
        topQueriesServices = new HashMap<>();
        for (MetricType metricType : MetricType.allMetricTypes()) {
            enableCollect.put(metricType, false);
            topQueriesServices.put(metricType, new TopQueriesService(metricType));
        }
        this.threadPool = threadPool;
        this.client = client;
        this.clusterService = clusterService;
    }

    /**
     * Ingest the query data into in-memory stores
     *
     * @param record the record to ingest
     */
    public boolean addRecord(final SearchQueryRecord record) {
        boolean shouldAdd = false;
        for (Map.Entry<MetricType, TopQueriesService> entry : topQueriesServices.entrySet()) {
            if (!enableCollect.get(entry.getKey())) {
                continue;
            }
            List<SearchQueryRecord> currentSnapshot = entry.getValue().getTopQueriesCurrentSnapshot();
            // skip add to top N queries store if the incoming record is smaller than the Nth record
            if (currentSnapshot.size() < entry.getValue().getTopNSize()
                || SearchQueryRecord.compare(record, currentSnapshot.get(0), entry.getKey()) > 0) {
                shouldAdd = true;
                break;
            }
        }
        if (shouldAdd) {
            return queryRecordsQueue.offer(record);
        }
        return false;
    }

    /**
     * Drain the queryRecordsQueue into internal stores and services
     */
    public void drainRecords() {
        SearchMetadataRequest request = new SearchMetadataRequest();

        // Am on Cluster Manager Node, get all top queries and tasks data from all nodes and correlate them
        client.execute(SearchMetadataAction.INSTANCE, request, new ActionListener<SearchMetadataResponse>() {
            @Override
            public void onResponse(SearchMetadataResponse searchMetadataResponse) {
                List<SearchQueryRecord> clusterQueryRecordsList = searchMetadataResponse.getNodes().stream().flatMap(a -> a.queryRecordList.stream()).collect(Collectors.toList());
                List<SearchTaskMetadata> clusterTasksList = searchMetadataResponse.getNodes().stream().flatMap(a -> a.taskMetadataList.stream()).collect(Collectors.toList());
                Map<Long, Integer> clusterTasksMap = searchMetadataResponse.getNodes().stream().flatMap(a -> a.taskStatusMap.entrySet().stream()).collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue, Integer::sum));
                drain(clusterQueryRecordsList, clusterTasksList, clusterTasksMap);
            }

            @Override
            public void onFailure(Exception e) {
                // TODO
            }
        });
    }

    private void drain(List<SearchQueryRecord> clusterQueryRecords, List<SearchTaskMetadata> clusterTaskRecords, Map<Long, Integer> clusterTasksStatusMap) {
        final List<SearchQueryRecord> finishedQueryRecord = correlateTasks(clusterQueryRecords, clusterTaskRecords, clusterTasksStatusMap);
        finishedQueryRecord.sort(Comparator.comparingLong(SearchQueryRecord::getTimestamp));
        for (MetricType metricType : MetricType.allMetricTypes()) {
            if (enableCollect.get(metricType)) {
                // ingest the records into topQueriesService
                topQueriesServices.get(metricType).consumeRecords(finishedQueryRecord);
            }
        }
    }



    public List<SearchQueryRecord> correlateTasks(List<SearchQueryRecord> clusterQueryRecords, List<SearchTaskMetadata> clusterTaskRecords, Map<Long, Integer> clusterTaskStatusMap) {
        List<SearchQueryRecord> finalResults = new ArrayList<>();
        // group taskRecords by parent task
        Map<Long, List<SearchTaskMetadata>> taskIdToResources = new HashMap<>();
        for (SearchTaskMetadata info : clusterTaskRecords) {
            taskIdToResources.putIfAbsent(info.parentTaskId, new ArrayList<>());
            taskIdToResources.get(info.parentTaskId).add(info);
        }
        for (SearchQueryRecord record : clusterQueryRecords) {
            if (!taskIdToResources.containsKey(record.taskId)) {
                // TODO: No task info for a request, this shouldn't happen - something is wrong.
                continue;
            }
            // parent task has finished
            // TODO can remove first check after debugging
            if (!clusterTaskStatusMap.containsKey(record.taskId) || clusterTaskStatusMap.get(record.taskId) == 0) {
                long cpuUsage = taskIdToResources.get(record.taskId).stream().map(r -> r.taskResourceUsage.getCpuTimeInNanos()).reduce(0L, Long::sum);
                long memUsage = taskIdToResources.get(record.taskId).stream().map(r -> r.taskResourceUsage.getMemoryInBytes()).reduce(0L, Long::sum);
                record.measurements.put(MetricType.CPU, cpuUsage);
                record.measurements.put(MetricType.JVM, memUsage);
                finalResults.add(record);
            } else {
                // write back since the task information is not completed
                queryRecordsQueue.offer(record);
                taskRecordsQueue.addAll(taskIdToResources.get(record.taskId));
            }
        }
        return finalResults;
    }

    /**
     * Get the top queries service based on metricType
     * @param metricType {@link MetricType}
     * @return {@link TopQueriesService}
     */
    public TopQueriesService getTopQueriesService(final MetricType metricType) {
        return topQueriesServices.get(metricType);
    }

    public List<SearchQueryRecord> getQueryRecordsList() {
        final List<SearchQueryRecord> queryRecords = new ArrayList<>();
        queryRecordsQueue.drainTo(queryRecords);

        return queryRecords;
    }

    public List<SearchTaskMetadata> getTaskRecordsList() {
        final List<SearchTaskMetadata> taskRecords = new ArrayList<>();
        taskRecordsQueue.drainTo(taskRecords);

        return taskRecords;
    }

    public Map<Long, Integer> getTaskStatusMap() {
        Map<Long, Integer> res = taskStatusMap.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e-> e.getValue().get()));
//        taskStatusMap.clear();

        return res;
    }

    /**
     * Set flag to enable or disable Query Insights data collection
     *
     * @param metricType {@link MetricType}
     * @param enable Flag to enable or disable Query Insights data collection
     */
    public void enableCollection(final MetricType metricType, final boolean enable) {
        this.enableCollect.put(metricType, enable);
        this.topQueriesServices.get(metricType).setEnabled(enable);
    }

    /**
     * Get if the Query Insights data collection is enabled for a MetricType
     *
     * @param metricType {@link MetricType}
     * @return if the Query Insights data collection is enabled
     */
    public boolean isCollectionEnabled(final MetricType metricType) {
        return this.enableCollect.get(metricType);
    }

    /**
     * Check if query insights service is enabled
     *
     * @return if query insights service is enabled
     */
    public boolean isEnabled() {
        for (MetricType t : MetricType.allMetricTypes()) {
            if (isCollectionEnabled(t)) {
                return true;
            }
        }
        return false;
    }

    @Override
    protected void doStart() {
        if (isEnabled() && clusterService.state().nodes().isLocalNodeElectedClusterManager()) {
            scheduledFuture = threadPool.scheduleWithFixedDelay(
                this::drainRecords,
                QueryInsightsSettings.QUERY_RECORD_QUEUE_DRAIN_INTERVAL,
                QueryInsightsSettings.QUERY_INSIGHTS_EXECUTOR
            );
        }
    }

    @Override
    protected void doStop() {
        if (scheduledFuture != null) {
            scheduledFuture.cancel();
        }
    }

    @Override
    protected void doClose() {}
}
