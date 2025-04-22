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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.rest.action.cat;

import org.opensearch.OpenSearchParseException;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.state.ClusterStateRequest;
import org.opensearch.action.admin.cluster.state.ClusterStateResponse;
import org.opensearch.action.admin.indices.settings.get.GetSettingsRequest;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.pagination.IndexPaginationStrategy;
import org.opensearch.action.pagination.PageToken;
import org.opensearch.action.support.GroupedActionListener;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.cluster.health.ClusterIndexHealth;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Table;
import org.opensearch.common.breaker.ResponseLimitBreachedException;
import org.opensearch.common.breaker.ResponseLimitSettings;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.time.DateFormatter;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexSettings;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.list.AbstractListAction;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterators;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest.DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;
import static org.opensearch.common.breaker.ResponseLimitSettings.LimitEntity.INDICES;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * _cat API action to list indices
 *
 * @opensearch.api
 */
public class RestIndicesAction extends AbstractListAction {

    private static final DateFormatter STRICT_DATE_TIME_FORMATTER = DateFormatter.forPattern("strict_date_time");

    private final ResponseLimitSettings responseLimitSettings;

    public RestIndicesAction(ResponseLimitSettings responseLimitSettings) {
        this.responseLimitSettings = responseLimitSettings;
    }

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/indices"), new Route(GET, "/_cat/indices/{index}")));
    }

    @Override
    public String getName() {
        return "cat_indices_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/indices\n");
        sb.append("/_cat/indices/{index}\n");
    }

    @Override
    public boolean isRequestLimitCheckSupported() {
        return true;
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final IndicesOptions indicesOptions = IndicesOptions.fromRequest(request, IndicesOptions.strictExpand());
        final boolean local = request.paramAsBoolean("local", false);
        TimeValue clusterManagerTimeout = request.paramAsTime("cluster_manager_timeout", DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
        // Remove the if condition and statements inside after removing MASTER_ROLE.
        if (request.hasParam("master_timeout")) {
            if (request.hasParam("cluster_manager_timeout")) {
                throw new OpenSearchParseException(DUPLICATE_PARAMETER_ERROR_MESSAGE);
            }
            clusterManagerTimeout = request.paramAsTime("master_timeout", DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT);
        }
        final TimeValue clusterManagerNodeTimeout = clusterManagerTimeout;
        final boolean includeUnloadedSegments = request.paramAsBoolean("include_unloaded_segments", false);

        return channel -> {
            final ActionListener<Table> listener = ActionListener.notifyOnce(new RestResponseListener<Table>(channel) {
                @Override
                public RestResponse buildResponse(final Table table) throws Exception {
                    return RestTable.buildResponse(table, channel);
                }
            });

            sendGetSettingsRequest(
                indices,
                indicesOptions,
                local,
                clusterManagerNodeTimeout,
                client,
                new ActionListener<GetSettingsResponse>() {
                    @Override
                    public void onResponse(final GetSettingsResponse getSettingsResponse) {
                        // The list of indices that will be returned is determined by the indices returned from the Get Settings call.
                        // All the other requests just provide additional detail, and wildcards may be resolved differently depending on the
                        // type of request in the presence of security plugins (looking at you, ClusterHealthRequest), so
                        // force the IndicesOptions for all the sub-requests to be as inclusive as possible.
                        final IndicesOptions subRequestIndicesOptions = IndicesOptions.lenientExpandHidden();
                        // Indices that were successfully resolved during the get settings request might be deleted when the
                        // subsequent cluster state, cluster health and indices stats requests execute. We have to distinguish two cases:
                        // 1) the deleted index was explicitly passed as parameter to the /_cat/indices request. In this case we
                        // want the subsequent requests to fail.
                        // 2) the deleted index was resolved as part of a wildcard or _all. In this case, we want the subsequent
                        // requests not to fail on the deleted index (as we want to ignore wildcards that cannot be resolved).
                        // This behavior can be ensured by letting the cluster state, cluster health and indices stats requests
                        // re-resolve the index names with the same indices options that we used for the initial cluster state
                        // request (strictExpand).
                        sendClusterStateRequest(
                            indices,
                            subRequestIndicesOptions,
                            local,
                            clusterManagerNodeTimeout,
                            client,
                            new ActionListener<ClusterStateResponse>() {
                                @Override
                                public void onResponse(ClusterStateResponse clusterStateResponse) {
                                    validateRequestLimit(clusterStateResponse, listener);
                                    IndexPaginationStrategy paginationStrategy = getPaginationStrategy(clusterStateResponse);
                                    // For non-paginated queries, indicesToBeQueried would be same as indices retrieved from
                                    // rest request and unresolved, while for paginated queries, it would be a list of indices
                                    // already resolved by ClusterStateRequest and to be displayed in a page.
                                    final String[] indicesToBeQueried = Objects.isNull(paginationStrategy)
                                        ? indices
                                        : paginationStrategy.getRequestedEntities().toArray(new String[0]);
                                    final GroupedActionListener<ActionResponse> groupedListener = createGroupedListener(
                                        request,
                                        4,
                                        listener,
                                        indicesToBeQueried,
                                        Objects.isNull(paginationStrategy) ? null : paginationStrategy.getResponseToken()
                                    );
                                    groupedListener.onResponse(getSettingsResponse);
                                    groupedListener.onResponse(clusterStateResponse);

                                    // For paginated queries, if strategy outputs no indices to be returned,
                                    // avoid fetching indices stats.
                                    if (shouldSkipIndicesStatsRequest(paginationStrategy)) {
                                        groupedListener.onResponse(IndicesStatsResponse.getEmptyResponse());
                                    } else {
                                        sendIndicesStatsRequest(
                                            indicesToBeQueried,
                                            subRequestIndicesOptions,
                                            includeUnloadedSegments,
                                            client,
                                            ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure)
                                        );
                                    }

                                    sendClusterHealthRequest(
                                        indicesToBeQueried,
                                        subRequestIndicesOptions,
                                        local,
                                        clusterManagerNodeTimeout,
                                        client,
                                        ActionListener.wrap(groupedListener::onResponse, groupedListener::onFailure)
                                    );
                                }

                                @Override
                                public void onFailure(Exception e) {
                                    listener.onFailure(e);
                                }
                            }
                        );
                    }

                    @Override
                    public void onFailure(final Exception e) {
                        listener.onFailure(e);
                    }
                }
            );
        };

    }

    private void validateRequestLimit(final ClusterStateResponse clusterStateResponse, final ActionListener<Table> listener) {
        if (isRequestLimitCheckSupported() && Objects.nonNull(clusterStateResponse) && Objects.nonNull(clusterStateResponse.getState())) {
            int limit = responseLimitSettings.getCatIndicesResponseLimit();
            if (ResponseLimitSettings.isResponseLimitBreached(clusterStateResponse.getState().getMetadata(), INDICES, limit)) {
                listener.onFailure(new ResponseLimitBreachedException("Too many indices requested.", limit, INDICES));
            }
        }
    }

    /**
     * We're using the Get Settings API here to resolve the authorized indices for the user.
     * This is because the Cluster State and Cluster Health APIs do not filter output based
     * on index privileges, so they can't be used to determine which indices are authorized
     * or not. On top of this, the Indices Stats API cannot be used either to resolve indices
     * as it does not provide information for all existing indices (for example recovering
     * indices or non replicated closed indices are not reported in indices stats response).
     */
    private void sendGetSettingsRequest(
        final String[] indices,
        final IndicesOptions indicesOptions,
        final boolean local,
        final TimeValue clusterManagerNodeTimeout,
        final NodeClient client,
        final ActionListener<GetSettingsResponse> listener
    ) {
        final GetSettingsRequest request = new GetSettingsRequest();
        request.indices(indices);
        request.indicesOptions(indicesOptions);
        request.local(local);
        request.clusterManagerNodeTimeout(clusterManagerNodeTimeout);
        request.names(IndexSettings.INDEX_SEARCH_THROTTLED.getKey());

        client.admin().indices().getSettings(request, listener);
    }

    private void sendClusterStateRequest(
        final String[] indices,
        final IndicesOptions indicesOptions,
        final boolean local,
        final TimeValue clusterManagerNodeTimeout,
        final NodeClient client,
        final ActionListener<ClusterStateResponse> listener
    ) {

        final ClusterStateRequest request = new ClusterStateRequest();
        request.indices(indices);
        request.indicesOptions(indicesOptions);
        request.local(local);
        request.clusterManagerNodeTimeout(clusterManagerNodeTimeout);

        client.admin().cluster().state(request, listener);
    }

    private void sendClusterHealthRequest(
        final String[] indices,
        final IndicesOptions indicesOptions,
        final boolean local,
        final TimeValue clusterManagerNodeTimeout,
        final NodeClient client,
        final ActionListener<ClusterHealthResponse> listener
    ) {

        final ClusterHealthRequest request = new ClusterHealthRequest();
        request.indices(indices);
        request.indicesOptions(indicesOptions);
        request.local(local);
        request.clusterManagerNodeTimeout(clusterManagerNodeTimeout);

        client.admin().cluster().health(request, listener);
    }

    private void sendIndicesStatsRequest(
        final String[] indices,
        final IndicesOptions indicesOptions,
        final boolean includeUnloadedSegments,
        final NodeClient client,
        final ActionListener<IndicesStatsResponse> listener
    ) {

        final IndicesStatsRequest request = new IndicesStatsRequest();
        request.indices(indices);
        request.indicesOptions(indicesOptions);
        request.all();
        request.includeUnloadedSegments(includeUnloadedSegments);

        client.admin().indices().stats(request, listener);
    }

    private GroupedActionListener<ActionResponse> createGroupedListener(
        final RestRequest request,
        final int size,
        final ActionListener<Table> listener,
        final String[] indicesToBeQueried,
        final PageToken pageToken
    ) {
        return new GroupedActionListener<>(new ActionListener<Collection<ActionResponse>>() {
            @Override
            public void onResponse(final Collection<ActionResponse> responses) {
                try {
                    GetSettingsResponse settingsResponse = extractResponse(responses, GetSettingsResponse.class);
                    Map<String, Settings> indicesSettings = StreamSupport.stream(
                        Spliterators.spliterator(settingsResponse.getIndexToSettings().entrySet(), 0),
                        false
                    ).collect(Collectors.toMap(cursor -> cursor.getKey(), cursor -> cursor.getValue()));

                    ClusterStateResponse stateResponse = extractResponse(responses, ClusterStateResponse.class);
                    Map<String, IndexMetadata> indicesStates = StreamSupport.stream(
                        stateResponse.getState().getMetadata().spliterator(),
                        false
                    ).collect(Collectors.toMap(indexMetadata -> indexMetadata.getIndex().getName(), Function.identity()));

                    ClusterHealthResponse healthResponse = extractResponse(responses, ClusterHealthResponse.class);
                    Map<String, ClusterIndexHealth> indicesHealths = healthResponse.getIndices();

                    IndicesStatsResponse statsResponse = extractResponse(responses, IndicesStatsResponse.class);
                    Map<String, IndexStats> indicesStats = statsResponse.getIndices();

                    Table responseTable = buildTable(
                        request,
                        indicesSettings,
                        indicesHealths,
                        indicesStats,
                        indicesStates,
                        getTableIterator(indicesToBeQueried, indicesSettings),
                        pageToken
                    );
                    listener.onResponse(responseTable);
                } catch (Exception e) {
                    onFailure(e);
                }
            }

            @Override
            public void onFailure(final Exception e) {
                listener.onFailure(e);
            }
        }, size);
    }

    private static final Set<String> RESPONSE_PARAMS;

    static {
        final Set<String> responseParams = new HashSet<>(asList("local", "health"));
        responseParams.addAll(AbstractCatAction.RESPONSE_PARAMS);
        RESPONSE_PARAMS = Collections.unmodifiableSet(responseParams);
    }

    @Override
    protected Set<String> responseParams() {
        return RESPONSE_PARAMS;
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        return getTableWithHeader(request, null);
    }

    protected Table getTableWithHeader(final RestRequest request, final PageToken pageToken) {
        Table table = new Table(pageToken);
        table.startHeaders();
        table.addCell("health", "alias:h;desc:current health status");
        table.addCell("status", "alias:s;desc:open/close status");
        table.addCell("index", "alias:i,idx;desc:index name");
        table.addCell("uuid", "alias:id,uuid;desc:index uuid");
        table.addCell("pri", "alias:p,shards.primary,shardsPrimary;text-align:right;desc:number of primary shards");
        table.addCell("rep", "alias:r,shards.replica,shardsReplica;text-align:right;desc:number of replica shards");
        table.addCell("docs.count", "alias:dc,docsCount;text-align:right;desc:available docs");
        table.addCell("docs.deleted", "alias:dd,docsDeleted;text-align:right;desc:deleted docs");

        table.addCell("creation.date", "alias:cd;default:false;desc:index creation date (millisecond value)");
        table.addCell("creation.date.string", "alias:cds;default:false;desc:index creation date (as string)");

        table.addCell("store.size", "sibling:pri;alias:ss,storeSize;text-align:right;desc:store size of primaries & replicas");
        table.addCell("pri.store.size", "text-align:right;desc:store size of primaries");

        table.addCell("completion.size", "sibling:pri;alias:cs,completionSize;default:false;text-align:right;desc:size of completion");
        table.addCell("pri.completion.size", "default:false;text-align:right;desc:size of completion");

        table.addCell(
            "fielddata.memory_size",
            "sibling:pri;alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache"
        );
        table.addCell("pri.fielddata.memory_size", "default:false;text-align:right;desc:used fielddata cache");

        table.addCell(
            "fielddata.evictions",
            "sibling:pri;alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions"
        );
        table.addCell("pri.fielddata.evictions", "default:false;text-align:right;desc:fielddata evictions");

        table.addCell(
            "query_cache.memory_size",
            "sibling:pri;alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache"
        );
        table.addCell("pri.query_cache.memory_size", "default:false;text-align:right;desc:used query cache");

        table.addCell(
            "query_cache.evictions",
            "sibling:pri;alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions"
        );
        table.addCell("pri.query_cache.evictions", "default:false;text-align:right;desc:query cache evictions");

        table.addCell(
            "request_cache.memory_size",
            "sibling:pri;alias:rcm,requestCacheMemory;default:false;text-align:right;desc:used request cache"
        );
        table.addCell("pri.request_cache.memory_size", "default:false;text-align:right;desc:used request cache");

        table.addCell(
            "request_cache.evictions",
            "sibling:pri;alias:rce,requestCacheEvictions;default:false;text-align:right;desc:request cache evictions"
        );
        table.addCell("pri.request_cache.evictions", "default:false;text-align:right;desc:request cache evictions");

        table.addCell(
            "request_cache.hit_count",
            "sibling:pri;alias:rchc,requestCacheHitCount;default:false;text-align:right;desc:request cache hit count"
        );
        table.addCell("pri.request_cache.hit_count", "default:false;text-align:right;desc:request cache hit count");

        table.addCell(
            "request_cache.miss_count",
            "sibling:pri;alias:rcmc,requestCacheMissCount;default:false;text-align:right;desc:request cache miss count"
        );
        table.addCell("pri.request_cache.miss_count", "default:false;text-align:right;desc:request cache miss count");

        table.addCell("flush.total", "sibling:pri;alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("pri.flush.total", "default:false;text-align:right;desc:number of flushes");

        table.addCell("flush.total_time", "sibling:pri;alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");
        table.addCell("pri.flush.total_time", "default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "sibling:pri;alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("pri.get.current", "default:false;text-align:right;desc:number of current get ops");

        table.addCell("get.time", "sibling:pri;alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("pri.get.time", "default:false;text-align:right;desc:time spent in get");

        table.addCell("get.total", "sibling:pri;alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("pri.get.total", "default:false;text-align:right;desc:number of get ops");

        table.addCell(
            "get.exists_time",
            "sibling:pri;alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets"
        );
        table.addCell("pri.get.exists_time", "default:false;text-align:right;desc:time spent in successful gets");

        table.addCell(
            "get.exists_total",
            "sibling:pri;alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets"
        );
        table.addCell("pri.get.exists_total", "default:false;text-align:right;desc:number of successful gets");

        table.addCell(
            "get.missing_time",
            "sibling:pri;alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets"
        );
        table.addCell("pri.get.missing_time", "default:false;text-align:right;desc:time spent in failed gets");

        table.addCell(
            "get.missing_total",
            "sibling:pri;alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets"
        );
        table.addCell("pri.get.missing_total", "default:false;text-align:right;desc:number of failed gets");

        table.addCell(
            "indexing.delete_current",
            "sibling:pri;alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions"
        );
        table.addCell("pri.indexing.delete_current", "default:false;text-align:right;desc:number of current deletions");

        table.addCell(
            "indexing.delete_time",
            "sibling:pri;alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions"
        );
        table.addCell("pri.indexing.delete_time", "default:false;text-align:right;desc:time spent in deletions");

        table.addCell(
            "indexing.delete_total",
            "sibling:pri;alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops"
        );
        table.addCell("pri.indexing.delete_total", "default:false;text-align:right;desc:number of delete ops");

        table.addCell(
            "indexing.index_current",
            "sibling:pri;alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops"
        );
        table.addCell("pri.indexing.index_current", "default:false;text-align:right;desc:number of current indexing ops");

        table.addCell(
            "indexing.index_time",
            "sibling:pri;alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing"
        );
        table.addCell("pri.indexing.index_time", "default:false;text-align:right;desc:time spent in indexing");

        table.addCell(
            "indexing.index_total",
            "sibling:pri;alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops"
        );
        table.addCell("pri.indexing.index_total", "default:false;text-align:right;desc:number of indexing ops");

        table.addCell(
            "indexing.index_failed",
            "sibling:pri;alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops"
        );
        table.addCell("pri.indexing.index_failed", "default:false;text-align:right;desc:number of failed indexing ops");

        table.addCell("merges.current", "sibling:pri;alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell("pri.merges.current", "default:false;text-align:right;desc:number of current merges");

        table.addCell(
            "merges.current_docs",
            "sibling:pri;alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs"
        );
        table.addCell("pri.merges.current_docs", "default:false;text-align:right;desc:number of current merging docs");

        table.addCell(
            "merges.current_size",
            "sibling:pri;alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges"
        );
        table.addCell("pri.merges.current_size", "default:false;text-align:right;desc:size of current merges");

        table.addCell("merges.total", "sibling:pri;alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("pri.merges.total", "default:false;text-align:right;desc:number of completed merge ops");

        table.addCell("merges.total_docs", "sibling:pri;alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("pri.merges.total_docs", "default:false;text-align:right;desc:docs merged");

        table.addCell("merges.total_size", "sibling:pri;alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("pri.merges.total_size", "default:false;text-align:right;desc:size merged");

        table.addCell(
            "merges.total_time",
            "sibling:pri;alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges"
        );
        table.addCell("pri.merges.total_time", "default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "sibling:pri;alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("pri.refresh.total", "default:false;text-align:right;desc:total refreshes");

        table.addCell("refresh.time", "sibling:pri;alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("pri.refresh.time", "default:false;text-align:right;desc:time spent in refreshes");

        table.addCell(
            "refresh.external_total",
            "sibling:pri;alias:rto,refreshTotal;default:false;text-align:right;desc:total external refreshes"
        );
        table.addCell("pri.refresh.external_total", "default:false;text-align:right;desc:total external refreshes");

        table.addCell(
            "refresh.external_time",
            "sibling:pri;alias:rti,refreshTime;default:false;text-align:right;desc:time spent in external refreshes"
        );
        table.addCell("pri.refresh.external_time", "default:false;text-align:right;desc:time spent in external refreshes");

        table.addCell(
            "refresh.listeners",
            "sibling:pri;alias:rli,refreshListeners;default:false;text-align:right;desc:number of pending refresh listeners"
        );
        table.addCell("pri.refresh.listeners", "default:false;text-align:right;desc:number of pending refresh listeners");

        table.addCell(
            "search.fetch_current",
            "sibling:pri;alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops"
        );
        table.addCell("pri.search.fetch_current", "default:false;text-align:right;desc:current fetch phase ops");

        table.addCell(
            "search.fetch_time",
            "sibling:pri;alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase"
        );
        table.addCell("pri.search.fetch_time", "default:false;text-align:right;desc:time spent in fetch phase");

        table.addCell("search.fetch_total", "sibling:pri;alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("pri.search.fetch_total", "default:false;text-align:right;desc:total fetch ops");

        table.addCell(
            "search.open_contexts",
            "sibling:pri;alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts"
        );
        table.addCell("pri.search.open_contexts", "default:false;text-align:right;desc:open search contexts");

        table.addCell(
            "search.query_current",
            "sibling:pri;alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops"
        );
        table.addCell("pri.search.query_current", "default:false;text-align:right;desc:current query phase ops");

        table.addCell(
            "search.query_time",
            "sibling:pri;alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase"
        );
        table.addCell("pri.search.query_time", "default:false;text-align:right;desc:time spent in query phase");

        table.addCell(
            "search.query_total",
            "sibling:pri;alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops"
        );
        table.addCell("pri.search.query_total", "default:false;text-align:right;desc:total query phase ops");
        table.addCell(
            "search.concurrent_query_current",
            "sibling:pri;alias:scqc,searchConcurrentQueryCurrent;default:false;text-align:right;desc:current concurrent query phase ops"
        );
        table.addCell("pri.search.concurrent_query_current", "default:false;text-align:right;desc:current concurrent query phase ops");

        table.addCell(
            "search.concurrent_query_time",
            "sibling:pri;alias:scqti,searchConcurrentQueryTime;default:false;text-align:right;desc:time spent in concurrent query phase"
        );
        table.addCell("pri.search.concurrent_query_time", "default:false;text-align:right;desc:time spent in concurrent query phase");

        table.addCell(
            "search.concurrent_query_total",
            "sibling:pri;alias:scqto,searchConcurrentQueryTotal;default:false;text-align:right;desc:total query phase ops"
        );
        table.addCell("pri.search.concurrent_query_total", "default:false;text-align:right;desc:total query phase ops");

        table.addCell(
            "search.concurrent_avg_slice_count",
            "sibling:pri;alias:casc,searchConcurrentAvgSliceCount;default:false;text-align:right;desc:average query concurrency"
        );
        table.addCell("pri.search.concurrent_avg_slice_count", "default:false;text-align:right;desc:average query concurrency");

        table.addCell(
            "search.scroll_current",
            "sibling:pri;alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts"
        );
        table.addCell("pri.search.scroll_current", "default:false;text-align:right;desc:open scroll contexts");

        table.addCell(
            "search.scroll_time",
            "sibling:pri;alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open"
        );
        table.addCell("pri.search.scroll_time", "default:false;text-align:right;desc:time scroll contexts held open");

        table.addCell(
            "search.scroll_total",
            "sibling:pri;alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts"
        );
        table.addCell("pri.search.scroll_total", "default:false;text-align:right;desc:completed scroll contexts");

        table.addCell(
            "search.point_in_time_current",
            "sibling:pri;alias:scc,searchPointInTimeCurrent;default:false;text-align:right;desc:open point in time contexts"
        );
        table.addCell("pri.search.point_in_time_current", "default:false;text-align:right;desc:open point in time contexts");

        table.addCell(
            "search.point_in_time_time",
            "sibling:pri;alias:scti,searchPointInTimeTime;default:false;text-align:right;desc:time point in time contexts held open"
        );
        table.addCell("pri.search.point_in_time_time", "default:false;text-align:right;desc:time point in time contexts held open");

        table.addCell(
            "search.point_in_time_total",
            "sibling:pri;alias:scto,searchPointInTimeTotal;default:false;text-align:right;desc:completed point in time contexts"
        );
        table.addCell("pri.search.point_in_time_total", "default:false;text-align:right;desc:completed point in time contexts");

        table.addCell("segments.count", "sibling:pri;alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("pri.segments.count", "default:false;text-align:right;desc:number of segments");

        table.addCell("segments.memory", "sibling:pri;alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell("pri.segments.memory", "default:false;text-align:right;desc:memory used by segments");

        table.addCell(
            "segments.index_writer_memory",
            "sibling:pri;alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer"
        );
        table.addCell("pri.segments.index_writer_memory", "default:false;text-align:right;desc:memory used by index writer");

        table.addCell(
            "segments.version_map_memory",
            "sibling:pri;alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map"
        );
        table.addCell("pri.segments.version_map_memory", "default:false;text-align:right;desc:memory used by version map");

        table.addCell(
            "segments.fixed_bitset_memory",
            "sibling:pri;alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for"
                + " nested object field types and type filters for types referred in _parent fields"
        );
        table.addCell(
            "pri.segments.fixed_bitset_memory",
            "default:false;text-align:right;desc:memory used by fixed bit sets for nested object"
                + " field types and type filters for types referred in _parent fields"
        );

        table.addCell("warmer.current", "sibling:pri;alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("pri.warmer.current", "default:false;text-align:right;desc:current warmer ops");

        table.addCell("warmer.total", "sibling:pri;alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("pri.warmer.total", "default:false;text-align:right;desc:total warmer ops");

        table.addCell(
            "warmer.total_time",
            "sibling:pri;alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers"
        );
        table.addCell("pri.warmer.total_time", "default:false;text-align:right;desc:time spent in warmers");

        table.addCell(
            "suggest.current",
            "sibling:pri;alias:suc,suggestCurrent;default:false;text-align:right;desc:number of current suggest ops"
        );
        table.addCell("pri.suggest.current", "default:false;text-align:right;desc:number of current suggest ops");

        table.addCell("suggest.time", "sibling:pri;alias:suti,suggestTime;default:false;text-align:right;desc:time spend in suggest");
        table.addCell("pri.suggest.time", "default:false;text-align:right;desc:time spend in suggest");

        table.addCell("suggest.total", "sibling:pri;alias:suto,suggestTotal;default:false;text-align:right;desc:number of suggest ops");
        table.addCell("pri.suggest.total", "default:false;text-align:right;desc:number of suggest ops");

        table.addCell("memory.total", "sibling:pri;alias:tm,memoryTotal;default:false;text-align:right;desc:total used memory");
        table.addCell("pri.memory.total", "default:false;text-align:right;desc:total user memory");

        table.addCell("search.throttled", "alias:sth;default:false;desc:indicates if the index is search throttled");

        table.endHeaders();
        return table;
    }

    // package private for testing
    protected Table buildTable(
        final RestRequest request,
        final Map<String, Settings> indicesSettings,
        final Map<String, ClusterIndexHealth> indicesHealths,
        final Map<String, IndexStats> indicesStats,
        final Map<String, IndexMetadata> indicesMetadatas,
        final Iterator<Tuple<String, Settings>> tableIterator,
        final PageToken pageToken
    ) {
        final String healthParam = request.param("health");
        final Table table = getTableWithHeader(request, pageToken);

        while (tableIterator.hasNext()) {
            final Tuple<String, Settings> tuple = tableIterator.next();
            String indexName = tuple.v1();
            Settings settings = tuple.v2();

            if (indicesMetadatas.containsKey(indexName) == false) {
                // the index exists in the Get Indices response but is not present in the cluster state:
                // it is likely that the index was deleted in the meanwhile, so we ignore it.
                continue;
            }

            final IndexMetadata indexMetadata = indicesMetadatas.get(indexName);
            final IndexMetadata.State indexState = indexMetadata.getState();
            final IndexStats indexStats = indicesStats.get(indexName);
            final boolean searchThrottled = IndexSettings.INDEX_SEARCH_THROTTLED.get(settings);

            final String health;
            final ClusterIndexHealth indexHealth = indicesHealths.get(indexName);
            if (indexHealth != null) {
                health = indexHealth.getStatus().toString().toLowerCase(Locale.ROOT);
            } else if (indexStats != null) {
                health = "red*";
            } else {
                health = "";
            }

            if (healthParam != null) {
                final ClusterHealthStatus healthStatusFilter = ClusterHealthStatus.fromString(healthParam);
                boolean skip;
                if (indexHealth != null) {
                    // index health is known but does not match the one requested
                    skip = indexHealth.getStatus() != healthStatusFilter;
                } else {
                    // index health is unknown, skip if we don't explicitly request RED health
                    skip = ClusterHealthStatus.RED != healthStatusFilter;
                }
                if (skip) {
                    continue;
                }
            }

            final CommonStats primaryStats;
            final CommonStats totalStats;

            if (indexStats == null || indexState == IndexMetadata.State.CLOSE) {
                // TODO: expose docs stats for replicated closed indices
                primaryStats = new CommonStats();
                totalStats = new CommonStats();
            } else {
                primaryStats = indexStats.getPrimaries();
                totalStats = indexStats.getTotal();
            }
            table.startRow();
            table.addCell(health);
            table.addCell(indexState.toString().toLowerCase(Locale.ROOT));
            table.addCell(indexName);
            table.addCell(indexMetadata.getIndexUUID());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfShards());
            table.addCell(indexHealth == null ? null : indexHealth.getNumberOfReplicas());

            table.addCell(primaryStats.getDocs() == null ? null : primaryStats.getDocs().getCount());
            table.addCell(primaryStats.getDocs() == null ? null : primaryStats.getDocs().getDeleted());

            table.addCell(indexMetadata.getCreationDate());
            ZonedDateTime creationTime = ZonedDateTime.ofInstant(Instant.ofEpochMilli(indexMetadata.getCreationDate()), ZoneOffset.UTC);
            table.addCell(STRICT_DATE_TIME_FORMATTER.format(creationTime));

            table.addCell(totalStats.getStore() == null ? null : totalStats.getStore().size());
            table.addCell(primaryStats.getStore() == null ? null : primaryStats.getStore().size());

            table.addCell(totalStats.getCompletion() == null ? null : totalStats.getCompletion().getSize());
            table.addCell(primaryStats.getCompletion() == null ? null : primaryStats.getCompletion().getSize());

            table.addCell(totalStats.getFieldData() == null ? null : totalStats.getFieldData().getMemorySize());
            table.addCell(primaryStats.getFieldData() == null ? null : primaryStats.getFieldData().getMemorySize());

            table.addCell(totalStats.getFieldData() == null ? null : totalStats.getFieldData().getEvictions());
            table.addCell(primaryStats.getFieldData() == null ? null : primaryStats.getFieldData().getEvictions());

            table.addCell(totalStats.getQueryCache() == null ? null : totalStats.getQueryCache().getMemorySize());
            table.addCell(primaryStats.getQueryCache() == null ? null : primaryStats.getQueryCache().getMemorySize());

            table.addCell(totalStats.getQueryCache() == null ? null : totalStats.getQueryCache().getEvictions());
            table.addCell(primaryStats.getQueryCache() == null ? null : primaryStats.getQueryCache().getEvictions());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getMemorySize());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getMemorySize());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getEvictions());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getEvictions());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getHitCount());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getHitCount());

            table.addCell(totalStats.getRequestCache() == null ? null : totalStats.getRequestCache().getMissCount());
            table.addCell(primaryStats.getRequestCache() == null ? null : primaryStats.getRequestCache().getMissCount());

            table.addCell(totalStats.getFlush() == null ? null : totalStats.getFlush().getTotal());
            table.addCell(primaryStats.getFlush() == null ? null : primaryStats.getFlush().getTotal());

            table.addCell(totalStats.getFlush() == null ? null : totalStats.getFlush().getTotalTime());
            table.addCell(primaryStats.getFlush() == null ? null : primaryStats.getFlush().getTotalTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().current());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().current());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getCount());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getExistsTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getExistsTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getExistsCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getExistsCount());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getMissingTime());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getMissingTime());

            table.addCell(totalStats.getGet() == null ? null : totalStats.getGet().getMissingCount());
            table.addCell(primaryStats.getGet() == null ? null : primaryStats.getGet().getMissingCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteCurrent());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteCurrent());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteTime());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteTime());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getDeleteCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getDeleteCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexCurrent());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexCurrent());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexTime());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexTime());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexCount());

            table.addCell(totalStats.getIndexing() == null ? null : totalStats.getIndexing().getTotal().getIndexFailedCount());
            table.addCell(primaryStats.getIndexing() == null ? null : primaryStats.getIndexing().getTotal().getIndexFailedCount());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrent());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrent());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrentNumDocs());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrentNumDocs());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getCurrentSize());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getCurrentSize());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotal());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotal());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalNumDocs());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalNumDocs());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalSize());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalSize());

            table.addCell(totalStats.getMerge() == null ? null : totalStats.getMerge().getTotalTime());
            table.addCell(primaryStats.getMerge() == null ? null : primaryStats.getMerge().getTotalTime());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getTotal());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getTotal());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getTotalTime());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getTotalTime());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getExternalTotal());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getExternalTotal());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getExternalTotalTime());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getExternalTotalTime());

            table.addCell(totalStats.getRefresh() == null ? null : totalStats.getRefresh().getListeners());
            table.addCell(primaryStats.getRefresh() == null ? null : primaryStats.getRefresh().getListeners());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getFetchCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getFetchCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getOpenContexts());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getOpenContexts());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getQueryCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getQueryCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getConcurrentQueryCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getConcurrentQueryCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getConcurrentQueryTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getConcurrentQueryTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getConcurrentQueryCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getConcurrentQueryCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getConcurrentAvgSliceCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getConcurrentAvgSliceCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getScrollCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getScrollCount());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getPitCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getPitCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getPitTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getPitTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getPitCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getPitCount());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getCount());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getCount());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getZeroMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getZeroMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getIndexWriterMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getIndexWriterMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getVersionMapMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getVersionMapMemory());

            table.addCell(totalStats.getSegments() == null ? null : totalStats.getSegments().getBitsetMemory());
            table.addCell(primaryStats.getSegments() == null ? null : primaryStats.getSegments().getBitsetMemory());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().current());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().current());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().total());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().total());

            table.addCell(totalStats.getWarmer() == null ? null : totalStats.getWarmer().totalTime());
            table.addCell(primaryStats.getWarmer() == null ? null : primaryStats.getWarmer().totalTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestCurrent());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestCurrent());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestTime());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestTime());

            table.addCell(totalStats.getSearch() == null ? null : totalStats.getSearch().getTotal().getSuggestCount());
            table.addCell(primaryStats.getSearch() == null ? null : primaryStats.getSearch().getTotal().getSuggestCount());

            table.addCell(totalStats.getTotalMemory());
            table.addCell(primaryStats.getTotalMemory());

            table.addCell(searchThrottled);

            table.endRow();

        }

        return table;
    }

    @SuppressWarnings("unchecked")
    private static <A extends ActionResponse> A extractResponse(final Collection<? extends ActionResponse> responses, Class<A> c) {
        return (A) responses.stream().filter(c::isInstance).findFirst().get();
    }

    @Override
    public boolean isActionPaginated() {
        return false;
    }

    protected IndexPaginationStrategy getPaginationStrategy(ClusterStateResponse clusterStateResponse) {
        return null;
    }

    /**
     * Provides the iterator to be used for building the response table.
     */
    protected Iterator<Tuple<String, Settings>> getTableIterator(String[] indices, Map<String, Settings> indexSettingsMap) {
        return new Iterator<>() {
            final Iterator<String> settingsMapIter = indexSettingsMap.keySet().iterator();

            @Override
            public boolean hasNext() {
                return settingsMapIter.hasNext();
            }

            @Override
            public Tuple<String, Settings> next() {
                String index = settingsMapIter.next();
                return new Tuple<>(index, indexSettingsMap.get(index));
            }
        };
    }

    private boolean shouldSkipIndicesStatsRequest(IndexPaginationStrategy paginationStrategy) {
        return Objects.nonNull(paginationStrategy) && paginationStrategy.getRequestedEntities().isEmpty();
    }

}
