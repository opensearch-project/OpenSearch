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

import org.opensearch.action.admin.cluster.shards.CatShardsAction;
import org.opensearch.action.admin.cluster.shards.CatShardsRequest;
import org.opensearch.action.admin.cluster.shards.CatShardsResponse;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.pagination.PageToken;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.UnassignedInfo;
import org.opensearch.common.Table;
import org.opensearch.common.logging.DeprecationLogger;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.Strings;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.CommitStats;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.get.GetStats;
import org.opensearch.index.merge.MergeStats;
import org.opensearch.index.refresh.RefreshStats;
import org.opensearch.index.search.stats.SearchStats;
import org.opensearch.index.seqno.SeqNoStats;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.warmer.WarmerStats;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;
import org.opensearch.rest.action.list.AbstractListAction;
import org.opensearch.search.suggest.completion.CompletionStats;

import java.time.Instant;
import java.util.List;
import java.util.Locale;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;
import static org.opensearch.search.SearchService.NO_TIMEOUT;

/**
 * _cat API action to get shard information
 *
 * @opensearch.api
 */
public class RestShardsAction extends AbstractListAction {

    private static final DeprecationLogger deprecationLogger = DeprecationLogger.getLogger(RestShardsAction.class);

    @Override
    public List<Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/shards"), new Route(GET, "/_cat/shards/{index}")));
    }

    @Override
    public String getName() {
        return "cat_shards_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected void documentation(StringBuilder sb) {
        sb.append("/_cat/shards\n");
        sb.append("/_cat/shards/{index}\n");
    }

    @Override
    public boolean isRequestLimitCheckSupported() {
        return true;
    }

    @Override
    public RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        final String[] indices = Strings.splitStringByCommaToArray(request.param("index"));
        final CatShardsRequest shardsRequest = new CatShardsRequest();
        shardsRequest.local(request.paramAsBoolean("local", shardsRequest.local()));
        shardsRequest.clusterManagerNodeTimeout(request.paramAsTime("cluster_manager_timeout", shardsRequest.clusterManagerNodeTimeout()));
        shardsRequest.setCancelAfterTimeInterval(request.paramAsTime("cancel_after_time_interval", NO_TIMEOUT));
        shardsRequest.setIndices(indices);
        shardsRequest.setRequestLimitCheckSupported(isRequestLimitCheckSupported());
        shardsRequest.setPageParams(pageParams);
        parseDeprecatedMasterTimeoutParameter(shardsRequest, request, deprecationLogger, getName());
        return channel -> client.execute(CatShardsAction.INSTANCE, shardsRequest, new RestResponseListener<CatShardsResponse>(channel) {
            @Override
            public RestResponse buildResponse(CatShardsResponse catShardsResponse) throws Exception {
                return RestTable.buildResponse(
                    buildTable(
                        request,
                        catShardsResponse.getNodes(),
                        catShardsResponse.getIndicesStatsResponse(),
                        catShardsResponse.getResponseShards(),
                        catShardsResponse.getPageToken()
                    ),
                    channel
                );
            }
        });
    }

    @Override
    protected Table getTableWithHeader(final RestRequest request) {
        return getTableWithHeader(request, null);
    }

    protected Table getTableWithHeader(final RestRequest request, final PageToken pageToken) {
        Table table = new Table(pageToken);
        table.startHeaders()
            .addCell("index", "default:true;alias:i,idx;desc:index name")
            .addCell("shard", "default:true;alias:s,sh;desc:shard name")
            .addCell("prirep", "alias:p,pr,primaryOrReplica;default:true;desc:primary or replica")
            .addCell("state", "default:true;alias:st;desc:shard state")
            .addCell("docs", "alias:d,dc;text-align:right;desc:number of docs in shard")
            .addCell("store", "alias:sto;text-align:right;desc:store size of shard (how much disk it uses)")
            .addCell("ip", "default:true;desc:ip of node where it lives")
            .addCell("id", "default:false;desc:unique id of node where it lives")
            .addCell("node", "default:true;alias:n;desc:name of node where it lives");

        table.addCell("sync_id", "alias:sync_id;default:false;desc:sync id");

        table.addCell("unassigned.reason", "alias:ur;default:false;desc:reason shard is unassigned");
        table.addCell("unassigned.at", "alias:ua;default:false;desc:time shard became unassigned (UTC)");
        table.addCell("unassigned.for", "alias:uf;default:false;text-align:right;desc:time has been unassigned");
        table.addCell("unassigned.details", "alias:ud;default:false;desc:additional details as to why the shard became unassigned");

        table.addCell("recoverysource.type", "alias:rs;default:false;desc:recovery source type");

        table.addCell("completion.size", "alias:cs,completionSize;default:false;text-align:right;desc:size of completion");

        table.addCell("fielddata.memory_size", "alias:fm,fielddataMemory;default:false;text-align:right;desc:used fielddata cache");
        table.addCell("fielddata.evictions", "alias:fe,fielddataEvictions;default:false;text-align:right;desc:fielddata evictions");

        table.addCell("query_cache.memory_size", "alias:qcm,queryCacheMemory;default:false;text-align:right;desc:used query cache");
        table.addCell("query_cache.evictions", "alias:qce,queryCacheEvictions;default:false;text-align:right;desc:query cache evictions");

        table.addCell("flush.total", "alias:ft,flushTotal;default:false;text-align:right;desc:number of flushes");
        table.addCell("flush.total_time", "alias:ftt,flushTotalTime;default:false;text-align:right;desc:time spent in flush");

        table.addCell("get.current", "alias:gc,getCurrent;default:false;text-align:right;desc:number of current get ops");
        table.addCell("get.time", "alias:gti,getTime;default:false;text-align:right;desc:time spent in get");
        table.addCell("get.total", "alias:gto,getTotal;default:false;text-align:right;desc:number of get ops");
        table.addCell("get.exists_time", "alias:geti,getExistsTime;default:false;text-align:right;desc:time spent in successful gets");
        table.addCell("get.exists_total", "alias:geto,getExistsTotal;default:false;text-align:right;desc:number of successful gets");
        table.addCell("get.missing_time", "alias:gmti,getMissingTime;default:false;text-align:right;desc:time spent in failed gets");
        table.addCell("get.missing_total", "alias:gmto,getMissingTotal;default:false;text-align:right;desc:number of failed gets");

        table.addCell(
            "indexing.delete_current",
            "alias:idc,indexingDeleteCurrent;default:false;text-align:right;desc:number of current deletions"
        );
        table.addCell("indexing.delete_time", "alias:idti,indexingDeleteTime;default:false;text-align:right;desc:time spent in deletions");
        table.addCell("indexing.delete_total", "alias:idto,indexingDeleteTotal;default:false;text-align:right;desc:number of delete ops");
        table.addCell(
            "indexing.index_current",
            "alias:iic,indexingIndexCurrent;default:false;text-align:right;desc:number of current indexing ops"
        );
        table.addCell("indexing.index_time", "alias:iiti,indexingIndexTime;default:false;text-align:right;desc:time spent in indexing");
        table.addCell("indexing.index_total", "alias:iito,indexingIndexTotal;default:false;text-align:right;desc:number of indexing ops");
        table.addCell(
            "indexing.index_failed",
            "alias:iif,indexingIndexFailed;default:false;text-align:right;desc:number of failed indexing ops"
        );

        table.addCell("merges.current", "alias:mc,mergesCurrent;default:false;text-align:right;desc:number of current merges");
        table.addCell(
            "merges.current_docs",
            "alias:mcd,mergesCurrentDocs;default:false;text-align:right;desc:number of current merging docs"
        );
        table.addCell("merges.current_size", "alias:mcs,mergesCurrentSize;default:false;text-align:right;desc:size of current merges");
        table.addCell("merges.total", "alias:mt,mergesTotal;default:false;text-align:right;desc:number of completed merge ops");
        table.addCell("merges.total_docs", "alias:mtd,mergesTotalDocs;default:false;text-align:right;desc:docs merged");
        table.addCell("merges.total_size", "alias:mts,mergesTotalSize;default:false;text-align:right;desc:size merged");
        table.addCell("merges.total_time", "alias:mtt,mergesTotalTime;default:false;text-align:right;desc:time spent in merges");

        table.addCell("refresh.total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total refreshes");
        table.addCell("refresh.time", "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in refreshes");
        table.addCell("refresh.external_total", "alias:rto,refreshTotal;default:false;text-align:right;desc:total external refreshes");
        table.addCell(
            "refresh.external_time",
            "alias:rti,refreshTime;default:false;text-align:right;desc:time spent in external refreshes"
        );
        table.addCell(
            "refresh.listeners",
            "alias:rli,refreshListeners;default:false;text-align:right;desc:number of pending refresh listeners"
        );

        table.addCell("search.fetch_current", "alias:sfc,searchFetchCurrent;default:false;text-align:right;desc:current fetch phase ops");
        table.addCell("search.fetch_time", "alias:sfti,searchFetchTime;default:false;text-align:right;desc:time spent in fetch phase");
        table.addCell("search.fetch_total", "alias:sfto,searchFetchTotal;default:false;text-align:right;desc:total fetch ops");
        table.addCell("search.open_contexts", "alias:so,searchOpenContexts;default:false;text-align:right;desc:open search contexts");
        table.addCell("search.query_current", "alias:sqc,searchQueryCurrent;default:false;text-align:right;desc:current query phase ops");
        table.addCell("search.query_time", "alias:sqti,searchQueryTime;default:false;text-align:right;desc:time spent in query phase");
        table.addCell("search.query_total", "alias:sqto,searchQueryTotal;default:false;text-align:right;desc:total query phase ops");
        table.addCell(
            "search.concurrent_query_current",
            "alias:scqc,searchConcurrentQueryCurrent;default:false;text-align:right;desc:current concurrent query phase ops"
        );
        table.addCell(
            "search.concurrent_query_time",
            "alias:scqti,searchConcurrentQueryTime;default:false;text-align:right;desc:time spent in concurrent query phase"
        );
        table.addCell(
            "search.concurrent_query_total",
            "alias:scqto,searchConcurrentQueryTotal;default:false;text-align:right;desc:total concurrent query phase ops"
        );
        table.addCell(
            "search.concurrent_avg_slice_count",
            "alias:casc,searchConcurrentAvgSliceCount;default:false;text-align:right;desc:average query concurrency"
        );
        table.addCell("search.scroll_current", "alias:scc,searchScrollCurrent;default:false;text-align:right;desc:open scroll contexts");
        table.addCell(
            "search.scroll_time",
            "alias:scti,searchScrollTime;default:false;text-align:right;desc:time scroll contexts held open"
        );
        table.addCell("search.scroll_total", "alias:scto,searchScrollTotal;default:false;text-align:right;desc:completed scroll contexts");
        table.addCell(
            "search.point_in_time_current",
            "alias:spc,searchPointInTimeCurrent;default:false;text-align:right;desc:open point in time contexts"
        );
        table.addCell(
            "search.point_in_time_time",
            "alias:spti,searchPointInTimeTime;default:false;text-align:right;desc:time point in time contexts held open"
        );
        table.addCell(
            "search.point_in_time_total",
            "alias:spto,searchPointInTimeTotal;default:false;text-align:right;desc:completed point in time contexts"
        );
        table.addCell(
            "search.search_idle_reactivate_count_total",
            "alias:ssirct,searchSearchIdleReactivateCountTotal;default:false;text-align:right;desc:number of times a shard reactivated"
        );

        table.addCell("segments.count", "alias:sc,segmentsCount;default:false;text-align:right;desc:number of segments");
        table.addCell("segments.memory", "alias:sm,segmentsMemory;default:false;text-align:right;desc:memory used by segments");
        table.addCell(
            "segments.index_writer_memory",
            "alias:siwm,segmentsIndexWriterMemory;default:false;text-align:right;desc:memory used by index writer"
        );
        table.addCell(
            "segments.version_map_memory",
            "alias:svmm,segmentsVersionMapMemory;default:false;text-align:right;desc:memory used by version map"
        );
        table.addCell(
            "segments.fixed_bitset_memory",
            "alias:sfbm,fixedBitsetMemory;default:false;text-align:right;desc:memory used by fixed bit sets for nested object"
                + " field types and type filters for types referred in _parent fields"
        );

        table.addCell("seq_no.max", "alias:sqm,maxSeqNo;default:false;text-align:right;desc:max sequence number");
        table.addCell("seq_no.local_checkpoint", "alias:sql,localCheckpoint;default:false;text-align:right;desc:local checkpoint");
        table.addCell("seq_no.global_checkpoint", "alias:sqg,globalCheckpoint;default:false;text-align:right;desc:global checkpoint");

        table.addCell("warmer.current", "alias:wc,warmerCurrent;default:false;text-align:right;desc:current warmer ops");
        table.addCell("warmer.total", "alias:wto,warmerTotal;default:false;text-align:right;desc:total warmer ops");
        table.addCell("warmer.total_time", "alias:wtt,warmerTotalTime;default:false;text-align:right;desc:time spent in warmers");

        table.addCell("path.data", "alias:pd,dataPath;default:false;text-align:right;desc:shard data path");
        table.addCell("path.state", "alias:ps,statsPath;default:false;text-align:right;desc:shard state path");
        table.addCell("docs.deleted", "alias:dd,docsDeleted;default:false;text-align:right;desc:number of deleted docs in shard");

        table.endHeaders();
        return table;
    }

    private static <S, T> Object getOrNull(S stats, Function<S, T> accessor, Function<T, Object> func) {
        if (stats != null) {
            T t = accessor.apply(stats);
            if (t != null) {
                return func.apply(t);
            }
        }
        return null;
    }

    // package private for testing
    Table buildTable(
        RestRequest request,
        DiscoveryNodes nodes,
        IndicesStatsResponse stats,
        List<ShardRouting> responseShards,
        PageToken pageToken
    ) {
        Table table = getTableWithHeader(request, pageToken);

        for (ShardRouting shard : responseShards) {
            ShardStats shardStats = stats.asMap().get(shard);
            CommonStats commonStats = null;
            CommitStats commitStats = null;
            if (shardStats != null) {
                commonStats = shardStats.getStats();
                commitStats = shardStats.getCommitStats();
            }

            table.startRow();

            table.addCell(shard.getIndexName());
            table.addCell(shard.id());

            if (shard.primary()) {
                table.addCell("p");
            } else {
                if (shard.isSearchOnly()) {
                    table.addCell("s");
                } else {
                    table.addCell("r");
                }
            }
            table.addCell(shard.state());
            table.addCell(getOrNull(commonStats, CommonStats::getDocs, DocsStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getStore, StoreStats::getSize));
            if (shard.assignedToNode()) {
                String ip = nodes.get(shard.currentNodeId()).getHostAddress();
                String nodeId = shard.currentNodeId();
                StringBuilder name = new StringBuilder();
                name.append(nodes.get(shard.currentNodeId()).getName());
                if (shard.relocating()) {
                    String reloIp = nodes.get(shard.relocatingNodeId()).getHostAddress();
                    String reloNme = nodes.get(shard.relocatingNodeId()).getName();
                    String reloNodeId = shard.relocatingNodeId();
                    name.append(" -> ");
                    name.append(reloIp);
                    name.append(" ");
                    name.append(reloNodeId);
                    name.append(" ");
                    name.append(reloNme);
                }
                table.addCell(ip);
                table.addCell(nodeId);
                table.addCell(name);
            } else {
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
            }

            table.addCell(commitStats == null ? null : commitStats.getUserData().get(Engine.SYNC_COMMIT_ID));

            if (shard.unassignedInfo() != null) {
                table.addCell(shard.unassignedInfo().getReason());
                Instant unassignedTime = Instant.ofEpochMilli(shard.unassignedInfo().getUnassignedTimeInMillis());
                table.addCell(UnassignedInfo.DATE_TIME_FORMATTER.format(unassignedTime));
                table.addCell(TimeValue.timeValueMillis(System.currentTimeMillis() - shard.unassignedInfo().getUnassignedTimeInMillis()));
                table.addCell(shard.unassignedInfo().getDetails());
            } else {
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
                table.addCell(null);
            }

            if (shard.recoverySource() != null) {
                table.addCell(shard.recoverySource().getType().toString().toLowerCase(Locale.ROOT));
            } else {
                table.addCell(null);
            }

            table.addCell(getOrNull(commonStats, CommonStats::getCompletion, CompletionStats::getSize));

            table.addCell(getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getMemorySize));
            table.addCell(getOrNull(commonStats, CommonStats::getFieldData, FieldDataStats::getEvictions));

            table.addCell(getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getMemorySize));
            table.addCell(getOrNull(commonStats, CommonStats::getQueryCache, QueryCacheStats::getEvictions));

            table.addCell(getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getFlush, FlushStats::getTotalTime));

            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::current));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getExistsCount));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingTime));
            table.addCell(getOrNull(commonStats, CommonStats::getGet, GetStats::getMissingCount));

            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getDeleteCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getIndexing, i -> i.getTotal().getIndexFailedCount()));

            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrent));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentNumDocs));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getCurrentSize));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalNumDocs));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalSize));
            table.addCell(getOrNull(commonStats, CommonStats::getMerge, MergeStats::getTotalTime));

            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getTotalTime));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getExternalTotal));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getExternalTotalTime));
            table.addCell(getOrNull(commonStats, CommonStats::getRefresh, RefreshStats::getListeners));

            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getFetchCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, SearchStats::getOpenContexts));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getQueryCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getConcurrentQueryCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getConcurrentQueryTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getConcurrentQueryCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getConcurrentAvgSliceCount()));

            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getScrollCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getPitCurrent()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getPitTime()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getPitCount()));
            table.addCell(getOrNull(commonStats, CommonStats::getSearch, i -> i.getTotal().getSearchIdleReactivateCount()));

            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getCount));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getZeroMemory));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getIndexWriterMemory));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getVersionMapMemory));
            table.addCell(getOrNull(commonStats, CommonStats::getSegments, SegmentsStats::getBitsetMemory));

            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getMaxSeqNo));
            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getLocalCheckpoint));
            table.addCell(getOrNull(shardStats, ShardStats::getSeqNoStats, SeqNoStats::getGlobalCheckpoint));

            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::current));
            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::total));
            table.addCell(getOrNull(commonStats, CommonStats::getWarmer, WarmerStats::totalTime));

            table.addCell(getOrNull(shardStats, ShardStats::getDataPath, s -> s));
            table.addCell(getOrNull(shardStats, ShardStats::getStatePath, s -> s));
            table.addCell(getOrNull(commonStats, CommonStats::getDocs, DocsStats::getDeleted));

            table.endRow();
        }

        return table;
    }

    @Override
    public boolean isActionPaginated() {
        return false;
    }
}
