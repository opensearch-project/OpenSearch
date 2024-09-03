/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.cat;

import org.opensearch.action.admin.indices.segments.IndexSegments;
import org.opensearch.action.admin.indices.segments.IndexShardSegments;
import org.opensearch.action.admin.indices.segments.IndicesSegmentResponse;
import org.opensearch.action.admin.indices.segments.PitSegmentsAction;
import org.opensearch.action.admin.indices.segments.PitSegmentsRequest;
import org.opensearch.action.admin.indices.segments.ShardSegments;
import org.opensearch.client.node.NodeClient;
import org.opensearch.cluster.node.DiscoveryNodes;
import org.opensearch.common.Table;
import org.opensearch.index.engine.Segment;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.RestResponse;
import org.opensearch.rest.action.RestResponseListener;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

import static java.util.Arrays.asList;
import static java.util.Collections.unmodifiableList;
import static org.opensearch.rest.RestRequest.Method.GET;

/**
 * Rest action for pit segments
 */
public class RestPitSegmentsAction extends AbstractCatAction {
    private final Supplier<DiscoveryNodes> nodesInCluster;

    public RestPitSegmentsAction(Supplier<DiscoveryNodes> nodesInCluster) {
        super();
        this.nodesInCluster = nodesInCluster;
    }

    @Override
    public List<RestHandler.Route> routes() {
        return unmodifiableList(asList(new Route(GET, "/_cat/pit_segments/_all"), new Route(GET, "/_cat/pit_segments")));
    }

    @Override
    public String getName() {
        return "cat_pit_segments_action";
    }

    @Override
    public boolean allowSystemIndexAccessByDefault() {
        return true;
    }

    @Override
    protected BaseRestHandler.RestChannelConsumer doCatRequest(final RestRequest request, final NodeClient client) {
        String allPitIdsQualifier = "_all";
        final PitSegmentsRequest pitSegmentsRequest;
        if (request.path().contains(allPitIdsQualifier)) {
            pitSegmentsRequest = new PitSegmentsRequest(allPitIdsQualifier);
        } else {
            pitSegmentsRequest = new PitSegmentsRequest();
            try {
                request.withContentOrSourceParamParserOrNull((xContentParser -> {
                    if (xContentParser != null) {
                        pitSegmentsRequest.fromXContent(xContentParser);
                    }
                }));
            } catch (IOException e) {
                throw new IllegalArgumentException("Failed to parse request body", e);
            }
        }
        return channel -> client.execute(PitSegmentsAction.INSTANCE, pitSegmentsRequest, new RestResponseListener<>(channel) {
            @Override
            public RestResponse buildResponse(final IndicesSegmentResponse indicesSegmentResponse) throws Exception {
                final Map<String, IndexSegments> indicesSegments = indicesSegmentResponse.getIndices();
                Table tab = buildTable(request, indicesSegments);
                return RestTable.buildResponse(tab, channel);
            }
        });
    }

    @Override
    public void documentation(StringBuilder sb) {
        sb.append("/_cat/pit_segments\n");
        sb.append("/_cat/pit_segments/{pit_id}\n");
    }

    @Override
    protected Table getTableWithHeader(RestRequest request) {
        Table table = new Table();
        table.startHeaders();
        table.addCell("index", "default:true;alias:i,idx;desc:index name");
        table.addCell("shard", "default:true;alias:s,sh;desc:shard name");
        table.addCell("prirep", "alias:p,pr,primaryOrReplica;default:true;desc:primary or replica");
        table.addCell("ip", "default:true;desc:ip of node where it lives");
        table.addCell("id", "default:false;desc:unique id of node where it lives");
        table.addCell("segment", "default:true;alias:seg;desc:segment name");
        table.addCell("generation", "default:true;alias:g,gen;text-align:right;desc:segment generation");
        table.addCell("docs.count", "default:true;alias:dc,docsCount;text-align:right;desc:number of docs in segment");
        table.addCell("docs.deleted", "default:true;alias:dd,docsDeleted;text-align:right;desc:number of deleted docs in segment");
        table.addCell("size", "default:true;alias:si;text-align:right;desc:segment size in bytes");
        table.addCell("size.memory", "default:true;alias:sm,sizeMemory;text-align:right;desc:segment memory in bytes");
        table.addCell("committed", "default:true;alias:ic,isCommitted;desc:is segment committed");
        table.addCell("searchable", "default:true;alias:is,isSearchable;desc:is segment searched");
        table.addCell("version", "default:true;alias:v,ver;desc:version");
        table.addCell("compound", "default:true;alias:ico,isCompound;desc:is segment compound");
        table.endHeaders();
        return table;
    }

    private Table buildTable(final RestRequest request, Map<String, IndexSegments> indicesSegments) {
        Table table = getTableWithHeader(request);

        DiscoveryNodes nodes = this.nodesInCluster.get();

        for (IndexSegments indexSegments : indicesSegments.values()) {
            Map<Integer, IndexShardSegments> shards = indexSegments.getShards();
            for (IndexShardSegments indexShardSegments : shards.values()) {
                ShardSegments[] shardSegments = indexShardSegments.getShards();
                for (ShardSegments shardSegment : shardSegments) {
                    List<Segment> segments = shardSegment.getSegments();
                    for (Segment segment : segments) {
                        table.startRow();
                        table.addCell(shardSegment.getShardRouting().getIndexName());
                        table.addCell(shardSegment.getShardRouting().getId());
                        table.addCell(shardSegment.getShardRouting().primary() ? "p" : "r");
                        table.addCell(nodes.get(shardSegment.getShardRouting().currentNodeId()).getHostAddress());
                        table.addCell(shardSegment.getShardRouting().currentNodeId());
                        table.addCell(segment.getName());
                        table.addCell(segment.getGeneration());
                        table.addCell(segment.getNumDocs());
                        table.addCell(segment.getDeletedDocs());
                        table.addCell(segment.getSize());
                        table.addCell(0L);
                        table.addCell(segment.isCommitted());
                        table.addCell(segment.isSearch());
                        table.addCell(segment.getVersion());
                        table.addCell(segment.isCompound());
                        table.endRow();

                    }
                }
            }
        }
        return table;
    }
}
