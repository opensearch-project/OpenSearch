/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.plugin.transport.grpc.proto.response.search;

import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ClusterStatistics;

import java.io.IOException;

/**
 * Utility class for converting SearchResponse objects to Protocol Buffers.
 * This class handles the conversion of search operation responses to their
 * Protocol Buffer representation.
 */
public class SearchResponseProtoUtils {

    private SearchResponseProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a SearchResponse to its Protocol Buffer representation.
     * This method is equivalent to {@link SearchResponse#toXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response The SearchResponse to convert
     * @return A Protocol Buffer SearchResponse representation
     * @throws IOException if there's an error during conversion
     */
    public static org.opensearch.protobufs.SearchResponse toProto(SearchResponse response) throws IOException {
        return innerToProto(response);
    }

    /**
     * Similar to {@link SearchResponse#innerToXContent(XContentBuilder, ToXContent.Params)}
     */
    private static org.opensearch.protobufs.SearchResponse innerToProto(SearchResponse response) throws IOException {
        org.opensearch.protobufs.SearchResponse.Builder searchResponseProtoBuilder = org.opensearch.protobufs.SearchResponse.newBuilder();
        org.opensearch.protobufs.ResponseBody.Builder searchResponseBodyProtoBuilder = org.opensearch.protobufs.ResponseBody.newBuilder();

        if (response.getScrollId() != null) {
            searchResponseBodyProtoBuilder.setScrollId(response.getScrollId());
        }
        if (response.pointInTimeId() != null) {
            searchResponseBodyProtoBuilder.setPitId(response.pointInTimeId());
        }

        searchResponseBodyProtoBuilder.setTook(response.getTook().getMillis());

        if (response.getPhaseTook() != null) {
            searchResponseBodyProtoBuilder.setPhaseTook(PhaseTookProtoUtils.toProto(response.getPhaseTook()));
        }

        searchResponseBodyProtoBuilder.setTimedOut(response.isTimedOut());

        if (response.isTerminatedEarly() != null) {
            searchResponseBodyProtoBuilder.setTerminatedEarly(response.isTerminatedEarly());
        }
        if (response.getNumReducePhases() != 1) {
            searchResponseBodyProtoBuilder.setNumReducePhases(response.getNumReducePhases());
        }

        ProtoActionsProtoUtils.buildBroadcastShardsHeader(
            searchResponseBodyProtoBuilder,
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getFailedShards(),
            response.getShardFailures()
        );

        ClustersProtoUtils.toProto(searchResponseBodyProtoBuilder, response.getClusters());
        SearchResponseSectionsProtoUtils.toProto(searchResponseBodyProtoBuilder, response);

        searchResponseProtoBuilder.setResponseBody(searchResponseBodyProtoBuilder.build());

        return searchResponseProtoBuilder.build();
    }

    /**
     * Utility class for converting PhaseTook components between OpenSearch and Protocol Buffers formats.
     * This class handles the transformation of phase timing information to ensure proper reporting
     * of search phase execution times.
     */
    public static class PhaseTookProtoUtils {
        /**
         * Private constructor to prevent instantiation.
         * This is a utility class with only static methods.
         */
        private PhaseTookProtoUtils() {
            // Utility class, no instances
        }

        /**
         * Similar to {@link SearchResponse.PhaseTook#toXContent(XContentBuilder, ToXContent.Params)}
         *
         * @param phaseTook
         * @return
         */
        public static org.opensearch.protobufs.PhaseTook toProto(SearchResponse.PhaseTook phaseTook) {

            org.opensearch.protobufs.PhaseTook.Builder phaseTookProtoBuilder = org.opensearch.protobufs.PhaseTook.newBuilder();

            if (phaseTook == null) {
                return phaseTookProtoBuilder.build();
            }

            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                long value;
                if (phaseTook.getPhaseTookMap().containsKey(searchPhaseName.getName())) {
                    value = phaseTook.getPhaseTookMap().get(searchPhaseName.getName());
                } else {
                    value = 0;
                }

                switch (searchPhaseName) {
                    case DFS_PRE_QUERY:
                        phaseTookProtoBuilder.setDfsPreQuery(value);
                        break;
                    case QUERY:
                        phaseTookProtoBuilder.setQuery(value);
                        break;
                    case FETCH:
                        phaseTookProtoBuilder.setFetch(value);
                        break;
                    case DFS_QUERY:
                        phaseTookProtoBuilder.setDfsQuery(value);
                        break;
                    case EXPAND:
                        phaseTookProtoBuilder.setExpand(value);
                        break;
                    case CAN_MATCH:
                        phaseTookProtoBuilder.setCanMatch(value);
                        break;
                    default:
                        throw new UnsupportedOperationException("searchPhaseName cannot be converted to phaseTook protobuf type");
                }
            }
            return phaseTookProtoBuilder.build();
        }

    }

    /**
     * Utility class for converting Clusters components between OpenSearch and Protocol Buffers formats.
     * This class handles the transformation of cluster statistics information to ensure proper reporting
     * of cross-cluster search results.
     */
    public static class ClustersProtoUtils {
        /**
         * Private constructor to prevent instantiation.
         * This is a utility class with only static methods.
         */
        private ClustersProtoUtils() {
            // Utility class, no instances
        }

        /**
         * Similar to {@link SearchResponse.Clusters#toXContent(XContentBuilder, ToXContent.Params)}
         *
         * @param protoResponseBuilder
         * @param clusters
         * @throws IOException
         */
        public static void toProto(org.opensearch.protobufs.ResponseBody.Builder protoResponseBuilder, SearchResponse.Clusters clusters)
            throws IOException {

            if (clusters.getTotal() > 0) {
                ClusterStatistics.Builder clusterStatistics = ClusterStatistics.newBuilder();
                clusterStatistics.setTotal(clusters.getTotal());
                clusterStatistics.setSuccessful(clusters.getSuccessful());
                clusterStatistics.setSkipped(clusters.getSkipped());

                protoResponseBuilder.setClusters(clusterStatistics.build());
            }

        }
    }
}
