/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search;

import org.opensearch.action.search.SearchPhaseName;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.protobufs.ClusterStatistics;

import java.io.IOException;
import java.util.Map;

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
        org.opensearch.protobufs.SearchResponse.Builder searchResponseProtoBuilder = org.opensearch.protobufs.SearchResponse.newBuilder();
        toProto(response, searchResponseProtoBuilder);
        return searchResponseProtoBuilder.build();
    }

    /**
     * Converts a SearchResponse to its Protocol Buffer representation.
     * Similar to {@link SearchResponse#innerToXContent(XContentBuilder, ToXContent.Params)}
     *
     * @param response The SearchResponse to convert
     * @param searchResponseProtoBuilder The builder to populate with the SearchResponse data
     * @throws IOException if there's an error during conversion
     */
    public static void toProto(SearchResponse response, org.opensearch.protobufs.SearchResponse.Builder searchResponseProtoBuilder)
        throws IOException {

        // Set optional fields only if they exist
        if (response.getScrollId() != null) {
            searchResponseProtoBuilder.setXScrollId(response.getScrollId());
        }
        if (response.pointInTimeId() != null) {
            searchResponseProtoBuilder.setPitId(response.pointInTimeId());
        }

        // Set required fields
        searchResponseProtoBuilder.setTook(response.getTook().getMillis());
        searchResponseProtoBuilder.setTimedOut(response.isTimedOut());

        // Set phase took information if available
        if (response.getPhaseTook() != null) {
            org.opensearch.protobufs.PhaseTook.Builder phaseTookBuilder = org.opensearch.protobufs.PhaseTook.newBuilder();
            PhaseTookProtoUtils.toProto(response.getPhaseTook(), phaseTookBuilder);
            searchResponseProtoBuilder.setPhaseTook(phaseTookBuilder.build());
        }

        // Set optional fields only if they differ from defaults
        if (response.isTerminatedEarly() != null) {
            searchResponseProtoBuilder.setTerminatedEarly(response.isTerminatedEarly());
        }
        if (response.getNumReducePhases() != 1) {
            searchResponseProtoBuilder.setNumReducePhases(response.getNumReducePhases());
        }

        // Build broadcast shards header
        ProtoActionsProtoUtils.buildBroadcastShardsHeader(
            searchResponseProtoBuilder,
            response.getTotalShards(),
            response.getSuccessfulShards(),
            response.getSkippedShards(),
            response.getFailedShards(),
            response.getShardFailures()
        );

        // Add clusters information
        ClustersProtoUtils.toProto(searchResponseProtoBuilder, response.getClusters());

        // Add search response sections
        SearchResponseSectionsProtoUtils.toProto(searchResponseProtoBuilder, response);

    }

    /**
     * Utility class for converting PhaseTook components between OpenSearch and Protocol Buffers formats.
     * This class handles the transformation of phase timing information to ensure proper reporting
     * of search phase execution times.
     */
    protected static class PhaseTookProtoUtils {
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
         * @param phaseTook The PhaseTook to convert
         * @param phaseTookProtoBuilder The builder to populate with the PhaseTook data
         */
        protected static void toProto(
            SearchResponse.PhaseTook phaseTook,
            org.opensearch.protobufs.PhaseTook.Builder phaseTookProtoBuilder
        ) {
            if (phaseTook == null) {
                return;
            }

            // Get the phase took map once to avoid repeated method calls
            Map<String, Long> phaseTookMap = phaseTook.getPhaseTookMap();

            // Process each search phase
            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                // Get the value from the map or default to 0
                long value = phaseTookMap.getOrDefault(searchPhaseName.getName(), 0L);

                // Set the appropriate field based on the phase name
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
        }

    }

    /**
     * Utility class for converting Clusters components between OpenSearch and Protocol Buffers formats.
     * This class handles the transformation of cluster statistics information to ensure proper reporting
     * of cross-cluster search results.
     */
    protected static class ClustersProtoUtils {
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
         * @param protoResponseBuilder The builder to populate with the Clusters data
         * @param clusters The Clusters to convert
         */
        protected static void toProto(
            org.opensearch.protobufs.SearchResponse.Builder protoResponseBuilder,
            SearchResponse.Clusters clusters
        ) {
            // Only add clusters information if there are clusters
            if (clusters.getTotal() > 0) {
                // Create and populate the cluster statistics builder
                ClusterStatistics.Builder clusterStatistics = ClusterStatistics.newBuilder()
                    .setTotal(clusters.getTotal())
                    .setSuccessful(clusters.getSuccessful())
                    .setSkipped(clusters.getSkipped());

                // Set the clusters field in the response builder
                protoResponseBuilder.setXClusters(clusterStatistics.build());
            }
        }
    }
}
