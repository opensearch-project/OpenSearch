/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.document.bulk;

import org.opensearch.action.bulk.BulkShardRequest;
import org.opensearch.protobufs.BulkRequest;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.document.RestBulkAction;
import org.opensearch.search.fetch.subphase.FetchSourceContext;
import org.opensearch.transport.client.Requests;
import org.opensearch.transport.client.node.NodeClient;
import org.opensearch.transport.grpc.proto.request.common.FetchSourceContextProtoUtils;
import org.opensearch.transport.grpc.proto.request.common.RefreshProtoUtils;

/**
 * Utility class for converting protobuf BulkRequest to OpenSearch BulkRequest.
 * <p>
 * This class provides functionality similar to the REST layer's bulk request processing.
 * The parameter mapping and processing logic should be kept consistent with the corresponding
 * REST implementation to ensure feature parity between gRPC and HTTP APIs.
 *
 * @see org.opensearch.rest.action.document.RestBulkAction#prepareRequest(RestRequest, NodeClient) REST equivalent for parameter processing
 * @see org.opensearch.action.bulk.BulkRequest OpenSearch internal bulk request representation
 * @see org.opensearch.protobufs.BulkRequest Protobuf definition for gRPC bulk requests
 */
public class BulkRequestProtoUtils {
    // protected final Settings settings;

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private BulkRequestProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a protobuf BulkRequest to an OpenSearch BulkRequest.
     * <p>
     * This method processes bulk request parameters and prepares the request for execution,
     * similar to {@link RestBulkAction#prepareRequest(RestRequest, NodeClient)}.
     * Please ensure to keep both implementations consistent for feature parity.
     *
     * @param request the protobuf bulk request to convert
     * @return the corresponding OpenSearch BulkRequest ready for execution
     * @see org.opensearch.rest.action.document.RestBulkAction#prepareRequest(RestRequest, NodeClient) REST equivalent
     */
    public static org.opensearch.action.bulk.BulkRequest prepareRequest(BulkRequest request) {
        org.opensearch.action.bulk.BulkRequest bulkRequest = Requests.bulkRequest();

        String defaultIndex = request.hasIndex() ? request.getIndex() : null;
        String defaultRouting = request.hasRouting() ? request.getRouting() : null;
        FetchSourceContext defaultFetchSourceContext = FetchSourceContextProtoUtils.parseFromProtoRequest(request);
        String defaultPipeline = request.hasPipeline() ? request.getPipeline() : null;

        if (request.hasWaitForActiveShards()) {
            bulkRequest.waitForActiveShards(ActiveShardCountProtoUtils.parseProto(request.getWaitForActiveShards()));
        }
        Boolean defaultRequireAlias = request.hasRequireAlias() ? request.getRequireAlias() : null;

        if (request.hasTimeout()) {
            bulkRequest.timeout(request.getTimeout());
        } else {
            bulkRequest.timeout(BulkShardRequest.DEFAULT_TIMEOUT);
        }

        bulkRequest.setRefreshPolicy(RefreshProtoUtils.getRefreshPolicy(request.getRefresh()));

        // Note: batch_size is deprecated in OS 3.x. Add batch_size parameter when backporting to OS 2.x
        /*
        if (request.hasBatchSize()){
            logger.info("The batch size option in bulk API is deprecated and will be removed in 3.0.");
        }
        bulkRequest.batchSize(request.hasBatchSize() ? request.getBatchSize() : Integer.MAX_VALUE);
        */

        bulkRequest.add(
            BulkRequestParserProtoUtils.getDocWriteRequests(
                request,
                defaultIndex,
                defaultRouting,
                defaultFetchSourceContext,
                defaultPipeline,
                defaultRequireAlias
            )
        );

        return bulkRequest;
    }
}
