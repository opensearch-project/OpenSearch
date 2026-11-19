/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.search.action;

import org.opensearch.arrow.search.scan.ArrowScanBatchResponse;
import org.opensearch.arrow.search.scan.ArrowShardStreamRequest;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.tasks.Task;
import org.opensearch.transport.StreamTransportResponseHandler;

/**
 * Opens shard streams on data nodes.
 */
interface ShardStreamClient {

    /**
     * Opens the shard stream of a shard copy. The handler receives the stream or the failure to open it. This method may
     * also throw, if the node cannot be reached at all.
     */
    void open(
        DiscoveryNode node,
        ArrowShardStreamRequest request,
        Task parentTask,
        StreamTransportResponseHandler<ArrowScanBatchResponse> handler
    );
}
