/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.transport;

import org.opensearch.cluster.node.ProtobufDiscoveryNode;

/**
 * Request for remote clusters
*
* @opensearch.internal
*/
public interface ProtobufRemoteClusterAwareRequest {

    /**
     * Returns the preferred discovery node for this request. The remote cluster client will attempt to send
    * this request directly to this node. Otherwise, it will send the request as a proxy action that will
    * be routed by the remote cluster to this node.
    *
    * @return preferred discovery node
    */
    ProtobufDiscoveryNode getPreferredTargetNode();

}
