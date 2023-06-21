/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.cluster;

import org.opensearch.cluster.service.ClusterService;

/**
 * A component that is in charge of applying an incoming cluster state to the node internal data structures.
* The single apply method is called before the cluster state becomes visible via {@link ClusterService#state()}.
*
* @opensearch.internal
*/
public interface ProtobufClusterStateApplier {

    /**
     * Called when a new cluster state ({@link ProtobufClusterChangedEvent#state()} needs to be applied. The cluster state to be applied is already
    * committed when this method is called, so an applier must therefore be prepared to deal with any state it receives without throwing
    * an exception. Throwing an exception from an applier is very bad because it will stop the application of this state before it has
    * reached all the other appliers, and will likely result in another attempt to apply the same (or very similar) cluster state which
    * might continue until this node is removed from the cluster.
    */
    void applyProtobufClusterState(ProtobufClusterChangedEvent event);
}
