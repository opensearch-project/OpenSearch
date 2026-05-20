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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.coordination;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.action.ActionListener;

/**
 * Publishes the cluster state
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public interface ClusterStatePublisher {
    /**
     * Publish all the changes to the cluster from the cluster-manager (can be called just by the cluster-manager). The publish
     * process should apply this state to the cluster-manager as well!
     * <p>
     * The publishListener allows to wait for the publication to complete, which can be either successful completion, timing out or failing.
     * The method is guaranteed to pass back a {@link FailedToCommitClusterStateException} to the publishListener if the change is not
     * committed and should be rejected. Any other exception signals that something bad happened but the change is committed.
     * <p>
     * The {@link AckListener} allows to keep track of the ack received from nodes, and verify whether
     * they updated their own cluster state or not.
     */
    void publish(ClusterChangedEvent clusterChangedEvent, ActionListener<Void> publishListener, AckListener ackListener);

    /**
     * An acknowledgement listener.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    interface AckListener {
        /**
         * Should be called when the cluster coordination layer has committed the cluster state (i.e. even if this publication fails,
         * it is guaranteed to appear in future publications).
         * @param commitTime the time it took to commit the cluster state
         */
        void onCommit(TimeValue commitTime);

        /**
         * Should be called whenever the cluster coordination layer receives confirmation from a node that it has successfully applied
         * the cluster state. In case of failures, an exception should be provided as parameter.
         * @param node the node
         * @param e the optional exception
         */
        void onNodeAck(DiscoveryNode node, @Nullable Exception e);
    }

}
