/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ProtobufActionRequest;
import org.opensearch.common.unit.TimeValue;

/**
 * A protobuf request for cluster-manager based operation.
*
* @opensearch.internal
*/
public abstract class ProtobufClusterManagerNodeRequest<Request extends ProtobufClusterManagerNodeRequest<Request>> extends
    ProtobufActionRequest {

    public static final TimeValue DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT} */
    @Deprecated
    public static final TimeValue DEFAULT_MASTER_NODE_TIMEOUT = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    protected TimeValue clusterManagerNodeTimeout = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout} */
    @Deprecated
    protected TimeValue masterNodeTimeout = clusterManagerNodeTimeout;

    protected ProtobufClusterManagerNodeRequest() {}

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
    */
    @SuppressWarnings("unchecked")
    public final Request clusterManagerNodeTimeout(TimeValue timeout) {
        this.clusterManagerNodeTimeout = timeout;
        return (Request) this;
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
    *
    * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout(TimeValue)}
    */
    @SuppressWarnings("unchecked")
    @Deprecated
    public final Request masterNodeTimeout(TimeValue timeout) {
        return clusterManagerNodeTimeout(timeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
    */
    public final Request clusterManagerNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(
            TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".clusterManagerNodeTimeout")
        );
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
    *
    * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout(String)}
    */
    @Deprecated
    public final Request masterNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(timeout);
    }

    public final TimeValue clusterManagerNodeTimeout() {
        return this.clusterManagerNodeTimeout;
    }

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout()} */
    @Deprecated
    public final TimeValue masterNodeTimeout() {
        return clusterManagerNodeTimeout();
    }
}
