/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
*/

package org.opensearch.action.support.clustermanager;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.cluster.ack.AckedRequest;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

import static org.opensearch.common.unit.TimeValue.timeValueSeconds;

/**
 * A base request for cluster-manager based operations.
 * It is similar to ClusterManagerNodeRequest, but extends ActionRequest and AckedRequest.
 * @opensearch.api
 */
public abstract class ClusterManagerNodeAckRequest extends ActionRequest implements AckedRequest {

    public static final TimeValue DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT = TimeValue.timeValueSeconds(30);
    protected TimeValue clusterManagerNodeTimeout = DEFAULT_CLUSTER_MANAGER_NODE_TIMEOUT;
    protected TimeValue masterNodeTimeout = clusterManagerNodeTimeout;
    public static final TimeValue DEFAULT_ACK_TIMEOUT = timeValueSeconds(30);
    protected TimeValue timeout = DEFAULT_ACK_TIMEOUT;

    protected ClusterManagerNodeAckRequest() {}

    protected ClusterManagerNodeAckRequest(StreamInput in) throws IOException {
        super(in);
        clusterManagerNodeTimeout = in.readTimeValue();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeTimeValue(clusterManagerNodeTimeout);
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    @SuppressWarnings("unchecked")
    public final ClusterManagerNodeAckRequest clusterManagerNodeTimeout(TimeValue timeout) {
        this.clusterManagerNodeTimeout = timeout;
        return this;
    }

    /**
     * A timeout value in case the cluster-manager has not been discovered yet or disconnected.
     */
    public final ClusterManagerNodeAckRequest clusterManagerNodeTimeout(String timeout) {
        return clusterManagerNodeTimeout(
            TimeValue.parseTimeValue(timeout, null, getClass().getSimpleName() + ".clusterManagerNodeTimeout")
        );
    }

    public final TimeValue clusterManagerNodeTimeout() {
        return this.clusterManagerNodeTimeout;
    }

    /** @deprecated As of 2.1, because supporting inclusive language, replaced by {@link #clusterManagerNodeTimeout()} */
    @Deprecated
    public final TimeValue masterNodeTimeout() {
        return clusterManagerNodeTimeout();
    }

    @Override
    public ActionRequestValidationException validate() {
        return null;
    }

    @Override
    public TimeValue ackTimeout() {
        return this.timeout;
    }
}
