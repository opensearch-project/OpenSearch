/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Base request for cluster-manager based read operations that allows to read the cluster state from the local node if needed
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeReadRequest}
 */
@Deprecated
public abstract class MasterNodeReadRequest<Request extends MasterNodeReadRequest<Request>> extends ClusterManagerNodeReadRequest<Request> {
    protected MasterNodeReadRequest() {}

    protected MasterNodeReadRequest(StreamInput in) throws IOException {
        super(in);
    }
}
