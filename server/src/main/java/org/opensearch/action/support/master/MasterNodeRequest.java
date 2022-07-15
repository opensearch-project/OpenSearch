/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.master;

import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * A based request for cluster-manager based operation.
 *
 * @opensearch.internal
 * @deprecated As of 2.1, because supporting inclusive language, replaced by {@link ClusterManagerNodeRequest}
 */
@Deprecated
public abstract class MasterNodeRequest<Request extends MasterNodeRequest<Request>> extends ClusterManagerNodeRequest<Request> {

    protected MasterNodeRequest() {}

    protected MasterNodeRequest(StreamInput in) throws IOException {
        super(in);
    }
}
