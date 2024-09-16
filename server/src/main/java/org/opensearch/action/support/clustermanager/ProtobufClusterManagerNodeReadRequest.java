/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.support.clustermanager;

import java.io.IOException;
import java.io.OutputStream;

/**
 * Base request for cluster-manager based read operations that allows to read the cluster state from the local node if needed
*
* @opensearch.internal
*/
public abstract class ProtobufClusterManagerNodeReadRequest<Request extends ProtobufClusterManagerNodeReadRequest<Request>> extends
    ProtobufClusterManagerNodeRequest<Request> {

    protected boolean local = false;

    protected ProtobufClusterManagerNodeReadRequest() {}

    protected ProtobufClusterManagerNodeReadRequest(byte[] in) throws IOException {

    }

    @Override
    public void writeTo(OutputStream out) throws IOException {

    }

    @SuppressWarnings("unchecked")
    public final Request local(boolean local) {
        this.local = local;
        return (Request) this;
    }

    /**
     * Return local information, do not retrieve the state from cluster-manager node (default: false).
    * @return <code>true</code> if local information is to be returned;
    * <code>false</code> if information is to be retrieved from cluster-manager node (default).
    */
    public final boolean local() {
        return local;
    }
}
