/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown when a node join request or a cluster-manager ping reaches a node which is not
 * currently acting as a cluster-manager or when a cluster state update task is to be executed
 * on a node that is no longer cluster-manager.
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link NotClusterManagerException}
 */
@Deprecated
public class NotMasterException extends NotClusterManagerException {

    public NotMasterException(String msg) {
        super(msg);
    }

    public NotMasterException(StreamInput in) throws IOException {
        super(in);
    }

}
