/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.discovery;

import org.opensearch.core.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Exception when the cluster-manager is not discovered
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link ClusterManagerNotDiscoveredException}
 */
@Deprecated
public class MasterNotDiscoveredException extends ClusterManagerNotDiscoveredException {

    public MasterNotDiscoveredException() {
        super();
    }

    public MasterNotDiscoveredException(Throwable cause) {
        super(cause);
    }

    public MasterNotDiscoveredException(String message) {
        super(message);
    }

    public MasterNotDiscoveredException(StreamInput in) throws IOException {
        super(in);
    }
}
