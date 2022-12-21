/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.support.replication;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;

import java.io.IOException;

/**
 * Thrown when a transport replication request is made to a replica with replication mode as NO_REPLICATION.
 *
 * @opensearch.internal
 */
public class UnsupportedReplicaActionException extends OpenSearchException {

    public UnsupportedReplicaActionException(StreamInput in) throws IOException {
        super(in);
    }

    public UnsupportedReplicaActionException(String msg, Object... args) {
        super(msg, args);
    }
}
