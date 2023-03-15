/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.bulk;

import org.opensearch.OpenSearchException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.rest.RestStatus;

import java.io.IOException;

/**
 * (Applicable for remote translog backed indexes) Thrown when there has been no successful leader checker call since
 * certain threshold. This exception is returned to the client.
 * TODO - This will also be used to fail the remote-backed index shards when the successful leader checker call happened too long ago.
 *
 * @opensearch.internal
 */
public class LeaderCheckerBeforeThresholdException extends OpenSearchException {
    public LeaderCheckerBeforeThresholdException(StreamInput in) throws IOException {
        super(in);
    }

    public LeaderCheckerBeforeThresholdException(String msg, Object... args) {
        super(msg, args);
    }

    @Override
    public RestStatus status() {
        return RestStatus.SERVICE_UNAVAILABLE;
    }
}
