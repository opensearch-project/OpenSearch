/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * ImmutableCriteriaException is thrown when user tries to modify criteria field for a document. This exception enforces
 * the immutability constraint on criteria field which will be needed to ensure consistency during updates.
 *
 */
public class ImmutableCriteriaException extends EngineException {
    public ImmutableCriteriaException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for updates; stack traces are expensive to compute and not very useful for VCEEs, so don't fill it in.
        return this;
    }

    public ImmutableCriteriaException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(shardId, msg, cause, params);
    }

    @Override
    public RestStatus status() {
        return RestStatus.BAD_REQUEST;
    }
}
