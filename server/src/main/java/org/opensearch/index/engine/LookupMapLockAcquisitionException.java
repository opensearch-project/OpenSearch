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

import java.io.IOException;

/**
 * This exception indicates that CompositeIndexWriter was unable to obtain lock on CriteriaBasedIndexWriterLookup map
 * during indexing.
 * indexing request contains this Exception in the response, we do not need to add a translog entry for this request.
 *
 */
public class LookupMapLockAcquisitionException extends EngineException {
    public LookupMapLockAcquisitionException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for updates; stack traces are expensive to compute and not very useful for VCEEs, so don't fill it in.
        return this;
    }

    public LookupMapLockAcquisitionException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(shardId, msg, cause, params);
    }
}
