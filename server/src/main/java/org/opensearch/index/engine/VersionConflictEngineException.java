/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.index.engine;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.index.seqno.SequenceNumbers;

import java.io.IOException;

/**
 * Exception thrown if there is a version conflict
 *
 * @opensearch.internal
 */
@PublicApi(since = "1.0.0")
public class VersionConflictEngineException extends EngineException {

    public VersionConflictEngineException(ShardId shardId, Engine.Operation op, long currentVersion, boolean deleted) {
        this(shardId, op.id(), op.versionType().explainConflictForWrites(currentVersion, op.version(), deleted));
    }

    public VersionConflictEngineException(
        ShardId shardId,
        String id,
        long compareAndWriteSeqNo,
        long compareAndWriteTerm,
        long currentSeqNo,
        long currentTerm
    ) {
        this(
            shardId,
            id,
            "required seqNo ["
                + compareAndWriteSeqNo
                + "], primary term ["
                + compareAndWriteTerm
                + "]."
                + (currentSeqNo == SequenceNumbers.UNASSIGNED_SEQ_NO
                    ? " but no document was found"
                    : " current document has seqNo [" + currentSeqNo + "] and primary term [" + currentTerm + "]")
        );
    }

    public VersionConflictEngineException(ShardId shardId, String id, String explanation) {
        this(shardId, "[{}]: version conflict, {}", null, id, explanation);
    }

    public VersionConflictEngineException(ShardId shardId, String msg, Throwable cause, Object... params) {
        super(shardId, msg, cause, params);
    }

    @Override
    public RestStatus status() {
        return RestStatus.CONFLICT;
    }

    public VersionConflictEngineException(StreamInput in) throws IOException {
        super(in);
    }

    @Override
    public Throwable fillInStackTrace() {
        // This is on the hot path for updates; stack traces are expensive to compute and not very useful for VCEEs, so don't fill it in.
        return this;
    }
}
