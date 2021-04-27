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

package org.opensearch.indices.recovery;

import org.opensearch.LegacyESVersion;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.ShardId;

import java.io.IOException;

final class RecoveryFinalizeRecoveryRequest extends RecoveryTransportRequest {

    private final long recoveryId;
    private final ShardId shardId;
    private final long globalCheckpoint;
    private final long trimAboveSeqNo;

    RecoveryFinalizeRecoveryRequest(StreamInput in) throws IOException {
        super(in);
        recoveryId = in.readLong();
        shardId = new ShardId(in);
        globalCheckpoint = in.readZLong();
        if (in.getVersion().onOrAfter(LegacyESVersion.V_7_4_0)) {
            trimAboveSeqNo = in.readZLong();
        } else {
            trimAboveSeqNo = SequenceNumbers.UNASSIGNED_SEQ_NO;
        }
    }

    RecoveryFinalizeRecoveryRequest(final long recoveryId, final long requestSeqNo, final ShardId shardId,
                                    final long globalCheckpoint, final long trimAboveSeqNo) {
        super(requestSeqNo);
        this.recoveryId = recoveryId;
        this.shardId = shardId;
        this.globalCheckpoint = globalCheckpoint;
        this.trimAboveSeqNo = trimAboveSeqNo;
    }

    public long recoveryId() {
        return this.recoveryId;
    }

    public ShardId shardId() {
        return shardId;
    }

    public long globalCheckpoint() {
        return globalCheckpoint;
    }

    public long trimAboveSeqNo() {
        return trimAboveSeqNo;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeLong(recoveryId);
        shardId.writeTo(out);
        out.writeZLong(globalCheckpoint);
        if (out.getVersion().onOrAfter(LegacyESVersion.V_7_4_0)) {
            out.writeZLong(trimAboveSeqNo);
        }
    }

}
