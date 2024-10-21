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

package org.opensearch.action.resync;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.translog.Translog;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.charset.StandardCharsets;

import static org.hamcrest.Matchers.equalTo;

public class ResyncReplicationRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        final byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
        final Translog.Index index = new Translog.Index("id", 0, randomNonNegativeLong(), randomNonNegativeLong(), bytes, null, -1);
        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        final ResyncReplicationRequest before = new ResyncReplicationRequest(shardId, 42L, 100, new Translog.Operation[] { index });

        final BytesStreamOutput out = new BytesStreamOutput();
        before.writeTo(out);

        final StreamInput in = out.bytes().streamInput();
        final ResyncReplicationRequest after = new ResyncReplicationRequest(in);

        assertThat(after, equalTo(before));
    }

    public void testContractBetweenEqualsAndHashCode() {
        final byte[] bytes = "{}".getBytes(StandardCharsets.UTF_8);
        final Translog.Index index = new Translog.Index("id", 0, 123L, -123L, bytes, null, -1);
        final ShardId shardId = new ShardId(new Index("index", "uuid"), 0);
        // Both created requests have arrays `operations` with the same content, and we want to verify that
        // equals() and hashCode() are following the contract:
        // If objects are equal, they have the same hash code
        final ResyncReplicationRequest request1 = new ResyncReplicationRequest(shardId, 42L, 100, new Translog.Operation[] { index });
        final ResyncReplicationRequest request2 = new ResyncReplicationRequest(shardId, 42L, 100, new Translog.Operation[] { index });

        assertEquals(request1, request2);
        assertEquals(request1.hashCode(), request2.hashCode());
    }

}
