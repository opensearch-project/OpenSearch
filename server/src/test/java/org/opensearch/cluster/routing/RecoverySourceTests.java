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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.cluster.routing;

import org.opensearch.Version;
import org.opensearch.common.UUIDs;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.repositories.IndexId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;

public class RecoverySourceTests extends OpenSearchTestCase {

    public void testSerialization() throws IOException {
        RecoverySource recoverySource = TestShardRouting.randomRecoverySource();
        BytesStreamOutput out = new BytesStreamOutput();
        recoverySource.writeTo(out);
        RecoverySource serializedRecoverySource = RecoverySource.readFrom(out.bytes().streamInput());
        assertEquals(recoverySource.getType(), serializedRecoverySource.getType());
        assertEquals(recoverySource, serializedRecoverySource);
    }

    public void testRecoverySourceTypeOrder() {
        assertEquals(RecoverySource.Type.EMPTY_STORE.ordinal(), 0);
        assertEquals(RecoverySource.Type.EXISTING_STORE.ordinal(), 1);
        assertEquals(RecoverySource.Type.PEER.ordinal(), 2);
        assertEquals(RecoverySource.Type.SNAPSHOT.ordinal(), 3);
        assertEquals(RecoverySource.Type.LOCAL_SHARDS.ordinal(), 4);
        assertEquals(RecoverySource.Type.REMOTE_STORE.ordinal(), 5);
        // check exhaustiveness
        for (RecoverySource.Type type : RecoverySource.Type.values()) {
            assertThat(type.ordinal(), greaterThanOrEqualTo(0));
            assertThat(type.ordinal(), lessThanOrEqualTo(5));
        }
    }

    public void testSerializationRemoteStoreRecoverySource() throws IOException {
        RecoverySource recoverySource = new RecoverySource.RemoteStoreRecoverySource(
            UUIDs.randomBase64UUID(),
            Version.CURRENT,
            new IndexId("some_index", UUIDs.randomBase64UUID(random()))
        );

        BytesStreamOutput out = new BytesStreamOutput();
        recoverySource.writeTo(out);
        RecoverySource serializedRecoverySource = RecoverySource.readFrom(out.bytes().streamInput());
        assertEquals(recoverySource.getType(), serializedRecoverySource.getType());
        assertEquals(recoverySource, serializedRecoverySource);
    }
}
