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

package org.opensearch.action.admin.indices.close;

import org.opensearch.Version;
import org.opensearch.action.support.ActiveShardCount;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.tasks.TaskId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

public class CloseIndexRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        final CloseIndexRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final CloseIndexRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new CloseIndexRequest(in);
            }
            assertEquals(request.timeout(), deserializedRequest.timeout());
            assertEquals(request.clusterManagerNodeTimeout(), deserializedRequest.clusterManagerNodeTimeout());
            assertEquals(request.indicesOptions(), deserializedRequest.indicesOptions());
            assertEquals(request.getParentTask(), deserializedRequest.getParentTask());
            assertEquals(request.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            assertArrayEquals(request.indices(), deserializedRequest.indices());
        }
    }

    public void testBwcSerialization() throws Exception {
        {
            final CloseIndexRequest request = randomRequest();
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(VersionUtils.randomCompatibleVersion(random(), Version.CURRENT));
                request.writeTo(out);

                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(out.getVersion());
                    assertEquals(request.getParentTask(), TaskId.readFromStream(in));
                    assertEquals(request.clusterManagerNodeTimeout(), in.readTimeValue());
                    assertEquals(request.timeout(), in.readTimeValue());
                    assertArrayEquals(request.indices(), in.readStringArray());
                    final IndicesOptions indicesOptions = IndicesOptions.readIndicesOptions(in);
                    assertEquals(request.indicesOptions(), indicesOptions);
                    assertEquals(request.waitForActiveShards(), ActiveShardCount.readFrom(in));
                }
            }
        }
        {
            final CloseIndexRequest sample = randomRequest();
            final Version version = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(version);
                sample.getParentTask().writeTo(out);
                out.writeTimeValue(sample.clusterManagerNodeTimeout());
                out.writeTimeValue(sample.timeout());
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);
                sample.waitForActiveShards().writeTo(out);

                final CloseIndexRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(version);
                    deserializedRequest = new CloseIndexRequest(in);
                }
                assertEquals(sample.getParentTask(), deserializedRequest.getParentTask());
                assertEquals(sample.clusterManagerNodeTimeout(), deserializedRequest.clusterManagerNodeTimeout());
                assertEquals(sample.timeout(), deserializedRequest.timeout());
                assertArrayEquals(sample.indices(), deserializedRequest.indices());
                assertEquals(sample.indicesOptions(), deserializedRequest.indicesOptions());
                assertEquals(sample.waitForActiveShards(), deserializedRequest.waitForActiveShards());
            }
        }
    }

    private CloseIndexRequest randomRequest() {
        CloseIndexRequest request = new CloseIndexRequest();
        request.indices(generateRandomStringArray(10, 5, false, false));
        if (randomBoolean()) {
            request.indicesOptions(
                IndicesOptions.fromOptions(randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean(), randomBoolean())
            );
        }
        if (randomBoolean()) {
            request.timeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.clusterManagerNodeTimeout(randomPositiveTimeValue());
        }
        if (randomBoolean()) {
            request.setParentTask(randomAlphaOfLength(5), randomNonNegativeLong());
        }
        if (randomBoolean()) {
            request.waitForActiveShards(
                randomFrom(ActiveShardCount.DEFAULT, ActiveShardCount.NONE, ActiveShardCount.ONE, ActiveShardCount.ALL)
            );
        }
        return request;
    }
}
