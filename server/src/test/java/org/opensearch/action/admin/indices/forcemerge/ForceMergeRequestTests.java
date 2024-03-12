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

package org.opensearch.action.admin.indices.forcemerge;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

public class ForceMergeRequestTests extends OpenSearchTestCase {

    public void testDescription() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals(
            "Force-merge indices [], maxSegments[-1], onlyExpungeDeletes[false], flush[true], primaryOnly[false]",
            request.getDescription()
        );

        request = new ForceMergeRequest("shop", "blog");
        assertEquals(
            "Force-merge indices [shop, blog], maxSegments[-1], onlyExpungeDeletes[false], flush[true], primaryOnly[false]",
            request.getDescription()
        );

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        request.primaryOnly(true);
        assertEquals(
            "Force-merge indices [], maxSegments[12], onlyExpungeDeletes[true], flush[false], primaryOnly[true]",
            request.getDescription()
        );
    }

    public void testToString() {
        ForceMergeRequest request = new ForceMergeRequest();
        assertEquals("ForceMergeRequest{maxNumSegments=-1, onlyExpungeDeletes=false, flush=true, primaryOnly=false}", request.toString());

        request = new ForceMergeRequest();
        request.maxNumSegments(12);
        request.onlyExpungeDeletes(true);
        request.flush(false);
        request.primaryOnly(true);
        assertEquals("ForceMergeRequest{maxNumSegments=12, onlyExpungeDeletes=true, flush=false, primaryOnly=true}", request.toString());
    }

    public void testSerialization() throws Exception {
        final ForceMergeRequest request = randomRequest();
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            request.writeTo(out);

            final ForceMergeRequest deserializedRequest;
            try (StreamInput in = out.bytes().streamInput()) {
                deserializedRequest = new ForceMergeRequest(in);
            }
            assertEquals(request.maxNumSegments(), deserializedRequest.maxNumSegments());
            assertEquals(request.onlyExpungeDeletes(), deserializedRequest.onlyExpungeDeletes());
            assertEquals(request.flush(), deserializedRequest.flush());
            assertEquals(request.primaryOnly(), deserializedRequest.primaryOnly());
            assertEquals(request.forceMergeUUID(), deserializedRequest.forceMergeUUID());
        }
    }

    public void testBwcSerialization() throws Exception {
        {
            final ForceMergeRequest sample = randomRequest();
            final Version compatibleVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(compatibleVersion);
                sample.writeTo(out);

                final ForceMergeRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(Version.CURRENT);
                    deserializedRequest = new ForceMergeRequest(in);
                }

                assertEquals(sample.maxNumSegments(), deserializedRequest.maxNumSegments());
                assertEquals(sample.onlyExpungeDeletes(), deserializedRequest.onlyExpungeDeletes());
                assertEquals(sample.flush(), deserializedRequest.flush());
                if (compatibleVersion.onOrAfter(Version.V_3_0_0)) {
                    assertEquals(sample.primaryOnly(), deserializedRequest.primaryOnly());
                    assertEquals(sample.forceMergeUUID(), deserializedRequest.forceMergeUUID());
                }
            }
        }

        {
            final ForceMergeRequest sample = randomRequest();
            final Version compatibleVersion = VersionUtils.randomCompatibleVersion(random(), Version.CURRENT);
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(Version.CURRENT);
                sample.getParentTask().writeTo(out);
                out.writeStringArray(sample.indices());
                sample.indicesOptions().writeIndicesOptions(out);
                out.writeInt(sample.maxNumSegments());
                out.writeBoolean(sample.onlyExpungeDeletes());
                out.writeBoolean(sample.flush());
                if (compatibleVersion.onOrAfter(Version.V_3_0_0)) {
                    out.writeBoolean(sample.primaryOnly());
                }
                if (compatibleVersion.onOrAfter(Version.V_3_0_0)) {
                    out.writeString(sample.forceMergeUUID());
                } else {
                    out.writeOptionalString(sample.forceMergeUUID());
                }

                final ForceMergeRequest deserializedRequest;
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(compatibleVersion);
                    deserializedRequest = new ForceMergeRequest(in);
                }

                assertEquals(sample.maxNumSegments(), deserializedRequest.maxNumSegments());
                assertEquals(sample.onlyExpungeDeletes(), deserializedRequest.onlyExpungeDeletes());
                assertEquals(sample.flush(), deserializedRequest.flush());
                if (compatibleVersion.onOrAfter(Version.V_3_0_0)) {
                    assertEquals(sample.primaryOnly(), deserializedRequest.primaryOnly());
                }
                assertEquals(sample.forceMergeUUID(), deserializedRequest.forceMergeUUID());

            }
        }
    }

    private ForceMergeRequest randomRequest() {
        ForceMergeRequest request = new ForceMergeRequest();
        if (randomBoolean()) {
            request.maxNumSegments(randomIntBetween(1, 10));
        }
        request.onlyExpungeDeletes(true);
        request.flush(randomBoolean());
        request.primaryOnly(randomBoolean());
        return request;
    }
}
