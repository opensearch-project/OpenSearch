/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.shards;

import org.opensearch.Version;
import org.opensearch.action.pagination.PageParams;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;

public class CatShardsRequestTests extends OpenSearchTestCase {

    public void testSerializationWithDefaultParameters() throws Exception {
        CatShardsRequest request = new CatShardsRequest();
        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);
                assertNull(deserialized.getPageParams());
                assertNull(deserialized.getCancelAfterTimeInterval());
                assertEquals(0, deserialized.getIndices().length);
                assertFalse(deserialized.isRequestLimitCheckSupported());
            }
        }
    }

    public void testSerializationWithStringPageParamsNull() throws Exception {
        CatShardsRequest catShardsRequest = new CatShardsRequest();
        catShardsRequest.setPageParams(new PageParams(null, null, randomIntBetween(1, 5)));
        int numIndices = randomIntBetween(1, 5);
        String[] indices = new String[numIndices];
        for (int i = 0; i < numIndices; i++) {
            indices[i] = randomAlphaOfLengthBetween(3, 10);
        }
        catShardsRequest.setIndices(indices);
        catShardsRequest.setCancelAfterTimeInterval(TimeValue.timeValueMillis(randomIntBetween(1, 5)));
        catShardsRequest.setRequestLimitCheckSupported(true);

        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            catShardsRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);
                // asserting pageParams of deserialized request
                assertEquals(catShardsRequest.getPageParams(), deserialized.getPageParams());
                // assert indices
                assertArrayEquals(catShardsRequest.getIndices(), deserialized.getIndices());
                // assert timeout
                assertEquals(catShardsRequest.getCancelAfterTimeInterval(), deserialized.getCancelAfterTimeInterval());
                assertTrue(deserialized.isRequestLimitCheckSupported());
            }
        }
    }

    public void testSerializationWithPageParamsSet() throws Exception {
        CatShardsRequest catShardsRequest = new CatShardsRequest();
        catShardsRequest.setPageParams(
            new PageParams(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomIntBetween(1, 5))
        );
        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            catShardsRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);

                // asserting pageParams of deserialized request
                assertEquals(catShardsRequest.getPageParams(), deserialized.getPageParams());
                assertEquals(0, deserialized.getIndices().length);
                assertNull(deserialized.getCancelAfterTimeInterval());
                assertFalse(deserialized.isRequestLimitCheckSupported());
            }
        }
    }

    public void testSerializationWithOlderVersionsParametersNotSerialized() throws Exception {
        CatShardsRequest catShardsRequest = new CatShardsRequest();
        catShardsRequest.setPageParams(
            new PageParams(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), randomIntBetween(1, 5))
        );
        catShardsRequest.setCancelAfterTimeInterval(TimeValue.timeValueMillis(randomIntBetween(1, 5)));
        catShardsRequest.setIndices(new String[2]);

        Version version = VersionUtils.getPreviousVersion(Version.V_2_18_0);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            catShardsRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);
                assertNull(deserialized.getPageParams());
                assertNull(deserialized.getIndices());
                assertNull(deserialized.getCancelAfterTimeInterval());
                assertFalse(deserialized.isRequestLimitCheckSupported());
            }
        }
    }

    // --- Mixed-version wire-compatibility coverage for the indicesStatsRequired field (V_3_8_0 gate) ---

    /**
     * At a version on/after the gate (both nodes on a version that knows the field), the flag must
     * round-trip exactly in both states. Models same-version feature-to-feature transport.
     */
    public void testIndicesStatsRequiredRoundTripsOnCurrentVersion() throws Exception {
        for (boolean flag : new boolean[] { true, false }) {
            CatShardsRequest request = new CatShardsRequest();
            request.setIndicesStatsRequired(flag);
            assertTrue(Version.CURRENT.onOrAfter(Version.V_3_8_0));
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                out.setVersion(Version.CURRENT);
                request.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    in.setVersion(Version.CURRENT);
                    CatShardsRequest deserialized = new CatShardsRequest(in);
                    assertEquals(flag, deserialized.isIndicesStatsRequired());
                }
            }
        }
    }

    /**
     * When the negotiated peer version is BEFORE the gate (a true old node that predates the
     * feature), the field is NOT written and NOT read, so the stream stays aligned and the reader
     * falls back to the safe default (true = fetch stats, i.e. legacy behavior). This is the mixed
     * cluster safety guarantee: an old peer never desynchronizes the stream, and the optimization
     * simply does not engage.
     */
    public void testIndicesStatsRequiredDefaultsTrueWhenPeerBeforeGate() throws Exception {
        Version oldVersion = VersionUtils.getPreviousVersion(Version.V_3_8_0);
        assertTrue(oldVersion.before(Version.V_3_8_0));
        CatShardsRequest request = new CatShardsRequest();
        request.setIndicesStatsRequired(false); // even if the coordinator wanted to skip stats...
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(oldVersion);
            request.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(oldVersion);
                CatShardsRequest deserialized = new CatShardsRequest(in);
                // ...an old peer reads the safe default and fetches stats as before (no skip).
                assertTrue(deserialized.isIndicesStatsRequired());
            }
        }
    }
}
