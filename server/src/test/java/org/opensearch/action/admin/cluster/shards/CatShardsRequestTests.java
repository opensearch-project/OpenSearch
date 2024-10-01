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

        Version version = Version.CURRENT;
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            catShardsRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);

                // asserting pageParams of deserialized request
                assertNotNull(deserialized.getPageParams());
                assertNull(deserialized.getPageParams().getRequestedToken());
                assertNull(deserialized.getPageParams().getSort());
                assertEquals(catShardsRequest.getPageParams().getSize(), deserialized.getPageParams().getSize());

                // assert indices
                assertArrayEquals(catShardsRequest.getIndices(), deserialized.getIndices());

                // assert timeout
                assertEquals(catShardsRequest.getCancelAfterTimeInterval(), deserialized.getCancelAfterTimeInterval());
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
                assertNotNull(deserialized.getPageParams());
                assertEquals(catShardsRequest.getPageParams().getRequestedToken(), deserialized.getPageParams().getRequestedToken());
                assertEquals(catShardsRequest.getPageParams().getSort(), deserialized.getPageParams().getSort());
                assertEquals(catShardsRequest.getPageParams().getSize(), deserialized.getPageParams().getSize());

                assertEquals(0, deserialized.getIndices().length);
                assertNull(deserialized.getCancelAfterTimeInterval());
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

        Version version = VersionUtils.getPreviousVersion(Version.CURRENT);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(version);
            catShardsRequest.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(version);
                CatShardsRequest deserialized = new CatShardsRequest(in);

                // asserting pageParams of deserialized request
                assertNull(deserialized.getPageParams());
                assertNull(deserialized.getIndices());
                assertNull(deserialized.getCancelAfterTimeInterval());
            }
        }
    }
}
