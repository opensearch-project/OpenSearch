/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.tiering;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;

import java.util.LinkedList;
import java.util.List;

public class HotToWarmTieringResponseTests extends AbstractWireSerializingTestCase<HotToWarmTieringResponse> {

    @Override
    protected Writeable.Reader<HotToWarmTieringResponse> instanceReader() {
        return HotToWarmTieringResponse::new;
    }

    @Override
    protected HotToWarmTieringResponse createTestInstance() {
        return randomHotToWarmTieringResponse();
    }

    @Override
    protected void assertEqualInstances(HotToWarmTieringResponse expected, HotToWarmTieringResponse actual) {
        assertNotSame(expected, actual);
        assertEquals(actual.isAcknowledged(), expected.isAcknowledged());

        for (int i = 0; i < expected.getFailedIndices().size(); i++) {
            HotToWarmTieringResponse.IndexResult expectedIndexResult = expected.getFailedIndices().get(i);
            HotToWarmTieringResponse.IndexResult actualIndexResult = actual.getFailedIndices().get(i);
            assertNotSame(expectedIndexResult, actualIndexResult);
            assertEquals(actualIndexResult.getIndex(), expectedIndexResult.getIndex());
            assertEquals(actualIndexResult.getFailureReason(), expectedIndexResult.getFailureReason());
        }
    }

    /**
     * Verifies that ToXContent works with any random {@link HotToWarmTieringResponse} object
     * @throws Exception - in case of error
     */
    public void testToXContentWorksForRandomResponse() throws Exception {
        HotToWarmTieringResponse testResponse = randomHotToWarmTieringResponse();
        XContentType xContentType = randomFrom(XContentType.values());
        try (XContentBuilder builder = XContentBuilder.builder(xContentType.xContent())) {
            testResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
    }

    /**
     * Verify the XContent output of the response object
     * @throws Exception - in case of error
     */
    public void testToXContentOutput() throws Exception {
        String[] indices = new String[] { "index2", "index1" };
        String[] errorReasons = new String[] { "reason2", "reason1" };
        List<HotToWarmTieringResponse.IndexResult> results = new LinkedList<>();
        for (int i = 0; i < indices.length; ++i) {
            results.add(new HotToWarmTieringResponse.IndexResult(indices[i], errorReasons[i]));
        }
        HotToWarmTieringResponse testResponse = new HotToWarmTieringResponse(true, results);

        // generate a corresponding expected xcontent
        XContentBuilder content = XContentFactory.jsonBuilder().startObject().field("acknowledged", true).startArray("failed_indices");
        // expected result should be in the sorted order
        content.startObject().field("index", "index1").field("error", "reason1").endObject();
        content.startObject().field("index", "index2").field("error", "reason2").endObject();
        content.endArray().endObject();
        assertEquals(content.toString(), testResponse.toString());
    }

    /**
     * @return - randomly generated object of type {@link HotToWarmTieringResponse.IndexResult}
     */
    private HotToWarmTieringResponse.IndexResult randomIndexResult() {
        String indexName = randomAlphaOfLengthBetween(1, 50);
        String failureReason = randomAlphaOfLengthBetween(1, 200);
        return new HotToWarmTieringResponse.IndexResult(indexName, failureReason);
    }

    /**
     * @return - randomly generated object of type {@link HotToWarmTieringResponse}
     */
    private HotToWarmTieringResponse randomHotToWarmTieringResponse() {
        int numIndexResult = randomIntBetween(0, 10);
        List<HotToWarmTieringResponse.IndexResult> indexResults = new LinkedList<>();
        for (int i = 0; i < numIndexResult; ++i) {
            indexResults.add(randomIndexResult());
        }
        return new HotToWarmTieringResponse(randomBoolean(), indexResults);
    }
}
