/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search;

import org.opensearch.action.search.DeletePitInfo;
import org.opensearch.action.search.DeletePitResponse;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class DeletePitResponseTests extends OpenSearchTestCase {

    public void testDeletePitResponseToXContent() throws IOException {
        DeletePitInfo deletePitInfo = new DeletePitInfo(true, "pitId");
        List<DeletePitInfo> deletePitInfoList = new ArrayList<>();
        deletePitInfoList.add(deletePitInfo);
        DeletePitResponse deletePitResponse = new DeletePitResponse(deletePitInfoList);

        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            deletePitResponse.toXContent(builder, ToXContent.EMPTY_PARAMS);
        }
        assertEquals(true, deletePitResponse.getDeletePitResults().get(0).getPitId().equals("pitId"));
        assertEquals(true, deletePitResponse.getDeletePitResults().get(0).isSuccessful());
    }

    public void testDeletePitResponseToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        DeletePitResponse originalResponse = createDeletePitResponseTestItem();

        BytesReference originalBytes = toShuffledXContent(originalResponse, xContentType, ToXContent.EMPTY_PARAMS, randomBoolean());
        DeletePitResponse parsedResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedResponse = DeletePitResponse.fromXContent(parser);
        }
        assertEquals(
            originalResponse.getDeletePitResults().get(0).isSuccessful(),
            parsedResponse.getDeletePitResults().get(0).isSuccessful()
        );
        assertEquals(originalResponse.getDeletePitResults().get(0).getPitId(), parsedResponse.getDeletePitResults().get(0).getPitId());
        BytesReference parsedBytes = XContentHelper.toXContent(parsedResponse, xContentType, randomBoolean());
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
    }

    private static DeletePitResponse createDeletePitResponseTestItem() {
        DeletePitInfo deletePitInfo = new DeletePitInfo(randomBoolean(), "pitId");
        List<DeletePitInfo> deletePitInfoList = new ArrayList<>();
        deletePitInfoList.add(deletePitInfo);
        return new DeletePitResponse(deletePitInfoList);
    }
}
