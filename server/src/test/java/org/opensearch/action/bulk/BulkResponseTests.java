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

package org.opensearch.action.bulk;

import org.opensearch.OpenSearchException;
import org.opensearch.ExceptionsHelper;
import org.opensearch.action.DocWriteRequest;
import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.delete.DeleteResponseTests;
import org.opensearch.action.index.IndexResponseTests;
import org.opensearch.action.update.UpdateResponseTests;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.OpenSearchExceptionTests.randomExceptions;
import static org.opensearch.action.bulk.BulkItemResponseTests.assertBulkItemResponse;
import static org.opensearch.action.bulk.BulkResponse.NO_INGEST_TOOK;
import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class BulkResponseTests extends OpenSearchTestCase {

    public void testToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();

        long took = randomFrom(randomNonNegativeLong(), -1L);
        long ingestTook = randomFrom(randomNonNegativeLong(), NO_INGEST_TOOK);
        int nbBulkItems = randomIntBetween(1, 10);

        BulkItemResponse[] bulkItems = new BulkItemResponse[nbBulkItems];
        BulkItemResponse[] expectedBulkItems = new BulkItemResponse[nbBulkItems];

        for (int i = 0; i < nbBulkItems; i++) {
            DocWriteRequest.OpType opType = randomFrom(DocWriteRequest.OpType.values());

            if (frequently()) {
                Tuple<? extends DocWriteResponse, ? extends DocWriteResponse> randomDocWriteResponses = null;
                if (opType == DocWriteRequest.OpType.INDEX || opType == DocWriteRequest.OpType.CREATE) {
                    randomDocWriteResponses = IndexResponseTests.randomIndexResponse();
                } else if (opType == DocWriteRequest.OpType.DELETE) {
                    randomDocWriteResponses = DeleteResponseTests.randomDeleteResponse();
                } else if (opType == DocWriteRequest.OpType.UPDATE) {
                    randomDocWriteResponses = UpdateResponseTests.randomUpdateResponse(xContentType);
                } else {
                    fail("Test does not support opType [" + opType + "]");
                }

                bulkItems[i] = new BulkItemResponse(i, opType, randomDocWriteResponses.v1());
                expectedBulkItems[i] = new BulkItemResponse(i, opType, randomDocWriteResponses.v2());
            } else {
                String index = randomAlphaOfLength(5);
                String id = randomAlphaOfLength(5);

                Tuple<Throwable, OpenSearchException> failures = randomExceptions();

                Exception bulkItemCause = (Exception) failures.v1();
                bulkItems[i] = new BulkItemResponse(i, opType, new BulkItemResponse.Failure(index, id, bulkItemCause));
                expectedBulkItems[i] = new BulkItemResponse(
                    i,
                    opType,
                    new BulkItemResponse.Failure(index, id, failures.v2(), ExceptionsHelper.status(bulkItemCause))
                );
            }
        }

        BulkResponse bulkResponse = new BulkResponse(bulkItems, took, ingestTook);
        BytesReference originalBytes = toShuffledXContent(bulkResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BulkResponse parsedBulkResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedBulkResponse = BulkResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        assertEquals(took, parsedBulkResponse.getTook().getMillis());
        assertEquals(ingestTook, parsedBulkResponse.getIngestTookInMillis());
        assertEquals(expectedBulkItems.length, parsedBulkResponse.getItems().length);

        for (int i = 0; i < expectedBulkItems.length; i++) {
            assertBulkItemResponse(expectedBulkItems[i], parsedBulkResponse.getItems()[i]);
        }

        BytesReference finalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        BytesReference expectedFinalBytes = toXContent(parsedBulkResponse, xContentType, humanReadable);
        assertToXContentEquivalent(expectedFinalBytes, finalBytes, xContentType);
    }
}
