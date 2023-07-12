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

package org.opensearch.action.delete;

import org.opensearch.action.support.replication.ReplicationResponse;
import org.opensearch.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.RandomObjects;

import java.io.IOException;
import java.util.function.Predicate;

import static org.opensearch.action.index.IndexResponseTests.assertDocWriteResponse;
import static org.opensearch.cluster.metadata.IndexMetadata.INDEX_UUID_NA_VALUE;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;

public class DeleteResponseTests extends OpenSearchTestCase {

    public void testToXContent() {
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "id", 3, 17, 5, true);
            String output = Strings.toString(XContentType.JSON, response);
            assertEquals(
                "{\"_index\":\"index\",\"_id\":\"id\",\"_version\":5,\"result\":\"deleted\","
                    + "\"_shards\":null,\"_seq_no\":3,\"_primary_term\":17}",
                output
            );
        }
        {
            DeleteResponse response = new DeleteResponse(new ShardId("index", "index_uuid", 0), "id", -1, 0, 7, true);
            response.setForcedRefresh(true);
            response.setShardInfo(new ReplicationResponse.ShardInfo(10, 5));
            String output = Strings.toString(XContentType.JSON, response);
            assertEquals(
                "{\"_index\":\"index\",\"_id\":\"id\",\"_version\":7,\"result\":\"deleted\","
                    + "\"forced_refresh\":true,\"_shards\":{\"total\":10,\"successful\":5,\"failed\":0}}",
                output
            );
        }
    }

    public void testToAndFromXContent() throws IOException {
        doFromXContentTestWithRandomFields(false);
    }

    /**
     * This test adds random fields and objects to the xContent rendered out to
     * ensure we can parse it back to be forward compatible with additions to
     * the xContent
     */
    public void testFromXContentWithRandomFields() throws IOException {
        doFromXContentTestWithRandomFields(true);
    }

    private void doFromXContentTestWithRandomFields(boolean addRandomFields) throws IOException {
        final Tuple<DeleteResponse, DeleteResponse> tuple = randomDeleteResponse();
        DeleteResponse deleteResponse = tuple.v1();
        DeleteResponse expectedDeleteResponse = tuple.v2();

        boolean humanReadable = randomBoolean();
        final XContentType xContentType = randomFrom(XContentType.values());
        BytesReference originalBytes = toShuffledXContent(deleteResponse, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);

        BytesReference mutated;
        if (addRandomFields) {
            // The ShardInfo.Failure's exception is rendered out in a "reason" object. We shouldn't add anything random there
            // because exception rendering and parsing are very permissive: any extra object or field would be rendered as
            // a exception custom metadata and be parsed back as a custom header, making it impossible to compare the results
            // in this test.
            Predicate<String> excludeFilter = path -> path.contains("reason");
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        DeleteResponse parsedDeleteResponse;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsedDeleteResponse = DeleteResponse.fromXContent(parser);
            assertNull(parser.nextToken());
        }

        // We can't use equals() to compare the original and the parsed delete response
        // because the random delete response can contain shard failures with exceptions,
        // and those exceptions are not parsed back with the same types.
        assertDocWriteResponse(expectedDeleteResponse, parsedDeleteResponse);
    }

    /**
     * Returns a tuple of {@link DeleteResponse}s.
     * <p>
     * The left element is the actual {@link DeleteResponse} to serialize while the right element is the
     * expected {@link DeleteResponse} after parsing.
     */
    public static Tuple<DeleteResponse, DeleteResponse> randomDeleteResponse() {
        String index = randomAlphaOfLength(5);
        String indexUUid = randomAlphaOfLength(5);
        int shardId = randomIntBetween(0, 5);
        String type = randomAlphaOfLength(5);
        String id = randomAlphaOfLength(5);
        long seqNo = randomFrom(SequenceNumbers.UNASSIGNED_SEQ_NO, randomNonNegativeLong(), (long) randomIntBetween(0, 10000));
        long primaryTerm = seqNo == SequenceNumbers.UNASSIGNED_SEQ_NO ? 0 : randomIntBetween(1, 10000);
        long version = randomBoolean() ? randomNonNegativeLong() : randomIntBetween(0, 10000);
        boolean found = randomBoolean();
        boolean forcedRefresh = randomBoolean();

        Tuple<ReplicationResponse.ShardInfo, ReplicationResponse.ShardInfo> shardInfos = RandomObjects.randomShardInfo(random());

        DeleteResponse actual = new DeleteResponse(new ShardId(index, indexUUid, shardId), id, seqNo, primaryTerm, version, found);
        actual.setForcedRefresh(forcedRefresh);
        actual.setShardInfo(shardInfos.v1());

        DeleteResponse expected = new DeleteResponse(new ShardId(index, INDEX_UUID_NA_VALUE, -1), id, seqNo, primaryTerm, version, found);
        expected.setForcedRefresh(forcedRefresh);
        expected.setShardInfo(shardInfos.v2());

        return Tuple.tuple(actual, expected);
    }

}
