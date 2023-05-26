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

package org.opensearch.action.search;

import org.opensearch.common.Strings;
import org.opensearch.common.UUIDs;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.search.internal.InternalScrollSearchRequest;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

import static org.opensearch.test.EqualsHashCodeTestUtils.checkEqualsAndHashCode;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.startsWith;

public class SearchScrollRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        SearchScrollRequest searchScrollRequest = createSearchScrollRequest();
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            searchScrollRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                SearchScrollRequest deserializedRequest = new SearchScrollRequest(in);
                assertEquals(deserializedRequest, searchScrollRequest);
                assertEquals(deserializedRequest.hashCode(), searchScrollRequest.hashCode());
                assertNotSame(deserializedRequest, searchScrollRequest);
            }
        }
    }

    public void testInternalScrollSearchRequestSerialization() throws IOException {
        SearchScrollRequest searchScrollRequest = createSearchScrollRequest();
        InternalScrollSearchRequest internalScrollSearchRequest = new InternalScrollSearchRequest(
            searchScrollRequest,
            new ShardSearchContextId(UUIDs.randomBase64UUID(), randomLong())
        );
        try (BytesStreamOutput output = new BytesStreamOutput()) {
            internalScrollSearchRequest.writeTo(output);
            try (StreamInput in = output.bytes().streamInput()) {
                InternalScrollSearchRequest deserializedRequest = new InternalScrollSearchRequest(in);
                assertEquals(deserializedRequest.contextId().getId(), internalScrollSearchRequest.contextId().getId());
                assertEquals(deserializedRequest.scroll(), internalScrollSearchRequest.scroll());
                assertNotSame(deserializedRequest, internalScrollSearchRequest);
            }
        }
    }

    public void testFromXContent() throws Exception {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        if (randomBoolean()) {
            // test that existing values get overridden
            searchScrollRequest = createSearchScrollRequest();
        }
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder().startObject().field("scroll_id", "SCROLL_ID").field("scroll", "1m").endObject()
            )
        ) {
            searchScrollRequest.fromXContent(parser);
        }
        assertEquals("SCROLL_ID", searchScrollRequest.scrollId());
        assertEquals(TimeValue.parseTimeValue("1m", null, "scroll"), searchScrollRequest.scroll().keepAlive());
    }

    public void testFromXContentWithUnknownParamThrowsException() throws Exception {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        XContentParser invalidContent = createParser(
            XContentFactory.jsonBuilder().startObject().field("scroll_id", "value_2").field("unknown", "keyword").endObject()
        );

        Exception e = expectThrows(IllegalArgumentException.class, () -> searchScrollRequest.fromXContent(invalidContent));
        assertThat(e.getMessage(), startsWith("Unknown parameter [unknown]"));
    }

    public void testToXContent() throws IOException {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest();
        searchScrollRequest.scrollId("SCROLL_ID");
        searchScrollRequest.scroll("1m");
        try (XContentBuilder builder = JsonXContent.contentBuilder()) {
            searchScrollRequest.toXContent(builder, ToXContent.EMPTY_PARAMS);
            assertEquals("{\"scroll_id\":\"SCROLL_ID\",\"scroll\":\"1m\"}", Strings.toString(builder));
        }
    }

    public void testToAndFromXContent() throws IOException {
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        SearchScrollRequest originalRequest = createSearchScrollRequest();
        BytesReference originalBytes = toShuffledXContent(originalRequest, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        SearchScrollRequest parsedRequest = new SearchScrollRequest();
        try (XContentParser parser = createParser(xContentType.xContent(), originalBytes)) {
            parsedRequest.fromXContent(parser);
        }
        assertEquals(originalRequest, parsedRequest);
        BytesReference parsedBytes = XContentHelper.toXContent(parsedRequest, xContentType, humanReadable);
        assertToXContentEquivalent(originalBytes, parsedBytes, xContentType);
    }

    public void testEqualsAndHashcode() {
        checkEqualsAndHashCode(createSearchScrollRequest(), SearchScrollRequestTests::copyRequest, SearchScrollRequestTests::mutate);
    }

    public static SearchScrollRequest createSearchScrollRequest() {
        SearchScrollRequest searchScrollRequest = new SearchScrollRequest(randomAlphaOfLengthBetween(3, 10));
        searchScrollRequest.scroll(randomPositiveTimeValue());
        return searchScrollRequest;
    }

    private static SearchScrollRequest copyRequest(SearchScrollRequest searchScrollRequest) {
        SearchScrollRequest result = new SearchScrollRequest();
        result.scrollId(searchScrollRequest.scrollId());
        result.scroll(searchScrollRequest.scroll());
        return result;
    }

    private static SearchScrollRequest mutate(SearchScrollRequest original) {
        SearchScrollRequest copy = copyRequest(original);
        if (randomBoolean()) {
            return copy.scrollId(original.scrollId() + "xyz");
        } else {
            return copy.scroll(new TimeValue(original.scroll().keepAlive().getMillis() + 1));
        }
    }
}
