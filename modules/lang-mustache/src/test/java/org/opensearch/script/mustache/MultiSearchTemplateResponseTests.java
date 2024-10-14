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

package org.opensearch.script.mustache;

import org.opensearch.OpenSearchException;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateResponseTests extends AbstractXContentTestCase<MultiSearchTemplateResponse> {

    @Override
    protected MultiSearchTemplateResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            // Creating a minimal response is OK, because SearchResponse self
            // is tested elsewhere.
            long tookInMillis = randomNonNegativeLong();
            int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
            int successfulShards = randomIntBetween(0, totalShards);
            int skippedShards = totalShards - successfulShards;
            InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
            SearchResponse.Clusters clusters = randomClusters();
            SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
            SearchResponse searchResponse = new SearchResponse(
                internalSearchResponse,
                null,
                totalShards,
                successfulShards,
                skippedShards,
                tookInMillis,
                ShardSearchFailure.EMPTY_ARRAY,
                clusters
            );
            searchTemplateResponse.setResponse(searchResponse);
            items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
        }
        return new MultiSearchTemplateResponse(items, overallTookInMillis);
    }

    private static SearchResponse.Clusters randomClusters() {
        int totalClusters = randomIntBetween(0, 10);
        int successfulClusters = randomIntBetween(0, totalClusters);
        int skippedClusters = totalClusters - successfulClusters;
        return new SearchResponse.Clusters(totalClusters, successfulClusters, skippedClusters);
    }

    private static MultiSearchTemplateResponse createTestInstanceWithFailures() {
        int numItems = randomIntBetween(0, 128);
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            if (randomBoolean()) {
                // Creating a minimal response is OK, because SearchResponse is tested elsewhere.
                long tookInMillis = randomNonNegativeLong();
                int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                int successfulShards = randomIntBetween(0, totalShards);
                int skippedShards = totalShards - successfulShards;
                InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
                SearchResponse.Clusters clusters = randomClusters();
                SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
                SearchResponse searchResponse = new SearchResponse(
                    internalSearchResponse,
                    null,
                    totalShards,
                    successfulShards,
                    skippedShards,
                    tookInMillis,
                    ShardSearchFailure.EMPTY_ARRAY,
                    clusters
                );
                searchTemplateResponse.setResponse(searchResponse);
                items[i] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);
            } else {
                items[i] = new MultiSearchTemplateResponse.Item(null, new OpenSearchException("an error"));
            }
        }
        return new MultiSearchTemplateResponse(items, overallTookInMillis);
    }

    @Override
    protected MultiSearchTemplateResponse doParseInstance(XContentParser parser) throws IOException {
        return MultiSearchTemplateResponse.fromXContext(parser);
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    protected Predicate<String> getRandomFieldsExcludeFilterWhenResultHasErrors() {
        return field -> field.startsWith("responses");
    }

    @Override
    protected void assertEqualInstances(MultiSearchTemplateResponse expectedInstance, MultiSearchTemplateResponse newInstance) {
        assertThat(newInstance.getTook(), equalTo(expectedInstance.getTook()));
        assertThat(newInstance.getResponses().length, equalTo(expectedInstance.getResponses().length));
        for (int i = 0; i < expectedInstance.getResponses().length; i++) {
            MultiSearchTemplateResponse.Item expectedItem = expectedInstance.getResponses()[i];
            MultiSearchTemplateResponse.Item actualItem = newInstance.getResponses()[i];
            if (expectedItem.isFailure()) {
                assertThat(actualItem.getResponse(), nullValue());
                assertThat(actualItem.getFailureMessage(), containsString(expectedItem.getFailureMessage()));
            } else {
                assertThat(actualItem.getResponse().toString(), equalTo(expectedItem.getResponse().toString()));
                assertThat(actualItem.getFailure(), nullValue());
            }
        }
    }

    /**
     * Test parsing {@link MultiSearchTemplateResponse} with inner failures as they don't support asserting on xcontent equivalence, given
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<MultiSearchTemplateResponse> instanceSupplier = MultiSearchTemplateResponseTests::createTestInstanceWithFailures;
        // with random fields insertion in the inner exceptions, some random stuff may be parsed back as metadata,
        // but that does not bother our assertions, as we only want to test that we don't break.
        boolean supportsUnknownFields = true;
        // exceptions are not of the same type whenever parsed back
        boolean assertToXContentEquivalence = false;
        AbstractXContentTestCase.testFromXContent(
            NUMBER_OF_TEST_RUNS,
            instanceSupplier,
            supportsUnknownFields,
            Strings.EMPTY_ARRAY,
            getRandomFieldsExcludeFilterWhenResultHasErrors(),
            this::createParser,
            this::doParseInstance,
            this::assertEqualInstances,
            assertToXContentEquivalence,
            ToXContent.EMPTY_PARAMS
        );
    }

    public void testToXContentWithFailures() throws Exception {
        long overallTookInMillis = randomNonNegativeLong();
        MultiSearchTemplateResponse.Item[] items = new MultiSearchTemplateResponse.Item[2];

        long tookInMillis = 1L;
        int totalShards = 2;
        int successfulShards = 2;
        int skippedShards = 0;
        InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
        SearchTemplateResponse searchTemplateResponse = new SearchTemplateResponse();
        SearchResponse searchResponse = new SearchResponse(
            internalSearchResponse,
            null,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            ShardSearchFailure.EMPTY_ARRAY,
            SearchResponse.Clusters.EMPTY
        );
        searchTemplateResponse.setResponse(searchResponse);
        items[0] = new MultiSearchTemplateResponse.Item(searchTemplateResponse, null);

        items[1] = new MultiSearchTemplateResponse.Item(null, new IllegalArgumentException("Invalid argument"));

        MultiSearchTemplateResponse response = new MultiSearchTemplateResponse(items, overallTookInMillis);

        XContentType contentType = randomFrom(XContentType.values());
        XContentBuilder expectedResponse = MediaTypeRegistry.contentBuilder(contentType)
            .startObject()
            .field("took", overallTookInMillis)
            .startArray("responses")
            .startObject()
            .field("took", 1)
            .field("timed_out", false)
            .startObject("_shards")
            .field("total", 2)
            .field("successful", 2)
            .field("skipped", 0)
            .field("failed", 0)
            .endObject()
            .startObject("hits")
            .startObject("total")
            .field("value", 0)
            .field("relation", "eq")
            .endObject()
            .field("max_score", 0.0F)
            .startArray("hits")
            .endArray()
            .endObject()
            .field("status", 200)
            .endObject()
            .startObject()
            .startObject("error")
            .field("type", "illegal_argument_exception")
            .field("reason", "Invalid argument")
            .startArray("root_cause")
            .startObject()
            .field("type", "illegal_argument_exception")
            .field("reason", "Invalid argument")
            .endObject()
            .endArray()
            .endObject()
            .field("status", 400)
            .endObject()
            .endArray()
            .endObject();

        XContentBuilder actualResponse = MediaTypeRegistry.contentBuilder(contentType);
        response.toXContent(actualResponse, ToXContent.EMPTY_PARAMS);

        assertToXContentEquivalent(BytesReference.bytes(expectedResponse), BytesReference.bytes(actualResponse), contentType);
    }
}
