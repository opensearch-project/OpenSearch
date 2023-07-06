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

import org.opensearch.OpenSearchException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.test.AbstractXContentTestCase;

import java.io.IOException;
import java.util.function.Predicate;
import java.util.function.Supplier;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchResponseTests extends AbstractXContentTestCase<MultiSearchResponse> {

    @Override
    protected MultiSearchResponse createTestInstance() {
        int numItems = randomIntBetween(0, 128);
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            // Creating a minimal response is OK, because SearchResponse self
            // is tested elsewhere.
            long tookInMillis = randomNonNegativeLong();
            int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
            int successfulShards = randomIntBetween(0, totalShards);
            int skippedShards = totalShards - successfulShards;
            SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
            InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
            SearchResponse searchResponse = new SearchResponse(
                internalSearchResponse,
                null,
                totalShards,
                successfulShards,
                skippedShards,
                tookInMillis,
                ShardSearchFailure.EMPTY_ARRAY,
                clusters,
                null
            );
            items[i] = new MultiSearchResponse.Item(searchResponse, null);
        }
        return new MultiSearchResponse(items, randomNonNegativeLong());
    }

    private static MultiSearchResponse createTestInstanceWithFailures() {
        int numItems = randomIntBetween(0, 128);
        MultiSearchResponse.Item[] items = new MultiSearchResponse.Item[numItems];
        for (int i = 0; i < numItems; i++) {
            if (randomBoolean()) {
                // Creating a minimal response is OK, because SearchResponse is tested elsewhere.
                long tookInMillis = randomNonNegativeLong();
                int totalShards = randomIntBetween(1, Integer.MAX_VALUE);
                int successfulShards = randomIntBetween(0, totalShards);
                int skippedShards = totalShards - successfulShards;
                SearchResponse.Clusters clusters = SearchResponseTests.randomClusters();
                InternalSearchResponse internalSearchResponse = InternalSearchResponse.empty();
                SearchResponse searchResponse = new SearchResponse(
                    internalSearchResponse,
                    null,
                    totalShards,
                    successfulShards,
                    skippedShards,
                    tookInMillis,
                    ShardSearchFailure.EMPTY_ARRAY,
                    clusters,
                    null
                );
                items[i] = new MultiSearchResponse.Item(searchResponse, null);
            } else {
                items[i] = new MultiSearchResponse.Item(null, new OpenSearchException("an error"));
            }
        }
        return new MultiSearchResponse(items, randomNonNegativeLong());
    }

    @Override
    protected MultiSearchResponse doParseInstance(XContentParser parser) throws IOException {
        return MultiSearchResponse.fromXContext(parser);
    }

    @Override
    protected void assertEqualInstances(MultiSearchResponse expected, MultiSearchResponse actual) {
        assertThat(actual.getTook(), equalTo(expected.getTook()));
        assertThat(actual.getResponses().length, equalTo(expected.getResponses().length));
        for (int i = 0; i < expected.getResponses().length; i++) {
            MultiSearchResponse.Item expectedItem = expected.getResponses()[i];
            MultiSearchResponse.Item actualItem = actual.getResponses()[i];
            if (expectedItem.isFailure()) {
                assertThat(actualItem.getResponse(), nullValue());
                assertThat(actualItem.getFailureMessage(), containsString(expectedItem.getFailureMessage()));
            } else {
                assertThat(actualItem.getResponse().toString(), equalTo(expectedItem.getResponse().toString()));
                assertThat(actualItem.getFailure(), nullValue());
            }
        }
    }

    @Override
    protected boolean supportsUnknownFields() {
        return true;
    }

    protected Predicate<String> getRandomFieldsExcludeFilterWhenResultHasErrors() {
        return field -> field.startsWith("responses");
    }

    /**
     * Test parsing {@link MultiSearchResponse} with inner failures as they don't support asserting on xcontent equivalence, given that
     * exceptions are not parsed back as the same original class. We run the usual {@link AbstractXContentTestCase#testFromXContent()}
     * without failures, and this other test with failures where we disable asserting on xcontent equivalence at the end.
     */
    public void testFromXContentWithFailures() throws IOException {
        Supplier<MultiSearchResponse> instanceSupplier = MultiSearchResponseTests::createTestInstanceWithFailures;
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

}
