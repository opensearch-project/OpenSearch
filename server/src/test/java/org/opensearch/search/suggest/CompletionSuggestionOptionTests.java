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

package org.opensearch.search.suggest;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.text.Text;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHitTests;
import org.opensearch.search.suggest.completion.CompletionSuggestion;
import org.opensearch.search.suggest.completion.CompletionSuggestion.Entry.Option;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.function.Predicate;

import static org.opensearch.common.xcontent.XContentHelper.toXContent;
import static org.opensearch.test.XContentTestUtils.insertRandomFields;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertToXContentEquivalent;

public class CompletionSuggestionOptionTests extends OpenSearchTestCase {

    public static Option createTestItem() {
        Text text = new Text(randomAlphaOfLengthBetween(5, 15));
        int docId = randomInt();
        int numberOfContexts = randomIntBetween(0, 3);
        Map<String, Set<String>> contexts = new HashMap<>();
        for (int i = 0; i < numberOfContexts; i++) {
            int numberOfValues = randomIntBetween(0, 3);
            Set<String> values = new HashSet<>();
            for (int v = 0; v < numberOfValues; v++) {
                values.add(randomAlphaOfLengthBetween(5, 15));
            }
            contexts.put(randomAlphaOfLengthBetween(5, 15), values);
        }
        SearchHit hit = null;
        float score = randomFloat();
        if (randomBoolean()) {
            hit = SearchHitTests.createTestItem(false, true);
            score = hit.getScore();
        }
        Option option = new CompletionSuggestion.Entry.Option(docId, text, score, contexts);
        option.setHit(hit);
        return option;
    }

    public void testFromXContent() throws IOException {
        doTestFromXContent(false);
    }

    public void testFromXContentWithRandomFields() throws IOException {
        doTestFromXContent(true);
    }

    private void doTestFromXContent(boolean addRandomFields) throws IOException {
        Option option = createTestItem();
        XContentType xContentType = randomFrom(XContentType.values());
        boolean humanReadable = randomBoolean();
        BytesReference originalBytes = toShuffledXContent(option, xContentType, ToXContent.EMPTY_PARAMS, humanReadable);
        BytesReference mutated;
        if (addRandomFields) {
            // "contexts" is an object consisting of key/array pairs, we shouldn't add anything random there
            // also there can be inner search hits fields inside this option, we need to exclude another couple of paths
            // where we cannot add random stuff. We also exclude the root level, this is done for SearchHits as all unknown fields
            // for SearchHit on a root level are interpreted as meta-fields and will be kept
            Predicate<String> excludeFilter = (path) -> path.endsWith(CompletionSuggestion.Entry.Option.CONTEXTS.getPreferredName())
                || path.endsWith("highlight")
                || path.contains("fields")
                || path.contains("_source")
                || path.contains("inner_hits")
                || path.isEmpty();
            mutated = insertRandomFields(xContentType, originalBytes, excludeFilter, random());
        } else {
            mutated = originalBytes;
        }
        Option parsed;
        try (XContentParser parser = createParser(xContentType.xContent(), mutated)) {
            parsed = Option.fromXContent(parser);
            assertNull(parser.nextToken());
        }
        assertEquals(option.getText(), parsed.getText());
        assertEquals(option.getHighlighted(), parsed.getHighlighted());
        assertEquals(option.getScore(), parsed.getScore(), Float.MIN_VALUE);
        assertEquals(option.collateMatch(), parsed.collateMatch());
        assertEquals(option.getContexts(), parsed.getContexts());
        assertToXContentEquivalent(originalBytes, toXContent(parsed, xContentType, humanReadable), xContentType);
    }

    public void testToXContent() throws IOException {
        Map<String, Set<String>> contexts = Collections.singletonMap("key", Collections.singleton("value"));
        CompletionSuggestion.Entry.Option option = new CompletionSuggestion.Entry.Option(1, new Text("someText"), 1.3f, contexts);
        BytesReference xContent = toXContent(option, XContentType.JSON, randomBoolean());
        assertEquals("{\"text\":\"someText\",\"score\":1.3,\"contexts\":{\"key\":[\"value\"]}}", xContent.utf8ToString());
    }
}
