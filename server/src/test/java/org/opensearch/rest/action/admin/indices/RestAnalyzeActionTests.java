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

package org.opensearch.rest.action.admin.indices;

import org.opensearch.action.admin.indices.analyze.AnalyzeAction;
import org.opensearch.client.node.NodeClient;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.index.analysis.NameOrDefinition;
import org.opensearch.rest.RestRequest;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.client.NoOpNodeClient;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class RestAnalyzeActionTests extends OpenSearchTestCase {

    public void testParseXContentForAnalyzeRequest() throws Exception {
        try (
            XContentParser content = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .array("filter", "lowercase")
                    .endObject()
            )
        ) {

            AnalyzeAction.Request analyzeRequest = AnalyzeAction.Request.fromXContent(content, "for test");

            assertThat(analyzeRequest.text().length, equalTo(1));
            assertThat(analyzeRequest.text(), equalTo(new String[] { "THIS IS A TEST" }));
            assertThat(analyzeRequest.tokenizer().name, equalTo("keyword"));
            assertThat(analyzeRequest.tokenFilters().size(), equalTo(1));
            for (NameOrDefinition filter : analyzeRequest.tokenFilters()) {
                assertThat(filter.name, equalTo("lowercase"));
            }
        }
    }

    public void testParseXContentForAnalyzeRequestWithCustomFilters() throws Exception {
        try (
            XContentParser content = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .startArray("filter")
                    .value("lowercase")
                    .startObject()
                    .field("type", "stop")
                    .array("stopwords", "foo", "buzz")
                    .endObject()
                    .endArray()
                    .startArray("char_filter")
                    .startObject()
                    .field("type", "mapping")
                    .array("mappings", "ph => f", "qu => q")
                    .endObject()
                    .endArray()
                    .field("normalizer", "normalizer")
                    .endObject()
            )
        ) {

            AnalyzeAction.Request analyzeRequest = AnalyzeAction.Request.fromXContent(content, "for test");

            assertThat(analyzeRequest.text().length, equalTo(1));
            assertThat(analyzeRequest.text(), equalTo(new String[] { "THIS IS A TEST" }));
            assertThat(analyzeRequest.tokenizer().name, equalTo("keyword"));
            assertThat(analyzeRequest.tokenFilters().size(), equalTo(2));
            assertThat(analyzeRequest.tokenFilters().get(0).name, equalTo("lowercase"));
            assertThat(analyzeRequest.tokenFilters().get(1).definition, notNullValue());
            assertThat(analyzeRequest.charFilters().size(), equalTo(1));
            assertThat(analyzeRequest.charFilters().get(0).definition, notNullValue());
            assertThat(analyzeRequest.normalizer(), equalTo("normalizer"));
        }
    }

    public void testParseXContentForAnalyzeRequestWithInvalidJsonThrowsException() {
        RestAnalyzeAction action = new RestAnalyzeAction();
        RestRequest request = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray("{invalid_json}"),
            XContentType.JSON
        ).build();
        try (NodeClient client = new NoOpNodeClient(this.getClass().getSimpleName())) {
            IOException e = expectThrows(IOException.class, () -> action.handleRequest(request, null, client));
            assertThat(e.getMessage(), containsString("expecting double-quote"));
        }
    }

    public void testParseXContentForAnalyzeRequestWithUnknownParamThrowsException() throws Exception {
        try (
            XContentParser invalidContent = createParser(
                XContentFactory.jsonBuilder().startObject().field("text", "THIS IS A TEST").field("unknown", "keyword").endObject()
            )
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(invalidContent, "for test")
            );
            assertThat(e.getMessage(), containsString("unknown field [unknown]"));
        }
    }

    public void testParseXContentForAnalyzeRequestWithInvalidStringExplainParamThrowsException() throws Exception {
        try (
            XContentParser invalidExplain = createParser(XContentFactory.jsonBuilder().startObject().field("explain", "fals").endObject())
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(invalidExplain, "for test")
            );
            assertThat(e.getMessage(), containsString("failed to parse field [explain]"));
        }
    }

    public void testParseXContentForAnalyzeRequestWithInvalidNormalizerThrowsException() throws Exception {
        try (
            XContentParser invalidExplain = createParser(XContentFactory.jsonBuilder().startObject().field("normalizer", true).endObject())
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(invalidExplain, "for test")
            );
            assertThat(e.getMessage(), containsString("normalizer doesn't support values of type: VALUE_BOOLEAN"));
        }
    }

    public void testDeprecatedParamIn2xException() throws Exception {
        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .array("filters", "lowercase")
                    .endObject()
            )
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(parser, "for test")
            );
            assertThat(e.getMessage(), containsString("unknown field [filters]"));
        }

        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .array("token_filters", "lowercase")
                    .endObject()
            )
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(parser, "for test")
            );
            assertThat(e.getMessage(), containsString("unknown field [token_filters]"));
        }

        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .array("char_filters", "lowercase")
                    .endObject()
            )
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(parser, "for test")
            );
            assertThat(e.getMessage(), containsString("unknown field [char_filters]"));
        }

        try (
            XContentParser parser = createParser(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .field("text", "THIS IS A TEST")
                    .field("tokenizer", "keyword")
                    .array("token_filter", "lowercase")
                    .endObject()
            )
        ) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> AnalyzeAction.Request.fromXContent(parser, "for test")
            );
            assertThat(e.getMessage(), containsString("unknown field [token_filter]"));
        }
    }
}
