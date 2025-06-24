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

import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.rest.RestRequest;
import org.opensearch.script.ScriptType;
import org.opensearch.search.Scroll;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.StreamsUtils;
import org.opensearch.test.rest.FakeRestRequest;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import static org.hamcrest.Matchers.arrayContaining;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.nullValue;

public class MultiSearchTemplateRequestTests extends OpenSearchTestCase {

    public void testParseRequest() throws Exception {
        byte[] data = StreamsUtils.copyToBytesFromClasspath("/org/opensearch/script/mustache/simple-msearch-template.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), MediaTypeRegistry.JSON)
            .build();

        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        assertThat(request.requests().size(), equalTo(3));
        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).getRequest().indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().requestCache(), equalTo(true));
        assertThat(request.requests().get(0).getRequest().preference(), nullValue());
        assertThat(request.requests().get(1).getRequest().indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).getRequest().indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).getRequest().requestCache(), nullValue());
        assertThat(request.requests().get(1).getRequest().preference(), equalTo("_local"));
        assertThat(request.requests().get(2).getRequest().indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).getRequest().routing(), equalTo("123"));
        assertNotNull(request.requests().get(0).getScript());
        assertNotNull(request.requests().get(1).getScript());
        assertNotNull(request.requests().get(2).getScript());

        assertEquals(ScriptType.INLINE, request.requests().get(0).getScriptType());
        assertEquals(ScriptType.INLINE, request.requests().get(1).getScriptType());
        assertEquals(ScriptType.INLINE, request.requests().get(2).getScriptType());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(0).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(1).getScript());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(2).getScript());
        assertEquals(1, request.requests().get(0).getScriptParams().size());
        assertEquals(1, request.requests().get(1).getScriptParams().size());
        assertEquals(1, request.requests().get(2).getScriptParams().size());
    }

    public void testParseWithCarriageReturn() throws Exception {
        final String content = "{\"index\":[\"test0\", \"test1\"], \"request_cache\": true}\r\n"
            + "{\"source\": {\"query\" : {\"match_{{template}}\" :{}}}, \"params\": {\"template\": \"all\" } }\r\n";
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(content),
            MediaTypeRegistry.JSON
        ).build();

        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        assertThat(request.requests().size(), equalTo(1));
        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).getRequest().indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().requestCache(), equalTo(true));
        assertThat(request.requests().get(0).getRequest().preference(), nullValue());
        assertNotNull(request.requests().get(0).getScript());
        assertEquals(ScriptType.INLINE, request.requests().get(0).getScriptType());
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", request.requests().get(0).getScript());
        assertEquals(1, request.requests().get(0).getScriptParams().size());
    }

    public void testMaxConcurrentSearchRequests() {
        MultiSearchTemplateRequest request = new MultiSearchTemplateRequest();
        request.maxConcurrentSearchRequests(randomIntBetween(1, Integer.MAX_VALUE));
        expectThrows(IllegalArgumentException.class, () -> request.maxConcurrentSearchRequests(randomIntBetween(Integer.MIN_VALUE, 0)));
    }

    public void testMultiSearchTemplateToJson() throws Exception {
        final int numSearchRequests = randomIntBetween(1, 10);
        MultiSearchTemplateRequest multiSearchTemplateRequest = new MultiSearchTemplateRequest();
        for (int i = 0; i < numSearchRequests; i++) {
            // Create a random request.
            String[] indices = { "test" };
            SearchRequest searchRequest = new SearchRequest(indices);
            // scroll is not supported in the current msearch or msearchtemplate api, so unset it:
            searchRequest.scroll((Scroll) null);
            searchRequest.pipeline("pipeline");
            // batched reduce size is currently not set-able on a per-request basis as it is a query string parameter only
            searchRequest.setBatchedReduceSize(SearchRequest.DEFAULT_BATCHED_REDUCE_SIZE);
            SearchTemplateRequest searchTemplateRequest = new SearchTemplateRequest(searchRequest);

            searchTemplateRequest.setScript("{\"query\": { \"match\" : { \"{{field}}\" : \"{{value}}\" }}}");
            searchTemplateRequest.setScriptType(ScriptType.INLINE);
            searchTemplateRequest.setProfile(randomBoolean());
            searchTemplateRequest.setSearchPipeline("pipeline");

            Map<String, Object> scriptParams = new HashMap<>();
            scriptParams.put("field", "name");
            scriptParams.put("value", randomAlphaOfLengthBetween(2, 5));
            searchTemplateRequest.setScriptParams(scriptParams);

            multiSearchTemplateRequest.add(searchTemplateRequest);
        }

        // Serialize the request
        String serialized = toJsonString(multiSearchTemplateRequest);

        // Deserialize the request
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(
            new BytesArray(serialized),
            MediaTypeRegistry.JSON
        ).build();
        MultiSearchTemplateRequest deser = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        // For object equality purposes need to set the search requests' source to non-null
        for (SearchTemplateRequest str : deser.requests()) {
            SearchRequest sr = str.getRequest();
            if (sr.source() == null) {
                sr.source(new SearchSourceBuilder());
            }
        }
        // Compare the deserialized request object with the original request object
        assertEquals(multiSearchTemplateRequest, deser);

        // Finally, serialize the deserialized request to compare JSON equivalence (in case Object.equals() fails to reveal a discrepancy)
        assertEquals(serialized, toJsonString(deser));
    }

    public void testParseRequestWithSearchPipeline() throws Exception {
        byte[] data = StreamsUtils.copyToBytesFromClasspath("/org/opensearch/script/mustache/simple-msearch-template.json");
        RestRequest restRequest = new FakeRestRequest.Builder(xContentRegistry()).withContent(new BytesArray(data), MediaTypeRegistry.JSON)
            .build();

        MultiSearchTemplateRequest request = RestMultiSearchTemplateAction.parseRequest(restRequest, true);

        assertThat(request.requests().size(), equalTo(3));
        SearchTemplateRequest searchTemplateRequest = request.requests().get(0);

        assertThat(request.requests().get(0).getRequest().indices()[0], equalTo("test0"));
        assertThat(request.requests().get(0).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(0).getRequest().indices(), arrayContaining("test0", "test1"));
        assertThat(request.requests().get(0).getRequest().pipeline(), equalTo("my_pipeline"));
        assertThat(request.requests().get(1).getRequest().indices()[0], equalTo("test2"));
        assertThat(request.requests().get(1).getRequest().indices()[1], equalTo("test3"));
        assertThat(request.requests().get(1).getRequest().indices(), arrayContaining("test2", "test3"));
        assertThat(request.requests().get(1).getRequest().pipeline(), equalTo("my_pipeline1"));
        assertThat(request.requests().get(2).getRequest().indices()[0], equalTo("test4"));
        assertThat(request.requests().get(2).getRequest().indices()[1], equalTo("test1"));
        assertThat(request.requests().get(2).getRequest().indices(), arrayContaining("test4", "test1"));
        assertThat(request.requests().get(2).getRequest().pipeline(), equalTo("my_pipeline2"));

        // Additional validation
        assertEquals("{\"query\":{\"match_{{template}}\":{}}}", searchTemplateRequest.getScript());
        assertEquals("all", searchTemplateRequest.getScriptParams().get("template"));
    }

    protected String toJsonString(MultiSearchTemplateRequest multiSearchTemplateRequest) throws IOException {
        byte[] bytes = MultiSearchTemplateRequest.writeMultiLineFormat(multiSearchTemplateRequest, MediaTypeRegistry.JSON.xContent());
        return new String(bytes, StandardCharsets.UTF_8);
    }

}
