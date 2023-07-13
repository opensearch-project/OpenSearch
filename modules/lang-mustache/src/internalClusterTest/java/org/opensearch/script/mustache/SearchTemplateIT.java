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

import org.opensearch.ResourceNotFoundException;
import org.opensearch.action.admin.cluster.storedscripts.GetStoredScriptResponse;
import org.opensearch.action.bulk.BulkRequestBuilder;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.Before;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

/**
 * Full integration test of the template query plugin.
 */
public class SearchTemplateIT extends OpenSearchSingleNodeTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return Collections.singleton(MustacheModulePlugin.class);
    }

    @Before
    public void setup() throws IOException {
        createIndex("test");
        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().field("text", "value1").endObject()).get();
        client().prepareIndex("test").setId("2").setSource(jsonBuilder().startObject().field("text", "value2").endObject()).get();
        client().admin().indices().prepareRefresh().get();
    }

    // Relates to #6318
    public void testSearchRequestFail() throws Exception {
        String query = "{ \"query\": {\"match_all\": {}}, \"size\" : \"{{my_size}}\"  }";

        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");

        expectThrows(
            Exception.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
                .setScript(query)
                .setScriptType(ScriptType.INLINE)
                .setScriptParams(randomBoolean() ? null : Collections.emptyMap())
                .get()
        );

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(searchRequest)
            .setScript(query)
            .setScriptType(ScriptType.INLINE)
            .setScriptParams(Collections.singletonMap("my_size", 1))
            .get();

        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can be expressed as a single escaped string.
     */
    public void testTemplateQueryAsEscapedString() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String query = "{"
            + "  \"source\" : \"{ \\\"size\\\": \\\"{{size}}\\\", \\\"query\\\":{\\\"match_all\\\":{}}}\","
            + "  \"params\":{"
            + "    \"size\": 1"
            + "  }"
            + "}";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, query));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the beginning of the string.
     */
    public void testTemplateQueryAsEscapedStringStartingWithConditionalClause() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString = "{"
            + "  \"source\" : \"{ {{#use_size}} \\\"size\\\": \\\"{{size}}\\\", {{/use_size}} \\\"query\\\":{\\\"match_all\\\":{}}}\","
            + "  \"params\":{"
            + "    \"size\": 1,"
            + "    \"use_size\": true"
            + "  }"
            + "}";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    /**
     * Test that template can contain conditional clause. In this case it is at
     * the end of the string.
     */
    public void testTemplateQueryAsEscapedStringWithConditionalClauseAtEnd() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        searchRequest.indices("_all");
        String templateString = "{"
            + "  \"source\" : \"{ \\\"query\\\":{\\\"match_all\\\":{}} {{#use_size}}, \\\"size\\\": \\\"{{size}}\\\" {{/use_size}} }\","
            + "  \"params\":{"
            + "    \"size\": 1,"
            + "    \"use_size\": true"
            + "  }"
            + "}";
        SearchTemplateRequest request = SearchTemplateRequest.fromXContent(createParser(JsonXContent.jsonXContent, templateString));
        request.setRequest(searchRequest);
        SearchTemplateResponse searchResponse = client().execute(SearchTemplateAction.INSTANCE, request).get();
        assertThat(searchResponse.getResponse().getHits().getHits().length, equalTo(1));
    }

    public void testIndexedTemplateClient() throws Exception {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("testTemplate")
                .setContent(
                    new BytesArray(
                        "{"
                            + "  \"script\": {"
                            + "    \"lang\": \"mustache\","
                            + "    \"source\": {"
                            + "      \"query\": {"
                            + "        \"match\": {"
                            + "            \"theField\": \"{{fieldParam}}\""
                            + "        }"
                            + "      }"
                            + "    }"
                            + "  }"
                            + "}"
                    ),
                    XContentType.JSON
                )
        );

        GetStoredScriptResponse getResponse = client().admin().cluster().prepareGetStoredScript("testTemplate").get();
        assertNotNull(getResponse.getSource());

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("testTemplate")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 4);

        assertAcked(client().admin().cluster().prepareDeleteStoredScript("testTemplate"));

        getResponse = client().admin().cluster().prepareGetStoredScript("testTemplate").get();
        assertNull(getResponse.getSource());
    }

    public void testIndexedTemplate() throws Exception {

        String script = "{"
            + "  \"script\": {"
            + "    \"lang\": \"mustache\","
            + "    \"source\": {"
            + "      \"query\": {"
            + "        \"match\": {"
            + "            \"theField\": \"{{fieldParam}}\""
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";

        assertAcked(client().admin().cluster().preparePutStoredScript().setId("1a").setContent(new BytesArray(script), XContentType.JSON));
        assertAcked(client().admin().cluster().preparePutStoredScript().setId("2").setContent(new BytesArray(script), XContentType.JSON));
        assertAcked(client().admin().cluster().preparePutStoredScript().setId("3").setContent(new BytesArray(script), XContentType.JSON));

        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> templateParams = new HashMap<>();
        templateParams.put("fieldParam", "foo");

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
            .setScript("1a")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 4);

        expectThrows(
            ResourceNotFoundException.class,
            () -> new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest().indices("test"))
                .setScript("1000")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get()
        );

        templateParams.put("fieldParam", "bar");
        searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("2")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(templateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 1);
    }

    // Relates to #10397
    public void testIndexedTemplateOverwrite() throws Exception {
        createIndex("testindex");
        ensureGreen("testindex");

        client().prepareIndex("testindex").setId("1").setSource(jsonBuilder().startObject().field("searchtext", "dev1").endObject()).get();
        client().admin().indices().prepareRefresh().get();

        int iterations = randomIntBetween(2, 11);
        String query = "{"
            + "  \"script\": {"
            + "    \"lang\": \"mustache\","
            + "    \"source\": {"
            + "      \"query\": {"
            + "        \"match_phrase_prefix\": {"
            + "            \"searchtext\": {"
            + "                \"query\": \"{{P_Keyword1}}\","
            + "                \"slop\": {{slop}}"
            + "            }"
            + "        }"
            + "      }"
            + "    }"
            + "  }"
            + "}";
        for (int i = 1; i < iterations; i++) {
            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutStoredScript()
                    .setId("git01")
                    .setContent(new BytesArray(query.replace("{{slop}}", Integer.toString(-1))), XContentType.JSON)
            );

            GetStoredScriptResponse getResponse = client().admin().cluster().prepareGetStoredScript("git01").get();
            assertNotNull(getResponse.getSource());

            Map<String, Object> templateParams = new HashMap<>();
            templateParams.put("P_Keyword1", "dev");

            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("testindex"))
                    .setScript("git01")
                    .setScriptType(ScriptType.STORED)
                    .setScriptParams(templateParams)
                    .get()
            );
            assertThat(e.getMessage(), containsString("No negative slop allowed"));

            assertAcked(
                client().admin()
                    .cluster()
                    .preparePutStoredScript()
                    .setId("git01")
                    .setContent(new BytesArray(query.replace("{{slop}}", Integer.toString(0))), XContentType.JSON)
            );

            SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("testindex"))
                .setScript("git01")
                .setScriptType(ScriptType.STORED)
                .setScriptParams(templateParams)
                .get();
            assertHitCount(searchResponse.getResponse(), 1);
        }
    }

    public void testIndexedTemplateWithArray() throws Exception {
        String multiQuery = "{\n"
            + "  \"script\": {\n"
            + "    \"lang\": \"mustache\",\n"
            + "    \"source\": {\n"
            + "      \"query\": {\n"
            + "        \"terms\": {\n"
            + "            \"theField\": [\n"
            + "                \"{{#fieldParam}}\",\n"
            + "                \"{{.}}\",\n"
            + "                \"{{/fieldParam}}\"\n"
            + "            ]\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        assertAcked(
            client().admin().cluster().preparePutStoredScript().setId("4").setContent(new BytesArray(multiQuery), XContentType.JSON)
        );
        BulkRequestBuilder bulkRequestBuilder = client().prepareBulk();
        bulkRequestBuilder.add(client().prepareIndex("test").setId("1").setSource("{\"theField\":\"foo\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("2").setSource("{\"theField\":\"foo 2\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("3").setSource("{\"theField\":\"foo 3\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("4").setSource("{\"theField\":\"foo 4\"}", XContentType.JSON));
        bulkRequestBuilder.add(client().prepareIndex("test").setId("5").setSource("{\"theField\":\"bar\"}", XContentType.JSON));
        bulkRequestBuilder.get();
        client().admin().indices().prepareRefresh().get();

        Map<String, Object> arrayTemplateParams = new HashMap<>();
        String[] fieldParams = { "foo", "bar" };
        arrayTemplateParams.put("fieldParam", fieldParams);

        SearchTemplateResponse searchResponse = new SearchTemplateRequestBuilder(client()).setRequest(new SearchRequest("test"))
            .setScript("4")
            .setScriptType(ScriptType.STORED)
            .setScriptParams(arrayTemplateParams)
            .get();
        assertHitCount(searchResponse.getResponse(), 5);
    }

}
