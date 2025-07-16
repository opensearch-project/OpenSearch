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

package org.opensearch.explain;

import org.apache.lucene.search.Explanation;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.explain.ExplainResponse;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.InputStreamStreamInput;
import org.opensearch.core.common.io.stream.OutputStreamStreamOutput;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.opensearch.cluster.metadata.IndexMetadata.SETTING_NUMBER_OF_SHARDS;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;

public class ExplainActionIT extends OpenSearchIntegTestCase {
    public void testSimple() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field", "value1").get();

        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertFalse(response.isExists()); // not a match b/c not realtime
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertFalse(response.isMatch()); // not a match b/c not realtime

        refresh();
        response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));

        response = client().prepareExplain(indexOrAlias(), "1").setQuery(QueryBuilders.termQuery("field", "value2")).get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertNotNull(response.getExplanation());
        assertFalse(response.getExplanation().isMatch());

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(
                QueryBuilders.boolQuery().must(QueryBuilders.termQuery("field", "value1")).must(QueryBuilders.termQuery("field", "value2"))
            )
            .get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertNotNull(response.getExplanation());
        assertFalse(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getDetails().length, equalTo(2));

        response = client().prepareExplain(indexOrAlias(), "2").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertFalse(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("2"));
    }

    public void testExplainWithFields() throws Exception {
        assertAcked(
            prepareCreate("test").setMapping("obj1.field1", "type=keyword,store=true", "obj1.field2", "type=keyword,store=true")
                .addAlias(new Alias("alias"))
        );
        ensureGreen("test");

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()
            )
            .get();

        refresh();
        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        Set<String> fields = new HashSet<>(response.getGetResult().getFields().keySet());
        assertThat(fields, equalTo(singleton("obj1.field1")));
        assertThat(response.getGetResult().getFields().get("obj1.field1").getValue().toString(), equalTo("value1"));
        assertThat(response.getGetResult().isSourceEmpty(), equalTo(true));

        refresh();
        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1")
            .setFetchSource(true)
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        fields = new HashSet<>(response.getGetResult().getFields().keySet());
        assertThat(fields, equalTo(singleton("obj1.field1")));
        assertThat(response.getGetResult().getFields().get("obj1.field1").getValue().toString(), equalTo("value1"));
        assertThat(response.getGetResult().isSourceEmpty(), equalTo(false));

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setStoredFields("obj1.field1", "obj1.field2")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        String v1 = response.getGetResult().field("obj1.field1").getValue();
        String v2 = response.getGetResult().field("obj1.field2").getValue();
        assertThat(v1, equalTo("value1"));
        assertThat(v2, equalTo("value2"));
    }

    @SuppressWarnings("unchecked")
    public void testExplainWithSource() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")));
        ensureGreen("test");

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().startObject("obj1").field("field1", "value1").field("field2", "value2").endObject().endObject()
            )
            .get();

        refresh();
        ExplainResponse response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource("obj1.field1", null)
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertNotNull(response.getExplanation());
        assertTrue(response.getExplanation().isMatch());
        assertThat(response.getExplanation().getValue(), equalTo(1.0f));
        assertThat(response.getGetResult().isExists(), equalTo(true));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        assertThat(response.getGetResult().getSource().size(), equalTo(1));
        assertThat(((Map<String, Object>) response.getGetResult().getSource().get("obj1")).get("field1").toString(), equalTo("value1"));

        response = client().prepareExplain(indexOrAlias(), "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource(null, "obj1.field2")
            .get();
        assertNotNull(response);
        assertTrue(response.isMatch());
        assertThat(((Map<String, Object>) response.getGetResult().getSource().get("obj1")).get("field1").toString(), equalTo("value1"));
    }

    public void testExplainWithFilteredAlias() {
        assertAcked(
            prepareCreate("test").setMapping("field2", "type=text")
                .addAlias(new Alias("alias1").filter(QueryBuilders.termQuery("field2", "value2")))
        );
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").get();
        refresh();

        ExplainResponse response = client().prepareExplain("alias1", "1").setQuery(QueryBuilders.matchAllQuery()).get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
    }

    public void testExplainWithFilteredAliasFetchSource() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("test")
                .setMapping("field2", "type=text")
                .addAlias(new Alias("alias1").filter(QueryBuilders.termQuery("field2", "value2")))
        );
        ensureGreen("test");

        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value1").get();
        refresh();

        ExplainResponse response = client().prepareExplain("alias1", "1")
            .setQuery(QueryBuilders.matchAllQuery())
            .setFetchSource(true)
            .get();

        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getGetResult(), notNullValue());
        assertThat(response.getGetResult().getIndex(), equalTo("test"));
        assertThat(response.getGetResult().getId(), equalTo("1"));
        assertThat(response.getGetResult().getSource(), notNullValue());
        assertThat((String) response.getGetResult().getSource().get("field1"), equalTo("value1"));
    }

    public void testExplainDateRangeInQueryString() {
        createIndex("test");

        ZonedDateTime now = ZonedDateTime.now(ZoneOffset.UTC);
        String aMonthAgo = DateTimeFormatter.ISO_LOCAL_DATE.format(now.minusMonths(1));
        String aMonthFromNow = DateTimeFormatter.ISO_LOCAL_DATE.format(now.plusMonths(1));

        client().prepareIndex("test").setId("1").setSource("past", aMonthAgo, "future", aMonthFromNow).get();

        refresh();

        ExplainResponse explainResponse = client().prepareExplain("test", "1").setQuery(queryStringQuery("past:[now-2M/d TO now/d]")).get();
        assertThat(explainResponse.isExists(), equalTo(true));
        assertThat(explainResponse.isMatch(), equalTo(true));
    }

    private static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    public void testStreamExplain() throws Exception {
        Explanation exp = Explanation.match(2f, "some explanation");

        // write
        ByteArrayOutputStream outBuffer = new ByteArrayOutputStream();
        OutputStreamStreamOutput out = new OutputStreamStreamOutput(outBuffer);
        Lucene.writeExplanation(out, exp);

        // read
        ByteArrayInputStream esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        InputStreamStreamInput esBuffer = new InputStreamStreamInput(esInBuffer);

        Explanation result = Lucene.readExplanation(esBuffer);
        assertThat(exp.toString(), equalTo(result.toString()));

        exp = Explanation.match(2.0f, "some explanation", Explanation.match(2.0f, "another explanation"));

        // write complex
        outBuffer = new ByteArrayOutputStream();
        out = new OutputStreamStreamOutput(outBuffer);
        Lucene.writeExplanation(out, exp);

        // read complex
        esInBuffer = new ByteArrayInputStream(outBuffer.toByteArray());
        esBuffer = new InputStreamStreamInput(esInBuffer);

        result = Lucene.readExplanation(esBuffer);
        assertThat(exp.toString(), equalTo(result.toString()));
    }

    public void testQueryRewrite() {
        client().admin()
            .indices()
            .prepareCreate("twitter")
            .setMapping("user", "type=integer", "followers", "type=integer")
            .setSettings(Settings.builder().put(SETTING_NUMBER_OF_SHARDS, 2).put("index.number_of_routing_shards", 2))
            .get();
        client().prepareIndex("twitter").setId("1").setSource("followers", new int[] { 1, 2, 3 }).get();
        refresh();

        TermsQueryBuilder termsLookupQuery = QueryBuilders.termsLookupQuery("user", new TermsLookup("twitter", "1", "followers"));
        ExplainResponse response = client().prepareExplain("twitter", "1").setQuery(termsLookupQuery).get();
        assertNotNull(response);
        assertTrue(response.isExists());
        assertFalse(response.isMatch());
        assertThat(response.getIndex(), equalTo("twitter"));
        assertThat(response.getId(), equalTo("1"));
        assertNotNull(response.getExplanation());
        assertFalse(response.getExplanation().isMatch());
    }
}
