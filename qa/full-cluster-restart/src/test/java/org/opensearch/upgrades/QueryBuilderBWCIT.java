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

package org.opensearch.upgrades;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.InputStreamStreamInput;
import org.opensearch.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.common.io.stream.NamedWriteableRegistry;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.Fuzziness;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.DisMaxQueryBuilder;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchPhraseQueryBuilder;
import org.opensearch.index.query.MatchQueryBuilder;
import org.opensearch.index.query.Operator;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.index.query.SpanNearQueryBuilder;
import org.opensearch.index.query.SpanTermQueryBuilder;
import org.opensearch.index.query.functionscore.FunctionScoreQueryBuilder;
import org.opensearch.index.query.functionscore.RandomScoreFunctionBuilder;
import org.opensearch.search.SearchModule;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;

/**
 * An integration test that tests whether percolator queries stored in older supported ES version can still be read by the
 * current ES version. Percolator queries are stored in the binary format in a dedicated doc values field (see
 * PercolatorFieldMapper#createQueryBuilderField(...) method). Using the query builders writable contract. This test
 * does best effort verifying that we don't break bwc for query builders between the first previous major version and
 * the latest current major release.
 *
 * The queries to test are specified in json format, which turns out to work because we tend break here rarely. If the
 * json format of a query being tested here then feel free to change this.
 */
public class QueryBuilderBWCIT extends AbstractFullClusterRestartTestCase {

    private static final List<Object[]> CANDIDATES = new ArrayList<>();

    static {
        addCandidate("\"match\": { \"keyword_field\": \"value\"}", new MatchQueryBuilder("keyword_field", "value"));
        addCandidate(
            "\"match\": { \"keyword_field\": {\"query\": \"value\", \"operator\": \"and\"} }",
            new MatchQueryBuilder("keyword_field", "value").operator(Operator.AND)
        );
        addCandidate(
            "\"match\": { \"keyword_field\": {\"query\": \"value\", \"analyzer\": \"english\"} }",
            new MatchQueryBuilder("keyword_field", "value").analyzer("english")
        );
        addCandidate(
            "\"match\": { \"keyword_field\": {\"query\": \"value\", \"minimum_should_match\": 3} }",
            new MatchQueryBuilder("keyword_field", "value").minimumShouldMatch("3")
        );
        addCandidate(
            "\"match\": { \"keyword_field\": {\"query\": \"value\", \"fuzziness\": \"auto\"} }",
            new MatchQueryBuilder("keyword_field", "value").fuzziness(Fuzziness.AUTO)
        );
        addCandidate("\"match_phrase\": { \"keyword_field\": \"value\"}", new MatchPhraseQueryBuilder("keyword_field", "value"));
        addCandidate(
            "\"match_phrase\": { \"keyword_field\": {\"query\": \"value\", \"slop\": 3}}",
            new MatchPhraseQueryBuilder("keyword_field", "value").slop(3)
        );
        addCandidate("\"range\": { \"long_field\": {\"gte\": 1, \"lte\": 9}}", new RangeQueryBuilder("long_field").from(1).to(9));
        addCandidate(
            "\"bool\": { \"must_not\": [{\"match_all\": {}}], \"must\": [{\"match_all\": {}}], " +
                "\"filter\": [{\"match_all\": {}}], \"should\": [{\"match_all\": {}}]}",
            new BoolQueryBuilder().mustNot(new MatchAllQueryBuilder()).must(new MatchAllQueryBuilder())
                .filter(new MatchAllQueryBuilder()).should(new MatchAllQueryBuilder())
        );
        addCandidate(
            "\"dis_max\": {\"queries\": [{\"match_all\": {}},{\"match_all\": {}},{\"match_all\": {}}], \"tie_breaker\": 0.01}",
            new DisMaxQueryBuilder().add(new MatchAllQueryBuilder()).add(new MatchAllQueryBuilder()).add(new MatchAllQueryBuilder())
                .tieBreaker(0.01f)
        );
        addCandidate(
            "\"constant_score\": {\"filter\": {\"match_all\": {}}, \"boost\": 0.1}",
            new ConstantScoreQueryBuilder(new MatchAllQueryBuilder()).boost(0.1f)
        );
        addCandidate(
            "\"function_score\": {\"query\": {\"match_all\": {}}," +
                "\"functions\": [{\"random_score\": {}, \"filter\": {\"match_all\": {}}, \"weight\": 0.2}]}",
            new FunctionScoreQueryBuilder(new MatchAllQueryBuilder(), new FunctionScoreQueryBuilder.FilterFunctionBuilder[]{
                new FunctionScoreQueryBuilder.FilterFunctionBuilder(new MatchAllQueryBuilder(),
                    new RandomScoreFunctionBuilder().setWeight(0.2f))})
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, " +
                "{ \"span_term\": { \"keyword_field\": \"value2\" }}]}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 0)
                .addClause(new SpanTermQueryBuilder("keyword_field", "value2"))
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, " +
                "{ \"span_term\": { \"keyword_field\": \"value2\" }}], \"slop\": 2}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 2)
                .addClause(new SpanTermQueryBuilder("keyword_field", "value2"))
        );
        addCandidate(
            "\"span_near\": {\"clauses\": [{ \"span_term\": { \"keyword_field\": \"value1\" }}, " +
                "{ \"span_term\": { \"keyword_field\": \"value2\" }}], \"slop\": 2, \"in_order\": false}",
            new SpanNearQueryBuilder(new SpanTermQueryBuilder("keyword_field", "value1"), 2)
                .addClause(new SpanTermQueryBuilder("keyword_field", "value2")).inOrder(false)
        );
    }

    private static void addCandidate(String querySource, QueryBuilder expectedQb) {
        CANDIDATES.add(new Object[]{"{\"query\": {" + querySource + "}}", expectedQb});
    }

    public void testQueryBuilderBWC() throws Exception {
        final String type = MapperService.SINGLE_MAPPING_NAME;
        String index = "queries";
        if (isRunningAgainstOldCluster()) {
            XContentBuilder mappingsAndSettings = jsonBuilder();
            mappingsAndSettings.startObject();
            {
                mappingsAndSettings.startObject("settings");
                mappingsAndSettings.field("number_of_shards", 1);
                mappingsAndSettings.field("number_of_replicas", 0);
                mappingsAndSettings.endObject();
            }
            {
                mappingsAndSettings.startObject("mappings");
                if (isRunningAgainstAncientCluster()) {
                    mappingsAndSettings.startObject(type);
                }
                mappingsAndSettings.startObject("properties");
                {
                    mappingsAndSettings.startObject("query");
                    mappingsAndSettings.field("type", "percolator");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("keyword_field");
                    mappingsAndSettings.field("type", "keyword");
                    mappingsAndSettings.endObject();
                }
                {
                    mappingsAndSettings.startObject("long_field");
                    mappingsAndSettings.field("type", "long");
                    mappingsAndSettings.endObject();
                }
                mappingsAndSettings.endObject();
                mappingsAndSettings.endObject();
                if (isRunningAgainstAncientCluster()) {
                    mappingsAndSettings.endObject();
                }
            }
            mappingsAndSettings.endObject();
            Request request = new Request("PUT", "/" + index);
            request.setOptions(allowTypesRemovalWarnings());
            request.setJsonEntity(Strings.toString(mappingsAndSettings));
            Response rsp = client().performRequest(request);
            assertEquals(200, rsp.getStatusLine().getStatusCode());

            for (int i = 0; i < CANDIDATES.size(); i++) {
                request = new Request("PUT", "/" + index + "/" + type + "/" + Integer.toString(i));
                request.setJsonEntity((String) CANDIDATES.get(i)[0]);
                rsp = client().performRequest(request);
                assertEquals(201, rsp.getStatusLine().getStatusCode());
            }
        } else {
            NamedWriteableRegistry registry = new NamedWriteableRegistry(new SearchModule(Settings.EMPTY,
                Collections.emptyList()).getNamedWriteables());

            for (int i = 0; i < CANDIDATES.size(); i++) {
                QueryBuilder expectedQueryBuilder = (QueryBuilder) CANDIDATES.get(i)[1];
                Request request = new Request("GET", "/" + index + "/_search");
                request.setJsonEntity("{\"query\": {\"ids\": {\"values\": [\"" + Integer.toString(i) + "\"]}}, " +
                        "\"docvalue_fields\": [{\"field\":\"query.query_builder_field\"}]}");
                Response rsp = client().performRequest(request);
                assertEquals(200, rsp.getStatusLine().getStatusCode());
                Map<?, ?> hitRsp = (Map<?, ?>) ((List<?>) ((Map<?, ?>)toMap(rsp).get("hits")).get("hits")).get(0);
                String queryBuilderStr = (String) ((List<?>) ((Map<?, ?>) hitRsp.get("fields")).get("query.query_builder_field")).get(0);
                byte[] qbSource = Base64.getDecoder().decode(queryBuilderStr);
                try (InputStream in = new ByteArrayInputStream(qbSource, 0, qbSource.length)) {
                    try (StreamInput input = new NamedWriteableAwareStreamInput(new InputStreamStreamInput(in), registry)) {
                        input.setVersion(getOldClusterVersion());
                        QueryBuilder queryBuilder = input.readNamedWriteable(QueryBuilder.class);
                        assert in.read() == -1;
                        assertEquals(expectedQueryBuilder, queryBuilder);
                    }
                }
            }
        }
    }

    private static Map<String, Object> toMap(Response response) throws IOException, ParseException {
        return toMap(EntityUtils.toString(response.getEntity()));
    }

    private static Map<String, Object> toMap(String response) throws IOException {
        return XContentHelper.convertToMap(JsonXContent.jsonXContent, response, false);
    }
}
