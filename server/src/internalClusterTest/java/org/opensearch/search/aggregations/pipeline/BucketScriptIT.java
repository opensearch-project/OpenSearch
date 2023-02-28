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

package org.opensearch.search.aggregations.pipeline;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.bytes.BytesArray;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.bucket.histogram.Histogram;
import org.opensearch.search.aggregations.bucket.range.Range;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.search.aggregations.pipeline.BucketHelpers.GapPolicy;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.aggregations.AggregationBuilders.dateRange;
import static org.opensearch.search.aggregations.AggregationBuilders.histogram;
import static org.opensearch.search.aggregations.AggregationBuilders.sum;
import static org.opensearch.search.aggregations.PipelineAggregatorBuilders.bucketScript;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class BucketScriptIT extends OpenSearchIntegTestCase {

    private static final String FIELD_1_NAME = "field1";
    private static final String FIELD_2_NAME = "field2";
    private static final String FIELD_3_NAME = "field3";
    private static final String FIELD_4_NAME = "field4";
    private static final String FIELD_5_NAME = "field5";

    private static int interval;
    private static int numDocs;
    private static int minNumber;
    private static int maxNumber;
    private static long date;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("_value0 + _value1 + _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            scripts.put("_value0 + _value1 / _value2", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 / value2;
            });

            scripts.put("_value0", vars -> vars.get("_value0"));

            scripts.put("foo + bar + baz", vars -> {
                double foo = (double) vars.get("foo");
                double bar = (double) vars.get("bar");
                double baz = (double) vars.get("baz");
                return foo + bar + baz;
            });

            scripts.put("(_value0 + _value1 + _value2) * factor", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return (value0 + value1 + value2) * (int) vars.get("factor");
            });

            scripts.put("my_script", vars -> {
                double value0 = (double) vars.get("_value0");
                double value1 = (double) vars.get("_value1");
                double value2 = (double) vars.get("_value2");
                return value0 + value1 + value2;
            });

            scripts.put("single_input", vars -> {
                double value = (double) vars.get("_value");
                return value;
            });

            scripts.put("return null", vars -> null);

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        createIndex("idx");
        createIndex("idx_unmapped");

        interval = randomIntBetween(1, 50);
        numDocs = randomIntBetween(10, 500);
        minNumber = -200;
        maxNumber = 200;
        date = randomLong();

        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int docs = 0; docs < numDocs; docs++) {
            builders.add(client().prepareIndex("idx").setSource(newDocBuilder()));
        }

        indexRandom(true, builders);
        ensureSearchable();
    }

    private XContentBuilder newDocBuilder() throws IOException {
        XContentBuilder jsonBuilder = jsonBuilder();
        jsonBuilder.startObject();
        jsonBuilder.field(FIELD_1_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_2_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_3_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_4_NAME, randomIntBetween(minNumber, maxNumber));
        jsonBuilder.field(FIELD_5_NAME, date);
        jsonBuilder.endObject();
        return jsonBuilder;
    }

    public void testInlineScript() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScript2() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 / _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue / field4SumValue));
            }
        }
    }

    public void testInlineScriptWithDateRange() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                dateRange("range").field(FIELD_5_NAME)
                    .addUnboundedFrom(date)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Range range = response.getAggregations().get("range");
        assertThat(range, notNullValue());
        assertThat(range.getName(), equalTo("range"));
        List<? extends Range.Bucket> buckets = range.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Range.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScriptSingleVariable() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0", Collections.emptyMap()),
                            "field2Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue));
            }
        }
    }

    public void testInlineScriptNamedVars() {
        Map<String, String> bucketsPathsMap = new HashMap<>();
        bucketsPathsMap.put("foo", "field2Sum");
        bucketsPathsMap.put("bar", "field3Sum");
        bucketsPathsMap.put("baz", "field4Sum");
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            bucketsPathsMap,
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "foo + bar + baz", Collections.emptyMap())
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScriptWithParams() {
        Map<String, Object> params = new HashMap<>();
        params.put("factor", 3);

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "(_value0 + _value1 + _value2) * factor", params);

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScript("seriesArithmetic", script, "field2Sum", "field3Sum", "field4Sum"))
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo((field2SumValue + field3SumValue + field4SumValue) * 3));
            }
        }
    }

    public void testInlineScriptInsertZeros() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        ).gapPolicy(GapPolicy.INSERT_ZEROS)
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(0.0));
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testInlineScriptReturnNull() {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(
                        bucketScript(
                            "nullField",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "return null", Collections.emptyMap())
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            assertNull(bucket.getAggregations().get("nullField"));
        }
    }

    public void testStoredScript() {
        assertAcked(
            client().admin()
                .cluster()
                .preparePutStoredScript()
                .setId("my_script")
                // Script source is not interpreted but it references a pre-defined script from CustomScriptPlugin
                .setContent(
                    new BytesArray("{ \"script\": {\"lang\": \"" + CustomScriptPlugin.NAME + "\"," + " \"source\": \"my_script\" } }"),
                    XContentType.JSON
                )
        );

        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.STORED, null, "my_script", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram deriv = response.getAggregations().get("histo");
        assertThat(deriv, notNullValue());
        assertThat(deriv.getName(), equalTo("histo"));
        assertThat(deriv.getBuckets().size(), equalTo(0));
    }

    public void testPartiallyUnmapped() throws Exception {
        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(
                        bucketScript(
                            "seriesArithmetic",
                            new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "_value0 + _value1 + _value2", Collections.emptyMap()),
                            "field2Sum",
                            "field3Sum",
                            "field4Sum"
                        )
                    )
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testSingleBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .field("buckets_path", "field2Sum")
            .startObject("script")
            .field("source", "single_input")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "seriesArithmetic"
        );

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(bucketScriptAgg)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue));
            }
        }
    }

    public void testArrayBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .array("buckets_path", "field2Sum", "field3Sum", "field4Sum")
            .startObject("script")
            .field("source", "_value0 + _value1 + _value2")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "seriesArithmetic"
        );

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScriptAgg)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }

    public void testObjectBucketPathAgg() throws Exception {
        XContentBuilder content = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("buckets_path")
            .field("_value0", "field2Sum")
            .field("_value1", "field3Sum")
            .field("_value2", "field4Sum")
            .endObject()
            .startObject("script")
            .field("source", "_value0 + _value1 + _value2")
            .field("lang", CustomScriptPlugin.NAME)
            .endObject()
            .endObject();
        BucketScriptPipelineAggregationBuilder bucketScriptAgg = BucketScriptPipelineAggregationBuilder.PARSER.parse(
            createParser(content),
            "seriesArithmetic"
        );

        SearchResponse response = client().prepareSearch("idx", "idx_unmapped")
            .addAggregation(
                histogram("histo").field(FIELD_1_NAME)
                    .interval(interval)
                    .subAggregation(sum("field2Sum").field(FIELD_2_NAME))
                    .subAggregation(sum("field3Sum").field(FIELD_3_NAME))
                    .subAggregation(sum("field4Sum").field(FIELD_4_NAME))
                    .subAggregation(bucketScriptAgg)
            )
            .get();

        assertSearchResponse(response);

        Histogram histo = response.getAggregations().get("histo");
        assertThat(histo, notNullValue());
        assertThat(histo.getName(), equalTo("histo"));
        List<? extends Histogram.Bucket> buckets = histo.getBuckets();

        for (int i = 0; i < buckets.size(); ++i) {
            Histogram.Bucket bucket = buckets.get(i);
            if (bucket.getDocCount() == 0) {
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, nullValue());
            } else {
                Sum field2Sum = bucket.getAggregations().get("field2Sum");
                assertThat(field2Sum, notNullValue());
                double field2SumValue = field2Sum.getValue();
                Sum field3Sum = bucket.getAggregations().get("field3Sum");
                assertThat(field3Sum, notNullValue());
                double field3SumValue = field3Sum.getValue();
                Sum field4Sum = bucket.getAggregations().get("field4Sum");
                assertThat(field4Sum, notNullValue());
                double field4SumValue = field4Sum.getValue();
                SimpleValue seriesArithmetic = bucket.getAggregations().get("seriesArithmetic");
                assertThat(seriesArithmetic, notNullValue());
                double seriesArithmeticValue = seriesArithmetic.value();
                assertThat(seriesArithmeticValue, equalTo(field2SumValue + field3SumValue + field4SumValue));
            }
        }
    }
}
