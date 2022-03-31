/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.Strings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.aggregations.AggregationTestScriptsPlugin;
import org.opensearch.search.aggregations.bucket.terms.StringTermsIT;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.support.MultiTermsValuesSourceConfig;
import org.opensearch.search.aggregations.support.ValueType;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.IsNull.notNullValue;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.search.aggregations.AggregationBuilders.multiTerms;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertSearchResponse;

/**
 * Copy from StringTermsIT.
 */
@OpenSearchIntegTestCase.SuiteScopeTestCase
public class MultiTermsIT extends AbstractTermsTestCase {
    private static final String SINGLE_VALUED_FIELD_NAME = "s_value";
    private static final String MULTI_VALUED_FIELD_NAME = "s_values";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(StringTermsIT.CustomScriptPlugin.class);
    }

    public static class CustomScriptPlugin extends AggregationTestScriptsPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = super.pluginScripts();

            scripts.put("'foo_' + _value", vars -> "foo_" + (String) vars.get("_value"));
            scripts.put("_value.substring(0,3)", vars -> ((String) vars.get("_value")).substring(0, 3));

            scripts.put("doc['" + MULTI_VALUED_FIELD_NAME + "']", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return doc.get(MULTI_VALUED_FIELD_NAME);
            });

            scripts.put("doc['" + SINGLE_VALUED_FIELD_NAME + "'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                ScriptDocValues.Strings value = (ScriptDocValues.Strings) doc.get(SINGLE_VALUED_FIELD_NAME);
                return value.getValue();
            });

            scripts.put("42", vars -> 42);

            return scripts;
        }

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> nonDeterministicPluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("Math.random()", vars -> StringTermsIT.randomDouble());

            return scripts;
        }
    }

    @Override
    public void setupSuiteScopeCluster() throws Exception {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
                .get()
        );
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            builders.add(
                client().prepareIndex("idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, "val" + i)
                            .field("i", i)
                            .field("constant", 1)
                            .field("tag", i < 5 / 2 + 1 ? "more" : "less")
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .value("val" + i)
                            .value("val" + (i + 1))
                            .endArray()
                            .endObject()
                    )
            );
        }

        getMultiSortDocs(builders);

        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("high_card_idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
                .get()
        );
        for (int i = 0; i < 100; i++) {
            builders.add(
                client().prepareIndex("high_card_idx")
                    .setSource(
                        jsonBuilder().startObject()
                            .field(SINGLE_VALUED_FIELD_NAME, "val" + Strings.padStart(i + "", 3, '0'))
                            .startArray(MULTI_VALUED_FIELD_NAME)
                            .value("val" + Strings.padStart(i + "", 3, '0'))
                            .value("val" + Strings.padStart((i + 1) + "", 3, '0'))
                            .endArray()
                            .endObject()
                    )
            );
        }
        prepareCreate("empty_bucket_idx").setMapping(SINGLE_VALUED_FIELD_NAME, "type=integer").get();

        for (int i = 0; i < 2; i++) {
            builders.add(
                client().prepareIndex("empty_bucket_idx")
                    .setId("" + i)
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, i * 2).endObject())
            );
        }
        indexRandom(true, builders);
        createIndex("idx_unmapped");
        ensureSearchable();
    }

    private void getMultiSortDocs(List<IndexRequestBuilder> builders) throws IOException {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("sort_idx")
                .setMapping(SINGLE_VALUED_FIELD_NAME, "type=keyword", MULTI_VALUED_FIELD_NAME, "type=keyword", "tag", "type=keyword")
                .get()
        );
        for (int i = 1; i <= 3; i++) {
            builders.add(
                client().prepareIndex("sort_idx")
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val1").field("l", 1).field("d", i).endObject())
            );
            builders.add(
                client().prepareIndex("sort_idx")
                    .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val2").field("l", 2).field("d", i).endObject())
            );
        }
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val3").field("l", 3).field("d", 2).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val4").field("l", 3).field("d", 3).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val5").field("l", 5).field("d", 2).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val6").field("l", 5).field("d", 1).endObject())
        );
        builders.add(
            client().prepareIndex("sort_idx")
                .setSource(jsonBuilder().startObject().field(SINGLE_VALUED_FIELD_NAME, "val7").field("l", 5).field("d", 1).endObject())
        );
    }

    private String key(Terms.Bucket bucket) {
        return bucket.getKeyAsString();
    }

    // the main purpose of this test is to make sure we're not allocating 2GB of memory per shard
    public void testSizeIsZero() {
        final int minDocCount = randomInt(1);
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareSearch("high_card_idx")
                .addAggregation(
                    multiTerms("mterms").terms(
                        asList(
                            new MultiTermsValuesSourceConfig.Builder().setFieldName(SINGLE_VALUED_FIELD_NAME).build(),
                            new MultiTermsValuesSourceConfig.Builder().setFieldName(MULTI_VALUED_FIELD_NAME).build()
                        )
                    ).minDocCount(minDocCount).size(0)
                )
                .get()
        );
        assertThat(exception.getMessage(), containsString("[size] must be greater than 0. Found [0] in [mterms]"));
    }

    public void testSingleValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("i").build(),
                        new MultiTermsValuesSourceConfig.Builder().setFieldName(SINGLE_VALUED_FIELD_NAME)
                            .setScript(
                                new Script(
                                    ScriptType.INLINE,
                                    StringTermsIT.CustomScriptPlugin.NAME,
                                    "'foo_' + _value",
                                    Collections.emptyMap()
                                )
                            )
                            .build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey(i + "|foo_val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i + "|foo_val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testSingleValuedFieldWithScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("i").build(),
                        new MultiTermsValuesSourceConfig.Builder().setScript(
                            new Script(
                                ScriptType.INLINE,
                                StringTermsIT.CustomScriptPlugin.NAME,
                                "doc['" + SINGLE_VALUED_FIELD_NAME + "'].value",
                                Collections.emptyMap()
                            )
                        ).setUserValueTypeHint(ValueType.STRING).build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(5));

        for (int i = 0; i < 5; i++) {
            Terms.Bucket bucket = terms.getBucketByKey(i + "|val" + i);
            assertThat(bucket, notNullValue());
            assertThat(key(bucket), equalTo(i + "|val" + i));
            assertThat(bucket.getDocCount(), equalTo(1L));
        }
    }

    public void testMultiValuedFieldWithValueScript() throws Exception {
        SearchResponse response = client().prepareSearch("idx")
            .addAggregation(
                multiTerms("mterms").terms(
                    asList(
                        new MultiTermsValuesSourceConfig.Builder().setFieldName("tag").build(),
                        new MultiTermsValuesSourceConfig.Builder().setFieldName(MULTI_VALUED_FIELD_NAME)
                            .setScript(
                                new Script(
                                    ScriptType.INLINE,
                                    StringTermsIT.CustomScriptPlugin.NAME,
                                    "_value.substring(0,3)",
                                    Collections.emptyMap()
                                )
                            )
                            .build()
                    )
                )
            )
            .get();

        assertSearchResponse(response);

        Terms terms = response.getAggregations().get("mterms");
        assertThat(terms, notNullValue());
        assertThat(terms.getName(), equalTo("mterms"));
        assertThat(terms.getBuckets().size(), equalTo(2));

        Terms.Bucket bucket = terms.getBucketByKey("more|val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("more|val"));
        assertThat(bucket.getDocCount(), equalTo(3L));

        bucket = terms.getBucketByKey("less|val");
        assertThat(bucket, notNullValue());
        assertThat(key(bucket), equalTo("less|val"));
        assertThat(bucket.getDocCount(), equalTo(2L));
    }

    private MultiTermsValuesSourceConfig field(String name) {
        return new MultiTermsValuesSourceConfig.Builder().setFieldName(name).build();
    }
}
