/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.terms;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationTestScriptsPlugin;
import org.opensearch.search.aggregations.bucket.AbstractTermsTestCase;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

@OpenSearchIntegTestCase.SuiteScopeTestCase
public class BaseStringTermsTestCase extends AbstractTermsTestCase {

    protected static final String SINGLE_VALUED_FIELD_NAME = "s_value";
    protected static final String MULTI_VALUED_FIELD_NAME = "s_values";
    protected static Map<String, Map<String, Object>> expectedMultiSortBuckets;

    public BaseStringTermsTestCase(Settings staticSettings) {
        super(staticSettings);
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(CustomScriptPlugin.class);
    }

    @Before
    public void randomizeOptimizations() {
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = randomBoolean();
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = randomBoolean();
    }

    @After
    public void resetOptimizations() {
        TermsAggregatorFactory.COLLECT_SEGMENT_ORDS = null;
        TermsAggregatorFactory.REMAP_GLOBAL_ORDS = null;
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

            scripts.put("Math.random()", vars -> randomDouble());

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
        expectedMultiSortBuckets = new HashMap<>();
        Map<String, Object> bucketProps = new HashMap<>();
        bucketProps.put("_term", "val1");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 1d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val2");
        bucketProps.put("_count", 3L);
        bucketProps.put("avg_l", 2d);
        bucketProps.put("sum_d", 6d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val3");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val4");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 3d);
        bucketProps.put("sum_d", 4d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val5");
        bucketProps.put("_count", 2L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 3d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val6");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);
        bucketProps = new HashMap<>();
        bucketProps.put("_term", "val7");
        bucketProps.put("_count", 1L);
        bucketProps.put("avg_l", 5d);
        bucketProps.put("sum_d", 1d);
        expectedMultiSortBuckets.put((String) bucketProps.get("_term"), bucketProps);

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

    protected String key(Terms.Bucket bucket) {
        return bucket.getKeyAsString();
    }
}
