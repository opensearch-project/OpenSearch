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

package org.opensearch.search.sort;

import com.carrotsearch.randomizedtesting.annotations.ParametersFactory;

import org.opensearch.action.index.IndexRequestBuilder;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.ShardSearchFailure;
import org.opensearch.common.geo.GeoPoint;
import org.opensearch.common.geo.GeoUtils;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.fielddata.ScriptDocValues;
import org.opensearch.plugins.Plugin;
import org.opensearch.script.MockScriptPlugin;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.SearchHit;
import org.opensearch.search.sort.ScriptSortBuilder.ScriptSortType;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.ParameterizedStaticSettingsOpenSearchIntegTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.function.Function;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.matchAllQuery;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.search.SearchService.CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING;
import static org.opensearch.search.sort.SortBuilders.scriptSort;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertNoFailures;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;

public class SimpleSortIT extends ParameterizedStaticSettingsOpenSearchIntegTestCase {

    private static final String DOUBLE_APOSTROPHE = "\u0027\u0027";

    public SimpleSortIT(Settings staticSettings) {
        super(staticSettings);
    }

    @ParametersFactory
    public static Collection<Object[]> parameters() {
        return Arrays.asList(
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), false).build() },
            new Object[] { Settings.builder().put(CLUSTER_CONCURRENT_SEGMENT_SEARCH_SETTING.getKey(), true).build() }
        );
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(CustomScriptPlugin.class, InternalSettingsPlugin.class);
    }

    public static class CustomScriptPlugin extends MockScriptPlugin {

        @Override
        protected Map<String, Function<Map<String, Object>, Object>> pluginScripts() {
            Map<String, Function<Map<String, Object>, Object>> scripts = new HashMap<>();

            scripts.put("doc['str_value'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("str_value")).getValue();
            });

            scripts.put("doc['id'].value", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("id")).getValue();
            });

            scripts.put("doc['id'][0]", vars -> {
                Map<?, ?> doc = (Map) vars.get("doc");
                return ((ScriptDocValues.Strings) doc.get("id")).get(0);
            });

            scripts.put("get min long", vars -> getMinValueScript(vars, Long.MAX_VALUE, "lvalue", l -> (Long) l));
            scripts.put("get min double", vars -> getMinValueScript(vars, Double.MAX_VALUE, "dvalue", d -> (Double) d));
            scripts.put("get min string", vars -> getMinValueScript(vars, Integer.MAX_VALUE, "svalue", s -> Integer.parseInt((String) s)));
            scripts.put("get min geopoint lon", vars -> getMinValueScript(vars, Double.MAX_VALUE, "gvalue", g -> ((GeoPoint) g).getLon()));

            scripts.put(DOUBLE_APOSTROPHE, vars -> DOUBLE_APOSTROPHE);

            return scripts;
        }

        /**
         * Return the minimal value from a set of values.
         */
        static <T extends Comparable<T>> T getMinValueScript(
            Map<String, Object> vars,
            T initialValue,
            String fieldName,
            Function<Object, T> converter
        ) {
            T retval = initialValue;
            Map<?, ?> doc = (Map) vars.get("doc");
            ScriptDocValues<?> values = (ScriptDocValues<?>) doc.get(fieldName);
            for (Object v : values) {
                T value = converter.apply(v);
                retval = (value.compareTo(retval) < 0) ? value : retval;
            }
            return retval;
        }
    }

    public void testSimpleSorts() throws Exception {
        Random random = random();
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("str_value")
                    .field("type", "keyword")
                    .endObject()
                    .startObject("boolean_value")
                    .field("type", "boolean")
                    .endObject()
                    .startObject("byte_value")
                    .field("type", "byte")
                    .endObject()
                    .startObject("short_value")
                    .field("type", "short")
                    .endObject()
                    .startObject("integer_value")
                    .field("type", "integer")
                    .endObject()
                    .startObject("long_value")
                    .field("type", "long")
                    .endObject()
                    .startObject("float_value")
                    .field("type", "float")
                    .endObject()
                    .startObject("double_value")
                    .field("type", "double")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        List<IndexRequestBuilder> builders = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            builders.add(
                client().prepareIndex("test")
                    .setId(Integer.toString(i))
                    .setSource(
                        jsonBuilder().startObject()
                            .field("str_value", new String(new char[] { (char) (97 + i), (char) (97 + i) }))
                            .field("boolean_value", true)
                            .field("byte_value", i)
                            .field("short_value", i)
                            .field("integer_value", i)
                            .field("long_value", i)
                            .field("float_value", 0.1 * i)
                            .field("double_value", 0.1 * i)
                            .endObject()
                    )
            );
        }
        Collections.shuffle(builders, random);
        for (IndexRequestBuilder builder : builders) {
            builder.get();
            if (random.nextBoolean()) {
                if (random.nextInt(5) != 0) {
                    refresh();
                } else {
                    client().admin().indices().prepareFlush().get();
                }
            }
        }
        refresh();

        // STRING script
        int size = 1 + random.nextInt(10);

        Script script = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['str_value'].value", Collections.emptyMap());

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .setSize(size)
            .addSort(new ScriptSortBuilder(script, ScriptSortType.STRING))
            .get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.getId(), equalTo(Integer.toString(i)));

            String expected = new String(new char[] { (char) (97 + i), (char) (97 + i) });
            assertThat(searchHit.getSortValues()[0].toString(), equalTo(expected));
        }

        size = 1 + random.nextInt(10);
        searchResponse = client().prepareSearch().setQuery(matchAllQuery()).setSize(size).addSort("str_value", SortOrder.DESC).get();

        assertHitCount(searchResponse, 10);
        assertThat(searchResponse.getHits().getHits().length, equalTo(size));
        for (int i = 0; i < size; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat(searchHit.getId(), equalTo(Integer.toString(9 - i)));

            String expected = new String(new char[] { (char) (97 + (9 - i)), (char) (97 + (9 - i)) });
            assertThat(searchHit.getSortValues()[0].toString(), equalTo(expected));
        }

        assertThat(searchResponse.toString(), not(containsString("error")));
        assertNoFailures(searchResponse);
    }

    public void testSortMinValueScript() throws IOException {
        String mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("lvalue")
            .field("type", "long")
            .endObject()
            .startObject("dvalue")
            .field("type", "double")
            .endObject()
            .startObject("svalue")
            .field("type", "keyword")
            .endObject()
            .startObject("gvalue")
            .field("type", "geo_point")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen();

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test")
                .setId("" + i)
                .setSource(
                    jsonBuilder().startObject()
                        .field("ord", i)
                        .array("svalue", new String[] { "" + i, "" + (i + 1), "" + (i + 2) })
                        .array("lvalue", new long[] { i, i + 1, i + 2 })
                        .array("dvalue", new double[] { i, i + 1, i + 2 })
                        .startObject("gvalue")
                        .field("lat", (double) i + 1)
                        .field("lon", (double) i)
                        .endObject()
                        .endObject()
                )
                .get();
        }

        for (int i = 10; i < 20; i++) { // add some docs that don't have values in those fields
            client().prepareIndex("test").setId("" + i).setSource(jsonBuilder().startObject().field("ord", i).endObject()).get();
        }
        client().admin().indices().prepareRefresh("test").get();

        // test the long values
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("min", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get min long", Collections.emptyMap()))
            .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
            .setSize(10)
            .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").getValue(), equalTo((long) i));
        }

        // test the double values
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("min", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get min double", Collections.emptyMap()))
            .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
            .setSize(10)
            .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").getValue(), equalTo((double) i));
        }

        // test the string values
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("min", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get min string", Collections.emptyMap()))
            .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
            .setSize(10)
            .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").getValue(), equalTo(i));
        }

        // test the geopoint values
        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("min", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "get min geopoint lon", Collections.emptyMap()))
            .addSort(SortBuilders.fieldSort("ord").order(SortOrder.ASC).unmappedType("long"))
            .setSize(10)
            .get();

        assertNoFailures(searchResponse);

        assertHitCount(searchResponse, 20L);
        for (int i = 0; i < 10; i++) {
            SearchHit searchHit = searchResponse.getHits().getAt(i);
            assertThat("res: " + i + " id: " + searchHit.getId(), searchHit.field("min").getValue(), closeTo(i, GeoUtils.TOLERANCE));
        }
    }

    public void testDocumentsWithNullValue() throws Exception {
        // TODO: sort shouldn't fail when sort field is mapped dynamically
        // We have to specify mapping explicitly because by the time search is performed dynamic mapping might not
        // be propagated to all nodes yet and sort operation fail when the sort field is not defined
        String mapping = jsonBuilder().startObject()
            .startObject("properties")
            .startObject("id")
            .field("type", "keyword")
            .endObject()
            .startObject("svalue")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(prepareCreate("test").setMapping(mapping));
        ensureGreen();

        client().prepareIndex("test").setSource(jsonBuilder().startObject().field("id", "1").field("svalue", "aaa").endObject()).get();

        client().prepareIndex("test").setSource(jsonBuilder().startObject().field("id", "2").nullField("svalue").endObject()).get();

        client().prepareIndex("test").setSource(jsonBuilder().startObject().field("id", "3").field("svalue", "bbb").endObject()).get();

        flush();
        refresh();

        Script scripField = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['id'].value", Collections.emptyMap());

        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("id", scripField)
            .addSort("svalue", SortOrder.ASC)
            .get();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").getValue(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").getValue(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").getValue(), equalTo("2"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("id", new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "doc['id'][0]", Collections.emptyMap()))
            .addSort("svalue", SortOrder.ASC)
            .get();

        assertNoFailures(searchResponse);

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").getValue(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(1).field("id").getValue(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(2).field("id").getValue(), equalTo("2"));

        searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addScriptField("id", scripField)
            .addSort("svalue", SortOrder.DESC)
            .get();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(3L));
        assertThat(searchResponse.getHits().getAt(0).field("id").getValue(), equalTo("3"));
        assertThat(searchResponse.getHits().getAt(1).field("id").getValue(), equalTo("1"));
        assertThat(searchResponse.getHits().getAt(2).field("id").getValue(), equalTo("2"));

        // a query with docs just with null values
        searchResponse = client().prepareSearch()
            .setQuery(termQuery("id", "2"))
            .addScriptField("id", scripField)
            .addSort("svalue", SortOrder.DESC)
            .get();

        if (searchResponse.getFailedShards() > 0) {
            logger.warn("Failed shards:");
            for (ShardSearchFailure shardSearchFailure : searchResponse.getShardFailures()) {
                logger.warn("-> {}", shardSearchFailure);
            }
        }
        assertThat(searchResponse.getFailedShards(), equalTo(0));

        assertThat(searchResponse.getHits().getTotalHits().value, equalTo(1L));
        assertThat(searchResponse.getHits().getAt(0).field("id").getValue(), equalTo("2"));
    }

    public void test2920() throws IOException {
        assertAcked(
            prepareCreate("test").setMapping(
                jsonBuilder().startObject()
                    .startObject("properties")
                    .startObject("value")
                    .field("type", "keyword")
                    .endObject()
                    .endObject()
                    .endObject()
            )
        );
        ensureGreen();
        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test")
                .setId(Integer.toString(i))
                .setSource(jsonBuilder().startObject().field("value", "" + i).endObject())
                .get();
        }
        refresh();

        Script sortScript = new Script(ScriptType.INLINE, CustomScriptPlugin.NAME, "\u0027\u0027", Collections.emptyMap());
        SearchResponse searchResponse = client().prepareSearch()
            .setQuery(matchAllQuery())
            .addSort(scriptSort(sortScript, ScriptSortType.STRING))
            .setSize(10)
            .get();
        assertNoFailures(searchResponse);
    }
}
