/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.bulk.BulkRequest;
import org.opensearch.action.bulk.BulkResponse;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.index.IndexRequest;
import org.opensearch.action.update.UpdateRequest;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.index.mapper.extrasource.BytesValue;
import org.opensearch.index.mapper.extrasource.ExtraFieldValues;
import org.opensearch.index.mapper.extrasource.ExtraFieldValuesMapperPlugin;
import org.opensearch.index.mapper.extrasource.PrimitiveFloatArray;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.Collections;
import java.util.Map;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.anyOf;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class ExtraFieldValuesRequestIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singletonList(ExtraFieldValuesMapperPlugin.class);
    }

    private static void assertTestMapping(OpenSearchIntegTestCase testCase, String index, Settings settings) throws Exception {
        assertAcked(
            testCase.prepareCreate(index)
                .setSettings(settings)
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")

                        .startObject("field")
                        .field("type", ExtraFieldValuesMapperPlugin.EXTRA_FIELDS_TEST)
                        .endObject()

                        .startObject("field_type")
                        .field("type", "keyword")
                        .field("store", true)
                        .endObject()

                        .startObject("field_len")
                        .field("type", "integer")
                        .field("store", true)
                        .endObject()

                        .startObject("field_dim")
                        .field("type", "integer")
                        .field("store", true)
                        .endObject()

                        .startObject("field_f0")
                        .field("type", "float")
                        .field("store", true)
                        .endObject()

                        .endObject()
                        .endObject()
                )
                .get()
        );
    }

    public void testIndexRequestExtraFieldValues() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        String index = "test";
        assertTestMapping(this, index, settings);

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("field", new BytesValue(new BytesArray(new byte[] { 1, 2, 3, 4 }))));

        IndexRequest req = new IndexRequest(index).id("1").source("{\"other\":\"x\"}", XContentType.JSON).extraFieldValues(efv);

        DocWriteResponse resp = client().index(req).actionGet();
        assertThat(resp.getResult(), anyOf(is(DocWriteResponse.Result.CREATED), is(DocWriteResponse.Result.UPDATED)));
        refresh(index);

        GetResponse get = client().prepareGet(index, "1").setStoredFields("field_type", "field_len").get();

        assertThat(get.isExists(), is(true));
        assertThat(get.getFields().keySet(), hasItems("field_type", "field_len"));
        assertThat(get.getField("field_type").getValue().toString(), is("BYTES"));
        assertThat(((Number) get.getField("field_len").getValue()).intValue(), is(4));
    }

    public void testUpdateRequestDocExtraFieldValues() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        String index = "test2";
        assertTestMapping(this, index, settings);

        // create doc without "field"
        client().prepareIndex(index).setId("1").setSource("{\"other\":\"x\"}", XContentType.JSON).get();
        refresh(index);

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("field", new PrimitiveFloatArray(new float[] { 10.5f, 20.25f })));

        UpdateRequest ur = new UpdateRequest(index, "1").doc("{\"other\":\"y\"}", XContentType.JSON).docExtraFieldValues(efv);

        client().update(ur).actionGet();
        refresh(index);

        GetResponse get = client().prepareGet(index, "1").setStoredFields("field_type", "field_dim", "field_f0").get();

        assertThat(get.isExists(), is(true));
        assertThat(get.getFields().keySet(), hasItems("field_type", "field_dim", "field_f0"));
        assertThat(get.getField("field_type").getValue().toString(), is("FLOAT_ARRAY"));
        assertThat(((Number) get.getField("field_dim").getValue()).intValue(), is(2));
        assertEquals(10.5f, ((Number) get.getField("field_f0").getValue()).floatValue(), 0.0f);
    }

    public void testUpdateRequestUpsertExtraFieldValues() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        String index = "test3";
        assertTestMapping(this, index, settings);

        ExtraFieldValues efv = new ExtraFieldValues(Map.of("field", new BytesValue(new BytesArray(new byte[] { 9 }))));

        UpdateRequest ur = new UpdateRequest(index, "1").doc("{\"other\":\"x\"}", XContentType.JSON)
            .upsert("{\"other\":\"x\"}", XContentType.JSON)
            .upsertExtraFieldValues(efv);

        client().update(ur).actionGet();
        refresh(index);

        GetResponse get = client().prepareGet(index, "1").setStoredFields("field_type", "field_len").get();

        assertThat(get.isExists(), is(true));
        assertThat(get.getFields().keySet(), hasItems("field_type", "field_len"));
        assertThat(get.getField("field_type").getValue().toString(), is("BYTES"));
        assertThat(((Number) get.getField("field_len").getValue()).intValue(), is(1));
    }

    public void testBulkIndexRequestCarriesExtraFieldValues() throws Exception {
        Settings settings = Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .build();

        String index = "test4";
        assertTestMapping(this, index, settings);

        BulkRequest bulk = new BulkRequest();

        bulk.add(
            new IndexRequest(index).id("1")
                .source("{\"other\":\"x\"}", XContentType.JSON)
                .extraFieldValues(new ExtraFieldValues(Map.of("field", new BytesValue(new BytesArray(new byte[] { 1, 2 })))))
        );

        bulk.add(
            new IndexRequest(index).id("2")
                .source("{\"other\":\"y\"}", XContentType.JSON)
                .extraFieldValues(new ExtraFieldValues(Map.of("field", new PrimitiveFloatArray(new float[] { 3.0f }))))
        );

        BulkResponse resp = client().bulk(bulk).actionGet();
        assertThat(resp.buildFailureMessage(), resp.hasFailures(), is(false));
        refresh(index);

        GetResponse g1 = client().prepareGet(index, "1").setStoredFields("field_type", "field_len").get();
        assertThat(g1.getFields().keySet(), hasItems("field_type", "field_len"));
        assertThat(g1.getField("field_type").getValue().toString(), is("BYTES"));
        assertThat(((Number) g1.getField("field_len").getValue()).intValue(), is(2));

        GetResponse g2 = client().prepareGet(index, "2").setStoredFields("field_type", "field_dim", "field_f0").get();
        assertThat(g2.getFields().keySet(), hasItems("field_type", "field_dim", "field_f0"));
        assertThat(g2.getField("field_type").getValue().toString(), is("FLOAT_ARRAY"));
        assertThat(((Number) g2.getField("field_dim").getValue()).intValue(), is(1));
        assertEquals(3.0f, ((Number) g2.getField("field_f0").getValue()).floatValue(), 0.0f);
    }
}
