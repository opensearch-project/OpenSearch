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

package org.opensearch.get;

import org.opensearch.action.DocWriteResponse;
import org.opensearch.action.admin.indices.alias.Alias;
import org.opensearch.action.admin.indices.flush.FlushResponse;
import org.opensearch.action.delete.DeleteResponse;
import org.opensearch.action.get.GetRequestBuilder;
import org.opensearch.action.get.GetResponse;
import org.opensearch.action.get.MultiGetRequest;
import org.opensearch.action.get.MultiGetRequestBuilder;
import org.opensearch.action.get.MultiGetResponse;
import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.update.UpdateResponse;
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.geometry.utils.Geohash;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.hamcrest.Matchers.containsInRelativeOrder;
import static org.hamcrest.Matchers.endsWith;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.startsWith;

public class GetActionIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Collections.singleton(InternalSettingsPlugin.class);
    }

    public void testSimpleGet() {
        assertAcked(
            prepareCreate("test").setMapping("field1", "type=keyword,store=true", "field2", "type=keyword,store=true")
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
                .addAlias(new Alias("alias").writeIndex(randomFrom(true, false, null)))
        );
        ensureGreen();

        GetResponse response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        logger.info("--> non realtime get 1");
        response = client().prepareGet(indexOrAlias(), "1").setRealtime(false).get();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> realtime get 1");
        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime get 1 (no source, implicit)");
        response = client().prepareGet(indexOrAlias(), "1").setStoredFields(Strings.EMPTY_ARRAY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        Set<String> fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(Collections.<String>emptySet()));
        assertThat(response.getSourceAsBytes(), nullValue());

        logger.info("--> realtime get 1 (no source, explicit)");
        response = client().prepareGet(indexOrAlias(), "1").setFetchSource(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(Collections.<String>emptySet()));
        assertThat(response.getSourceAsBytes(), nullValue());

        logger.info("--> realtime get 1 (no type)");
        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field");
        response = client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytes(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source");
        response = client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").setFetchSource("field1", null).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap(), hasKey("field1"));
        assertThat(response.getSourceAsMap(), not(hasKey("field2")));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime get 1");
        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> refresh the index, so we load it from it");
        refresh();

        logger.info("--> non realtime get 1 (loaded from index)");
        response = client().prepareGet(indexOrAlias(), "1").setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2"));

        logger.info("--> realtime fetch of field (loaded from index)");
        response = client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytes(), nullValue());
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> realtime fetch of field & source (loaded from index)");
        response = client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").setFetchSource(true).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsBytes(), not(nullValue()));
        assertThat(response.getField("field1").getValues().get(0).toString(), equalTo("value1"));
        assertThat(response.getField("field2"), nullValue());

        logger.info("--> update doc 1");
        client().prepareIndex("test").setId("1").setSource("field1", "value1_1", "field2", "value2_1").get();

        logger.info("--> realtime get 1");
        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_1"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_1"));

        logger.info("--> update doc 1 again");
        client().prepareIndex("test").setId("1").setSource("field1", "value1_2", "field2", "value2_2").get();

        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getSourceAsMap().get("field1").toString(), equalTo("value1_2"));
        assertThat(response.getSourceAsMap().get("field2").toString(), equalTo("value2_2"));

        DeleteResponse deleteResponse = client().prepareDelete("test", "1").get();
        assertEquals(DocWriteResponse.Result.DELETED, deleteResponse.getResult());

        response = client().prepareGet(indexOrAlias(), "1").get();
        assertThat(response.isExists(), equalTo(false));
    }

    public void testGetWithAliasPointingToMultipleIndices() {
        client().admin().indices().prepareCreate("index1").addAlias(new Alias("alias1").indexRouting("0")).get();
        if (randomBoolean()) {
            client().admin()
                .indices()
                .prepareCreate("index2")
                .addAlias(new Alias("alias1").indexRouting("0").writeIndex(randomFrom(false, null)))
                .get();
        } else {
            client().admin().indices().prepareCreate("index3").addAlias(new Alias("alias1").indexRouting("1").writeIndex(true)).get();
        }
        IndexResponse indexResponse = client().prepareIndex("index1").setId("id").setSource(Collections.singletonMap("foo", "bar")).get();
        assertThat(indexResponse.status().getStatus(), equalTo(RestStatus.CREATED.getStatus()));

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareGet("alias1", "_alias_id").get()
        );
        assertThat(exception.getMessage(), endsWith("can't execute a single index op"));
    }

    static String indexOrAlias() {
        return randomBoolean() ? "test" : "alias";
    }

    public void testSimpleMultiGet() throws Exception {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias").writeIndex(randomFrom(true, false, null)))
                .setMapping("field", "type=keyword,store=true")
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
        );
        ensureGreen();

        MultiGetResponse response = client().prepareMultiGet().add(indexOrAlias(), "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 10; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        response = client().prepareMultiGet()
            .add(indexOrAlias(), "1")
            .add(indexOrAlias(), "15")
            .add(indexOrAlias(), "3")
            .add(indexOrAlias(), "9")
            .add(indexOrAlias(), "11")
            .get();
        assertThat(response.getResponses().length, equalTo(5));
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("15"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(false));
        assertThat(response.getResponses()[2].getId(), equalTo("3"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[3].getId(), equalTo("9"));
        assertThat(response.getResponses()[3].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[3].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[3].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[4].getId(), equalTo("11"));
        assertThat(response.getResponses()[4].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[4].getResponse().getIndex(), equalTo("test"));
        assertThat(response.getResponses()[4].getResponse().isExists(), equalTo(false));

        // multi get with specific field
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").storedFields("field"))
            .add(new MultiGetRequest.Item(indexOrAlias(), "3").storedFields("field"))
            .get();

        assertThat(response.getResponses().length, equalTo(2));
        assertThat(response.getResponses()[0].getResponse().getSourceAsBytes(), nullValue());
        assertThat(response.getResponses()[0].getResponse().getField("field").getValues().get(0).toString(), equalTo("value1"));
    }

    public void testGetDocWithMultivaluedFields() throws Exception {
        String mapping1 = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("field")
            .field("type", "text")
            .field("store", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();
        assertAcked(prepareCreate("test").setMapping(mapping1));
        ensureGreen();

        GetResponse response = client().prepareGet("test", "1").get();
        assertThat(response.isExists(), equalTo(false));
        assertThat(response.isExists(), equalTo(false));

        client().prepareIndex("test").setId("1").setSource(jsonBuilder().startObject().array("field", "1", "2").endObject()).get();

        response = client().prepareGet("test", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        Set<String> fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));

        // Now test values being fetched from stored fields.
        refresh();
        response = client().prepareGet("test", "1").setStoredFields("field").get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        fields = new HashSet<>(response.getFields().keySet());
        assertThat(fields, equalTo(singleton("field")));
        assertThat(response.getFields().get("field").getValues().size(), equalTo(2));
        assertThat(response.getFields().get("field").getValues().get(0).toString(), equalTo("1"));
        assertThat(response.getFields().get("field").getValues().get(1).toString(), equalTo("2"));
    }

    public void testGetWithVersion() {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen();

        GetResponse response = client().prepareGet("test", "1").get();
        assertThat(response.isExists(), equalTo(false));

        logger.info("--> index doc 1");
        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        response = client().prepareGet(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        response = client().prepareGet(indexOrAlias(), "1").setVersion(1).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getVersion(), equalTo(1L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(2).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        // From Lucene index:
        refresh();

        response = client().prepareGet(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        response = client().prepareGet(indexOrAlias(), "1").setVersion(1).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(1L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(2).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        logger.info("--> index doc 1 again, so increasing the version");
        client().prepareIndex("test").setId("1").setSource("field1", "value1", "field2", "value2").get();

        // From translog:

        response = client().prepareGet(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(1).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        response = client().prepareGet(indexOrAlias(), "1").setVersion(2).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        // From Lucene index:
        refresh();

        response = client().prepareGet(indexOrAlias(), "1").setVersion(Versions.MATCH_ANY).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));

        try {
            client().prepareGet(indexOrAlias(), "1").setVersion(1).setRealtime(false).get();
            fail();
        } catch (VersionConflictEngineException e) {
            // all good
        }

        response = client().prepareGet(indexOrAlias(), "1").setVersion(2).setRealtime(false).get();
        assertThat(response.isExists(), equalTo(true));
        assertThat(response.getId(), equalTo("1"));
        assertThat(response.getIndex(), equalTo("test"));
        assertThat(response.getVersion(), equalTo(2L));
    }

    public void testMultiGetWithVersion() throws Exception {
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSettings(Settings.builder().put("index.refresh_interval", -1)));
        ensureGreen();

        MultiGetResponse response = client().prepareMultiGet().add(indexOrAlias(), "1").get();
        assertThat(response.getResponses().length, equalTo(1));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(false));

        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(2))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("[1]: version conflict"));
        assertThat(response.getResponses()[2].getFailure().getFailure(), instanceOf(VersionConflictEngineException.class));

        // Version from Lucene index
        refresh();
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "1").version(2))
            .setRealtime(false)
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("1"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[1].getId(), equalTo("1"));
        assertThat(response.getResponses()[1].getFailure(), nullValue());
        assertThat(response.getResponses()[1].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[1].getResponse().getSourceAsMap().get("field").toString(), equalTo("value1"));
        assertThat(response.getResponses()[2].getFailure(), notNullValue());
        assertThat(response.getResponses()[2].getFailure().getId(), equalTo("1"));
        assertThat(response.getResponses()[2].getFailure().getMessage(), startsWith("[1]: version conflict"));
        assertThat(response.getResponses()[2].getFailure().getFailure(), instanceOf(VersionConflictEngineException.class));

        for (int i = 0; i < 3; i++) {
            client().prepareIndex("test").setId(Integer.toString(i)).setSource("field", "value" + i).get();
        }

        // Version from translog
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(2))
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("[2]: version conflict"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));

        // Version from Lucene index
        refresh();
        response = client().prepareMultiGet()
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(Versions.MATCH_ANY))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(1))
            .add(new MultiGetRequest.Item(indexOrAlias(), "2").version(2))
            .setRealtime(false)
            .get();
        assertThat(response.getResponses().length, equalTo(3));
        // [0] version doesn't matter, which is the default
        assertThat(response.getResponses()[0].getFailure(), nullValue());
        assertThat(response.getResponses()[0].getId(), equalTo("2"));
        assertThat(response.getResponses()[0].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[0].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[0].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
        assertThat(response.getResponses()[1].getFailure(), notNullValue());
        assertThat(response.getResponses()[1].getFailure().getId(), equalTo("2"));
        assertThat(response.getResponses()[1].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[1].getFailure().getMessage(), startsWith("[2]: version conflict"));
        assertThat(response.getResponses()[2].getId(), equalTo("2"));
        assertThat(response.getResponses()[2].getIndex(), equalTo("test"));
        assertThat(response.getResponses()[2].getFailure(), nullValue());
        assertThat(response.getResponses()[2].getResponse().isExists(), equalTo(true));
        assertThat(response.getResponses()[2].getResponse().getSourceAsMap().get("field").toString(), equalTo("value2"));
    }

    public void testGetFieldsNonLeafField() throws Exception {
        assertAcked(
            prepareCreate("test").addAlias(new Alias("alias"))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("field1")
                        .startObject("properties")
                        .startObject("field2")
                        .field("type", "text")
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
        );

        client().prepareIndex("test")
            .setId("1")
            .setSource(jsonBuilder().startObject().startObject("field1").field("field2", "value1").endObject().endObject())
            .get();

        IllegalArgumentException exc = expectThrows(
            IllegalArgumentException.class,
            () -> client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").get()
        );
        assertThat(exc.getMessage(), equalTo("field [field1] isn't a leaf field"));

        flush();

        exc = expectThrows(IllegalArgumentException.class, () -> client().prepareGet(indexOrAlias(), "1").setStoredFields("field1").get());
        assertThat(exc.getMessage(), equalTo("field [field1] isn't a leaf field"));
    }

    public void testGetFieldsComplexField() throws Exception {
        assertAcked(
            prepareCreate("my-index")
                // multi types in 5.6
                .setSettings(Settings.builder().put("index.refresh_interval", -1))
                .setMapping(
                    jsonBuilder().startObject()
                        .startObject("properties")
                        .startObject("field1")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field2")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field3")
                        .field("type", "object")
                        .startObject("properties")
                        .startObject("field4")
                        .field("type", "text")
                        .field("store", true)
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                        .endObject()
                )
        );

        BytesReference source = BytesReference.bytes(
            jsonBuilder().startObject()
                .startArray("field1")
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value1")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .startObject()
                .startObject("field2")
                .startArray("field3")
                .startObject()
                .field("field4", "value2")
                .endObject()
                .endArray()
                .endObject()
                .endObject()
                .endArray()
                .endObject()
        );

        logger.info("indexing documents");

        client().prepareIndex("my-index").setId("1").setSource(source, MediaTypeRegistry.JSON).get();

        logger.info("checking real time retrieval");

        String field = "field1.field2.field3.field4";
        GetResponse getResponse = client().prepareGet("my-index", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        getResponse = client().prepareGet("my-index", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));

        logger.info("waiting for recoveries to complete");

        // Flush fails if shard has ongoing recoveries, make sure the cluster is settled down
        ensureGreen();

        logger.info("flushing");
        FlushResponse flushResponse = client().admin().indices().prepareFlush("my-index").setForce(true).get();
        if (flushResponse.getSuccessfulShards() == 0) {
            StringBuilder sb = new StringBuilder("failed to flush at least one shard. total shards [").append(
                flushResponse.getTotalShards()
            ).append("], failed shards: [").append(flushResponse.getFailedShards()).append("]");
            for (DefaultShardOperationFailedException failure : flushResponse.getShardFailures()) {
                sb.append("\nShard failure: ").append(failure);
            }
            fail(sb.toString());
        }

        logger.info("checking post-flush retrieval");

        getResponse = client().prepareGet("my-index", "1").setStoredFields(field).get();
        assertThat(getResponse.isExists(), equalTo(true));
        assertThat(getResponse.getField(field).getValues().size(), equalTo(2));
        assertThat(getResponse.getField(field).getValues().get(0).toString(), equalTo("value1"));
        assertThat(getResponse.getField(field).getValues().get(1).toString(), equalTo("value2"));
    }

    public void testUngeneratedFieldsThatAreNeverStored() throws IOException {
        String createIndexSource = "{\n"
            + "  \"settings\": {\n"
            + "    \"index.translog.flush_threshold_size\": \"1pb\",\n"
            + "    \"refresh_interval\": \"-1\"\n"
            + "  },\n"
            + "  \"mappings\": {\n"
            + "    \"_doc\": {\n"
            + "      \"properties\": {\n"
            + "        \"suggest\": {\n"
            + "          \"type\": \"completion\"\n"
            + "        }\n"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, MediaTypeRegistry.JSON));
        ensureGreen();
        String doc = "{\n"
            + "  \"suggest\": {\n"
            + "    \"input\": [\n"
            + "      \"Nevermind\",\n"
            + "      \"Nirvana\"\n"
            + "    ]\n"
            + "  }\n"
            + "}";

        index("test", "_doc", "1", doc);
        String[] fieldsList = { "suggest" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
    }

    public void testUngeneratedFieldsThatAreAlwaysStored() throws IOException {
        String createIndexSource = "{\n"
            + "  \"settings\": {\n"
            + "    \"index.translog.flush_threshold_size\": \"1pb\",\n"
            + "    \"refresh_interval\": \"-1\"\n"
            + "  }\n"
            + "}";
        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, MediaTypeRegistry.JSON));
        ensureGreen();

        client().prepareIndex("test").setId("1").setRouting("routingValue").setId("1").setSource("{}", MediaTypeRegistry.JSON).get();

        String[] fieldsList = { "_routing" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "routingValue");
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "routingValue");
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "routingValue");
    }

    public void testUngeneratedFieldsNotPartOfSourceStored() throws IOException {
        String createIndexSource = "{\n"
            + "  \"settings\": {\n"
            + "    \"index.translog.flush_threshold_size\": \"1pb\",\n"
            + "    \"refresh_interval\": \"-1\"\n"
            + "  }\n"
            + "}";

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, MediaTypeRegistry.JSON));
        ensureGreen();
        String doc = "{\n" + "  \"text\": \"some text.\"\n" + "}\n";
        client().prepareIndex("test").setId("1").setSource(doc, MediaTypeRegistry.JSON).setRouting("1").get();
        String[] fieldsList = { "_routing" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "1");
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "1");
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList, "1");
    }

    public void testGeneratedStringFieldsUnstored() throws IOException {
        indexSingleDocumentWithStringFieldsGeneratedFromText(false, randomBoolean());
        String[] fieldsList = { "_field_names" };
        // before refresh - document is only in translog
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
        refresh();
        // after refresh - document is in translog and also indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysNull(indexOrAlias(), "_doc", "1", fieldsList);
    }

    public void testGeneratedStringFieldsStored() throws IOException {
        indexSingleDocumentWithStringFieldsGeneratedFromText(true, randomBoolean());
        String[] fieldsList = { "text1", "text2" };
        String[] alwaysNotStoredFieldsList = { "_field_names" };
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList);
        assertGetFieldsNull(indexOrAlias(), "_doc", "1", alwaysNotStoredFieldsList);
        flush();
        // after flush - document is in not anymore translog - only indexed
        assertGetFieldsAlwaysWorks(indexOrAlias(), "_doc", "1", fieldsList);
        assertGetFieldsNull(indexOrAlias(), "_doc", "1", alwaysNotStoredFieldsList);
    }

    public void testDerivedSourceSimple() throws IOException {
        // Create index with derived source index setting enabled
        String createIndexSource = """
            {
                "settings": {
                    "index": {
                        "number_of_shards": 2,
                        "number_of_replicas": 0,
                        "derived_source": {
                            "enabled": true
                        },
                        "refresh_interval": -1
                    }
                },
                "mappings": {
                    "_doc": {
                        "properties": {
                            "geopoint_field": {
                                "type": "geo_point"
                            },
                            "keyword_field": {
                                "type": "keyword"
                            },
                            "numeric_field": {
                                "type": "long"
                            },
                            "date_field": {
                                "type": "date"
                            },
                            "bool_field": {
                                "type": "boolean"
                            },
                            "text_field": {
                                "type": "text"
                            },
                            "ip_field": {
                                "type": "ip"
                            }
                        }
                    }
                }
            }""";

        assertAcked(prepareCreate("test_derive").setSource(createIndexSource, MediaTypeRegistry.JSON));
        ensureGreen();

        // Index a document with various field types
        client().prepareIndex("test_derive")
            .setId("1")
            .setSource(
                jsonBuilder().startObject()
                    .field("geopoint_field", Geohash.stringEncode(40.33, 75.98))
                    .field("keyword_field", "test_keyword")
                    .field("numeric_field", 123)
                    .field("date_field", "2023-01-01")
                    .field("bool_field", true)
                    .field("text_field", "test text")
                    .field("ip_field", "1.2.3.4")
                    .endObject()
            )
            .get();

        // before refresh - document is only in translog
        GetResponse getResponse = client().prepareGet("test_derive", "1").get();
        assertTrue(getResponse.isExists());
        Map<String, Object> source = getResponse.getSourceAsMap();
        assertNotNull("Derived source should not be null", source);
        validateDeriveSource(source);

        refresh();
        // after refresh - document is in translog and also indexed
        getResponse = client().prepareGet("test_derive", "1").get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertNotNull("Derived source should not be null", source);
        validateDeriveSource(source);

        flush();
        // after flush - document is in not anymore translog - only indexed
        getResponse = client().prepareGet("test_derive", "1").get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertNotNull("Derived source should not be null", source);
        validateDeriveSource(source);

        // Test get with selective field inclusion
        getResponse = client().prepareGet("test_derive", "1").setFetchSource(new String[] { "keyword_field", "numeric_field" }, null).get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertEquals(2, source.size());
        assertEquals("test_keyword", source.get("keyword_field"));
        assertEquals(123, source.get("numeric_field"));

        // Test get with field exclusion
        getResponse = client().prepareGet("test_derive", "1").setFetchSource(null, new String[] { "text_field", "date_field" }).get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertEquals(5, source.size());
        assertFalse(source.containsKey("text_field"));
        assertFalse(source.containsKey("date_field"));
    }

    public void testDerivedSource_MultiValuesAndComplexField() throws Exception {
        // Create mapping with properly closed objects
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("level1")
            .startObject("properties")
            .startObject("level2")
            .startObject("properties")
            .startObject("level3")
            .startObject("properties")
            .startObject("num_field")
            .field("type", "integer")
            .endObject()
            .startObject("ip_field")
            .field("type", "ip")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        // Create index with settings and mapping
        assertAcked(
            prepareCreate("test_derive").setSettings(
                Settings.builder()
                    .put("index.number_of_shards", 1)
                    .put("index.number_of_replicas", 0)
                    .put("index.derived_source.enabled", true)
            ).setMapping(mapping)
        );
        ensureGreen();

        // Create source document
        XContentBuilder sourceBuilder = jsonBuilder().startObject()
            .startArray("level1")
            .startObject()
            .startObject("level2")
            .startArray("level3")
            .startObject()
            .startArray("num_field")
            .value(2)
            .value(1)
            .value(1)
            .endArray()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .startObject()
            .startObject("level2")
            .startArray("level3")
            .startObject()
            .startArray("ip_field")
            .value("1.2.3.4")
            .value("2.3.4.5")
            .value("1.2.3.4")
            .endArray()
            .endObject()
            .endArray()
            .endObject()
            .endObject()
            .endArray()
            .endObject();

        // Index the document
        IndexResponse indexResponse = client().prepareIndex("test_derive").setId("1").setSource(sourceBuilder).get();
        assertThat(indexResponse.status(), equalTo(RestStatus.CREATED));

        refresh();

        // Test numeric field retrieval
        GetResponse getResponse = client().prepareGet("test_derive", "1").get();
        assertThat(getResponse.isExists(), equalTo(true));
        Map<String, Object> source = getResponse.getSourceAsMap();
        Map<String, Object> level1 = (Map<String, Object>) source.get("level1");
        Map<String, Object> level2 = (Map<String, Object>) level1.get("level2");
        Map<String, Object> level3 = (Map<String, Object>) level2.get("level3");
        List<Object> numValues = (List<Object>) level3.get("num_field");
        assertThat(numValues.size(), equalTo(3));
        // Number field is stored as Sorted Numeric, so result should be in sorted order
        assertThat(numValues, containsInRelativeOrder(1, 1, 2));

        List<Object> ipValues = (List<Object>) level3.get("ip_field");
        assertThat(ipValues.size(), equalTo(2));
        // Ip field is stored as Sorted Set, so duplicates should be removed and result should be in sorted order
        assertThat(ipValues, containsInRelativeOrder("1.2.3.4", "2.3.4.5"));
    }

    public void testDerivedSourceTranslogReadPreference() throws Exception {
        // Create index with derived source enabled and translog read preference set
        Settings.Builder settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", -1)
            .put(IndexSettings.INDEX_DERIVED_SOURCE_SETTING.getKey(), true)
            .put(IndexSettings.INDEX_DERIVED_SOURCE_TRANSLOG_ENABLED_SETTING.getKey(), true);

        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("geopoint_field")
            .field("type", "geo_point")
            .endObject()
            .startObject("date_field")
            .field("type", "date")
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(prepareCreate("test").setSettings(settings).setMapping(mapping));
        ensureGreen();

        // Index a document
        IndexResponse indexResponse = client().prepareIndex("test")
            .setId("1")
            .setSource(
                jsonBuilder().startObject().field("geopoint_field", "40.7128,-74.0060").field("date_field", "2025-07-29").endObject()
            )
            .get();
        assertEquals(RestStatus.CREATED, indexResponse.status());

        // Get document prior to update, which would be derived source
        GetResponse getResponse = client().prepareGet("test", "1").get();
        assertTrue(getResponse.isExists());
        Map<String, Object> source = getResponse.getSourceAsMap();
        assertNotNull(source);
        Map<String, Object> geopoint = (Map<String, Object>) source.get("geopoint_field");
        assertEquals(40.7128, (double) geopoint.get("lat"), 0.0001);
        assertEquals(-74.0060, (double) geopoint.get("lon"), 0.0001);
        assertEquals("2025-07-29T00:00:00.000Z", source.get("date_field"));

        // Update document, so that version map gets created and get will be served from translog
        UpdateResponse updateResponse = client().prepareUpdate("test", "1")
            .setDoc(jsonBuilder().startObject().field("geopoint_field", "51.5074,-0.1278").field("date_field", "2025-07-30").endObject())
            .get();
        assertEquals(RestStatus.OK, updateResponse.status());

        // Get updated document, this will be derived source as per translog read preference setting
        getResponse = client().prepareGet("test", "1").get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertNotNull(source);
        geopoint = (Map<String, Object>) source.get("geopoint_field");
        assertEquals(51.5074, (double) geopoint.get("lat"), 0.0001);
        assertEquals(-0.1278, (double) geopoint.get("lon"), 0.0001);
        assertEquals("2025-07-30T00:00:00.000Z", source.get("date_field"));

        // Update translog read preference to source
        assertAcked(
            client().admin()
                .indices()
                .prepareUpdateSettings("test")
                .setSettings(Settings.builder().put(IndexSettings.INDEX_DERIVED_SOURCE_TRANSLOG_ENABLED_SETTING.getKey(), false))
                .get()
        );

        // Get document after setting update for fetching original source
        getResponse = client().prepareGet("test", "1").get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertNotNull(source);
        assertEquals("51.5074,-0.1278", source.get("geopoint_field"));
        assertEquals("2025-07-30", source.get("date_field"));

        // Flush the index
        flushAndRefresh("test");

        // Get document after flush, it should be a derived source
        getResponse = client().prepareGet("test", "1").get();
        assertTrue(getResponse.isExists());
        source = getResponse.getSourceAsMap();
        assertNotNull(source);
        geopoint = (Map<String, Object>) source.get("geopoint_field");
        assertEquals(51.5074, (double) geopoint.get("lat"), 0.0001);
        assertEquals(-0.1278, (double) geopoint.get("lon"), 0.0001);
        assertEquals("2025-07-30T00:00:00.000Z", source.get("date_field"));
    }

    void validateDeriveSource(Map<String, Object> source) {
        Map<String, Object> latLon = (Map<String, Object>) source.get("geopoint_field");
        assertEquals(75.98, (Double) latLon.get("lat"), 0.001);
        assertEquals(40.33, (Double) latLon.get("lon"), 0.001);
        assertEquals("test_keyword", source.get("keyword_field"));
        assertEquals(123, source.get("numeric_field"));
        assertEquals("2023-01-01T00:00:00.000Z", source.get("date_field"));
        assertEquals(true, source.get("bool_field"));
        assertEquals("test text", source.get("text_field"));
        assertEquals("1.2.3.4", source.get("ip_field"));
    }

    void indexSingleDocumentWithStringFieldsGeneratedFromText(boolean stored, boolean sourceEnabled) {

        String storedString = stored ? "true" : "false";
        String createIndexSource = "{\n"
            + "  \"settings\": {\n"
            + "    \"index.translog.flush_threshold_size\": \"1pb\",\n"
            + "    \"refresh_interval\": \"-1\"\n"
            + "  },\n"
            + "  \"mappings\": {\n"
            + "    \"_doc\": {\n"
            + "      \"_source\" : {\"enabled\" : "
            + sourceEnabled
            + "},"
            + "      \"properties\": {\n"
            + "        \"text1\": {\n"
            + "          \"type\": \"text\",\n"
            + "          \"store\": \""
            + storedString
            + "\""
            + "        },\n"
            + "        \"text2\": {\n"
            + "          \"type\": \"text\",\n"
            + "          \"store\": \""
            + storedString
            + "\""
            + "        }"
            + "      }\n"
            + "    }\n"
            + "  }\n"
            + "}";

        assertAcked(prepareCreate("test").addAlias(new Alias("alias")).setSource(createIndexSource, MediaTypeRegistry.JSON));
        ensureGreen();
        String doc = "{\n" + "  \"text1\": \"some text.\"\n," + "  \"text2\": \"more text.\"\n" + "}\n";
        index("test", "_doc", "1", doc);
    }

    private void assertGetFieldsAlwaysWorks(String index, String type, String docId, String[] fields) {
        assertGetFieldsAlwaysWorks(index, type, docId, fields, null);
    }

    private void assertGetFieldsAlwaysWorks(String index, String type, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldWorks(index, type, docId, field, routing);
            assertGetFieldWorks(index, type, docId, field, routing);
        }
    }

    private void assertGetFieldWorks(String index, String type, String docId, String field, @Nullable String routing) {
        GetResponse response = getDocument(index, type, docId, field, routing);
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
        assertNotNull(response.getField(field));
        response = multiGetDocument(index, type, docId, field, routing);
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
        assertNotNull(response.getField(field));
    }

    protected void assertGetFieldsNull(String index, String type, String docId, String[] fields) {
        assertGetFieldsNull(index, type, docId, fields, null);
    }

    protected void assertGetFieldsNull(String index, String type, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldNull(index, type, docId, field, routing);
        }
    }

    protected void assertGetFieldsAlwaysNull(String index, String type, String docId, String[] fields) {
        assertGetFieldsAlwaysNull(index, type, docId, fields, null);
    }

    protected void assertGetFieldsAlwaysNull(String index, String type, String docId, String[] fields, @Nullable String routing) {
        for (String field : fields) {
            assertGetFieldNull(index, type, docId, field, routing);
            assertGetFieldNull(index, type, docId, field, routing);
        }
    }

    protected void assertGetFieldNull(String index, String type, String docId, String field, @Nullable String routing) {
        // for get
        GetResponse response = getDocument(index, type, docId, field, routing);
        assertTrue(response.isExists());
        assertNull(response.getField(field));
        assertThat(response.getId(), equalTo(docId));
        // same for multi get
        response = multiGetDocument(index, type, docId, field, routing);
        assertNull(response.getField(field));
        assertThat(response.getId(), equalTo(docId));
        assertTrue(response.isExists());
    }

    private GetResponse multiGetDocument(String index, String type, String docId, String field, @Nullable String routing) {
        MultiGetRequest.Item getItem = new MultiGetRequest.Item(index, docId).storedFields(field);
        if (routing != null) {
            getItem.routing(routing);
        }
        MultiGetRequestBuilder multiGetRequestBuilder = client().prepareMultiGet().add(getItem);
        MultiGetResponse multiGetResponse = multiGetRequestBuilder.get();
        assertThat(multiGetResponse.getResponses().length, equalTo(1));
        return multiGetResponse.getResponses()[0].getResponse();
    }

    private GetResponse getDocument(String index, String type, String docId, String field, @Nullable String routing) {
        GetRequestBuilder getRequestBuilder = client().prepareGet().setIndex(index).setId(docId).setStoredFields(field);
        if (routing != null) {
            getRequestBuilder.setRouting(routing);
        }
        return getRequestBuilder.get();
    }
}
