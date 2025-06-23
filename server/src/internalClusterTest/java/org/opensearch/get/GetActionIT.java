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
import org.opensearch.common.Nullable;
import org.opensearch.common.lucene.uid.Versions;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.support.DefaultShardOperationFailedException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.engine.VersionConflictEngineException;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import static java.util.Collections.singleton;
import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
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
