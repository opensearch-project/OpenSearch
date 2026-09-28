/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.fieldcaps;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.index.engine.dataformat.stub.MockDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.InternalSettingsPlugin;
import org.opensearch.test.OpenSearchSingleNodeTestCase;

import java.util.Collection;
import java.util.Map;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

public class TransportFieldCapabilitiesIndexActionTests extends OpenSearchSingleNodeTestCase {

    // The pluggable data format index needs a registered data format plus committer engine; without
    // a committer factory EngineConfigFactory rejects index creation when pluggable is enabled.
    @Override
    protected Collection<Class<? extends Plugin>> getPlugins() {
        return pluginList(InternalSettingsPlugin.class, MockDataFormatPlugin.class, MockCommitterEnginePlugin.class);
    }

    // With disable_objects=true, {"attributes": {"foo": {"bar": "baz"}}} is flattened into the leaf
    // field "attributes.foo.bar". The intermediate path "attributes.foo" has no ObjectMapper and
    // _field_caps must not silently return empty results when walking the parent chain.
    public void testFieldCapsDisableObjects() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("attributes")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test")
            .setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .startObject("foo")
                    .field("bar", "baz")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected attributes.foo.bar in field caps", fields.containsKey("attributes.foo.bar"));
        assertTrue("expected attributes in field caps", fields.containsKey("attributes"));
        assertEquals("object", fields.get("attributes").values().iterator().next().getType());
        // phantom intermediate path has no ObjectMapper and must not exist
        assertFalse("attributes.foo should not exist in field caps", fields.containsKey("attributes.foo"));
    }

    // Indexing a second document after a disable_objects field must not corrupt the flattened field
    // name. Previously "attributes.foo.bar" became "attributes.foo.foo.bar" after a mapping merge
    // triggered by an unrelated document.
    public void testFieldCapsDisableObjectsAfterUnrelatedDocument() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("attributes")
            .field("type", "object")
            .field("disable_objects", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        // doc 1: creates the disable_objects leaf field
        client().prepareIndex("test")
            .setId("1")
            .setSource(
                XContentFactory.jsonBuilder()
                    .startObject()
                    .startObject("attributes")
                    .startObject("foo")
                    .field("bar", "baz")
                    .endObject()
                    .endObject()
                    .endObject()
            )
            .get();

        // doc 2: unrelated - must not corrupt the existing mapping
        client().prepareIndex("test").setId("2").setSource("{\"key\": 1}", MediaTypeRegistry.JSON).get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected attributes.foo.bar in field caps", fields.containsKey("attributes.foo.bar"));
        assertTrue("expected attributes.foo.bar.keyword in field caps", fields.containsKey("attributes.foo.bar.keyword"));
        assertFalse("attributes.foo.foo.bar must not exist", fields.containsKey("attributes.foo.foo.bar"));
        assertTrue("expected attributes in field caps", fields.containsKey("attributes"));
        assertTrue("expected key in field caps", fields.containsKey("key"));
    }

    // Regression test: the parentPath fix in ParametrizedFieldMapper must not break normal object
    // field handling. Regular nested object fields (without disable_objects) must still produce the
    // correct parent entries in _field_caps.
    public void testFieldCapsNormalNestedObjectUnaffected() throws Exception {
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("user")
            .field("type", "object")
            .startObject("properties")
            .startObject("address")
            .field("type", "object")
            .startObject("properties")
            .startObject("city")
            .field("type", "keyword")
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .endObject()
            .toString();

        assertAcked(client().admin().indices().prepareCreate("test").setMapping(mapping));

        client().prepareIndex("test")
            .setId("1")
            .setSource("{\"user\":{\"address\":{\"city\":\"Chemnitz\"}}}", MediaTypeRegistry.JSON)
            .get();

        client().admin().indices().prepareRefresh("test").get();

        FieldCapabilitiesResponse response = client().fieldCaps(new FieldCapabilitiesRequest().fields("*").indices("test")).actionGet();

        Map<String, Map<String, FieldCapabilities>> fields = response.get();

        assertTrue("expected user.address.city", fields.containsKey("user.address.city"));
        assertTrue("expected user.address as object", fields.containsKey("user.address"));
        assertEquals("object", fields.get("user.address").values().iterator().next().getType());
        assertTrue("expected user as object", fields.containsKey("user"));
        assertEquals("object", fields.get("user").values().iterator().next().getType());
    }

    // Covers the pluggable branch in TransportFieldCapabilitiesIndexAction#shardOperation. A long
    // with index:false, doc_values:true has isSearchable()==false but is searchable via doc values,
    // so field caps must report searchable=true on a pluggable index and searchable=false otherwise.
    @LockFeatureFlag(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG)
    public void testSearchableReportedForDocValuesOnlyFieldOnPluggableIndex() throws Exception {
        // A doc-values-only long: not indexed for search, but backed by doc values.
        String mapping = XContentFactory.jsonBuilder()
            .startObject()
            .startObject("properties")
            .startObject("num")
            .field("type", "long")
            .field("index", false)
            .field("doc_values", true)
            .endObject()
            .endObject()
            .endObject()
            .toString();

        // Pluggable index: doc values make the field searchable, so field caps must report true.
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate("pluggable")
                .setSettings(Settings.builder().put(IndexSettings.PLUGGABLE_DATAFORMAT_ENABLED_SETTING.getKey(), true))
                .setMapping(mapping)
        );
        // Pluggable indices are append-only, so a custom document id is rejected; let it auto-generate.
        client().prepareIndex("pluggable").setSource("{\"num\": 42}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareRefresh("pluggable").get();

        FieldCapabilitiesResponse pluggableResponse = client().fieldCaps(new FieldCapabilitiesRequest().fields("num").indices("pluggable"))
            .actionGet();
        assertTrue(
            "doc-values-only field must be searchable on a pluggable index",
            pluggableResponse.get().get("num").get("long").isSearchable()
        );

        // Normal index: same mapping, no pluggable setting. The field stays non-searchable.
        assertAcked(client().admin().indices().prepareCreate("normal").setMapping(mapping));
        client().prepareIndex("normal").setSource("{\"num\": 42}", MediaTypeRegistry.JSON).get();
        client().admin().indices().prepareRefresh("normal").get();

        FieldCapabilitiesResponse normalResponse = client().fieldCaps(new FieldCapabilitiesRequest().fields("num").indices("normal"))
            .actionGet();
        assertFalse(
            "doc-values-only field must not be searchable on a normal index",
            normalResponse.get().get("num").get("long").isSearchable()
        );
    }
}
