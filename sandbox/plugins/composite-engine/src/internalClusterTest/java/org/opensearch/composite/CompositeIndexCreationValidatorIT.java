/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import org.opensearch.action.admin.indices.create.CreateIndexResponse;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.io.IOException;

/**
 * Integration tests for {@link CompositeIndexCreationValidator} — the mapping-time validations applied
 * to fields declared directly inside a {@code nested} object on a composite (pluggable data format)
 * index. Runs as an {@link org.opensearch.index.IndexCreationValidator}: throws
 * {@link IllegalArgumentException} and runs after mapping merge, so a shape it rejects is one
 * {@code ObjectMapper}/{@code DocumentMapper} themselves accept.
 *
 * <p>A dotted field name inside nested (e.g. {@code "meta.name"}) is rejected via the SAME message as
 * a plain {@code object} sub-field — both build the identical disallowed intermediate object mapper in
 * the resolved tree, which this validator walks instead of raw JSON.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class CompositeIndexCreationValidatorIT extends AbstractCompositeEngineIT {

    private static int indexCounter = 0;

    private Settings pluggableSettings() {
        return Settings.builder()
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .build();
    }

    private void startCluster() {
        internalCluster().startClusterManagerOnlyNode();
        internalCluster().startDataOnlyNode();
    }

    /** Builds a {@code _doc} mapping with a single {@code nested} field {@code n} whose sole sub-field is supplied by the caller. */
    private String nestedMapping(CheckedConsumer<XContentBuilder, IOException> subField) throws IOException {
        return nestedMapping(null, subField);
    }

    /** As above, additionally setting {@code n}'s own {@code dynamic} value (skipped if {@code null}). */
    private String nestedMapping(String dynamicValue, CheckedConsumer<XContentBuilder, IOException> subField) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject();
        builder.startObject("properties");
        builder.startObject("n");
        builder.field("type", "nested");
        if (dynamicValue != null) {
            builder.field("dynamic", dynamicValue);
        }
        builder.startObject("properties");
        subField.accept(builder);
        builder.endObject();
        builder.endObject();
        builder.endObject();
        builder.endObject();
        return BytesReference.bytes(builder).utf8ToString();
    }

    private String nextIndexName() {
        return "composite-nested-validation-" + (indexCounter++);
    }

    private void assertRejected(String mapping, String... expectedMessageFragments) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().prepareCreate(nextIndexName()).setSettings(pluggableSettings()).setMapping(mapping).get()
        );
        for (String fragment : expectedMessageFragments) {
            assertTrue(
                "expected message to contain [" + fragment + "] but was [" + e.getMessage() + "]",
                e.getMessage().contains(fragment)
            );
        }
    }

    private void assertAccepted(String mapping) {
        String indexName = nextIndexName();
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(pluggableSettings())
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    // ---- rejections -----------------------------------------------------------------------------

    public void testObjectInsideNestedRejected() throws IOException {
        startCluster();
        assertRejected(nestedMapping("false", b -> {
            b.startObject("obj").field("type", "object").startObject("properties");
            b.startObject("x").field("type", "keyword").endObject();
            b.endObject().endObject();
        }), "Object field [obj] inside nested field [n]", "not supported on composite (pluggable data format)");
    }

    /** An implicit object (a bare {@code properties} block with no {@code type}) resolves to CONTENT_TYPE and is rejected too. */
    public void testImplicitObjectInsideNestedRejected() throws IOException {
        startCluster();
        assertRejected(nestedMapping("false", b -> {
            b.startObject("obj").startObject("properties");
            b.startObject("x").field("type", "keyword").endObject();
            b.endObject().endObject();
        }), "Object field [obj] inside nested field [n]");
    }

    /**
     * "meta.name" implicitly builds an intermediate plain-object mapper for "meta" in the resolved tree
     * — the SAME disallowed shape as an explicit object, rejected via the same message (see class javadoc).
     */
    public void testDottedFieldNameInsideNestedRejected() throws IOException {
        startCluster();
        assertRejected(
            nestedMapping("false", b -> b.startObject("meta.name").field("type", "keyword").endObject()),
            "Object field [meta] inside nested field [n]"
        );
    }

    /** No "dynamic" key at all — inherits the TRUE default, which is what must be rejected. */
    public void testNestedWithoutDynamicFalseRejected() throws IOException {
        startCluster();
        assertRejected(
            nestedMapping(b -> b.startObject("a").field("type", "keyword").endObject()),
            "Nested field [n]",
            "dynamic: false",
            "dynamic: strict"
        );
    }

    public void testNestedDynamicTrueRejected() throws IOException {
        startCluster();
        assertRejected(nestedMapping("true", b -> b.startObject("a").field("type", "keyword").endObject()), "Nested field [n]");
    }

    /** A matching dynamic template can still add a brand-new mapper under this mode, so it's just as unsafe as dynamic:true. */
    public void testNestedDynamicFalseAllowTemplatesRejected() throws IOException {
        startCluster();
        assertRejected(
            nestedMapping("false_allow_templates", b -> b.startObject("a").field("type", "keyword").endObject()),
            "Nested field [n]"
        );
    }

    /** No properties block at all — must still be rejected. */
    public void testNestedWithNoPropertiesAtAllRejected() throws Exception {
        startCluster();
        XContentBuilder builder = XContentFactory.jsonBuilder();
        builder.startObject().startObject("properties").startObject("n").field("type", "nested").endObject().endObject().endObject();
        assertRejected(BytesReference.bytes(builder).utf8ToString(), "Nested field [n]");
    }

    // ---- allowed shapes ---------------------------------------------------------------------------

    /** flat_object is the intended container for open key spaces inside nested — must be accepted. */
    public void testFlatObjectInsideNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> b.startObject("meta").field("type", "flat_object").endObject()));
    }

    /** {@code index} is not validated inside nested (see class javadoc) — {@code index: false} must be accepted. */
    public void testIndexFalseInsideNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> b.startObject("a").field("type", "keyword").field("index", false).endObject()));
    }

    /** Same as {@link #testIndexFalseInsideNestedAllowed}, for the other explicit value: {@code index: true}. */
    public void testIndexTrueInsideNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> b.startObject("a").field("type", "keyword").field("index", true).endObject()));
    }

    /** A leaf with no explicit index parameter must still be accepted — this is the common, unremarkable case. */
    public void testDefaultLeafInsideNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> b.startObject("a").field("type", "keyword").endObject()));
    }

    /** A mix of explicit {@code index} values across a field and its multi-field must be accepted. */
    public void testMultiFieldIndexTrueInsideNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> {
            b.startObject("author").field("type", "keyword").field("index", false).startObject("fields");
            b.startObject("raw").field("type", "keyword").field("index", true).endObject();
            b.endObject().endObject();
        }));
    }

    /** strict is equally safe as false: an undeclared leaf is rejected outright rather than skipped. */
    public void testNestedDynamicStrictAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("strict", b -> b.startObject("a").field("type", "keyword").endObject()));
    }

    // ---- vanilla (non-composite) indices are unaffected ---------------------------------------------

    /** The validator is a no-op unless the pluggable data format setting is on — a vanilla index accepts every shape above. */
    public void testVanillaIndexUnaffected() throws IOException {
        startCluster();
        String indexName = nextIndexName();
        String mapping = nestedMapping(b -> {
            b.startObject("obj").field("type", "object").startObject("properties");
            b.startObject("x").field("type", "keyword").field("index", true).endObject();
            b.endObject().endObject();
        });
        // Explicit replicas=0: this cluster has only one data node, so the default (1) would leave a
        // replica UNASSIGNED forever and ensureGreen would time out.
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .setMapping(mapping)
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
    }

    /** Nested-in-nested, itself declaring dynamic:false, is accepted. */
    public void testNestedInNestedAllowed() throws IOException {
        startCluster();
        assertAccepted(nestedMapping("false", b -> {
            b.startObject("author").field("type", "keyword").endObject();
            b.startObject("replies");
            b.field("type", "nested");
            b.field("dynamic", "false");
            b.startObject("properties");
            b.startObject("text").field("type", "keyword").endObject();
            b.endObject();
            b.endObject();
        }));
    }

    // ---- mapping updates (PUT _mapping) -----------------------------------------------------------
    // The same validator runs on mapping updates, so a shape rejected at creation cannot be
    // introduced afterwards by PUT _mapping on an existing index.

    /** Creates a valid composite index with no nested field, for the update tests to target. */
    private String createValidIndex() {
        String indexName = nextIndexName();
        CreateIndexResponse response = client().admin()
            .indices()
            .prepareCreate(indexName)
            .setSettings(pluggableSettings())
            .setMapping("{\"properties\":{\"title\":{\"type\":\"keyword\"}}}")
            .get();
        assertTrue(response.isAcknowledged());
        ensureGreen(indexName);
        return indexName;
    }

    private void assertMappingUpdateRejected(String indexName, String mapping, String... expectedMessageFragments) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin().indices().preparePutMapping(indexName).setSource(mapping, XContentType.JSON).get()
        );
        for (String fragment : expectedMessageFragments) {
            assertTrue(
                "expected message to contain [" + fragment + "] but was [" + e.getMessage() + "]",
                e.getMessage().contains(fragment)
            );
        }
    }

    /** PUT _mapping adding a nested field without dynamic:false|strict must be rejected like creation is. */
    public void testPutMappingNestedWithoutDynamicFalseRejected() throws IOException {
        startCluster();
        String indexName = createValidIndex();
        assertMappingUpdateRejected(
            indexName,
            nestedMapping(b -> b.startObject("a").field("type", "keyword").endObject()),
            "Nested field [n]",
            "dynamic: false"
        );
    }

    /** PUT _mapping adding a nested field with dynamic:true must be rejected like creation is. */
    public void testPutMappingNestedDynamicTrueRejected() throws IOException {
        startCluster();
        String indexName = createValidIndex();
        assertMappingUpdateRejected(
            indexName,
            nestedMapping("true", b -> b.startObject("a").field("type", "keyword").endObject()),
            "Nested field [n]"
        );
    }

    /** PUT _mapping adding a nested field with a plain object child must be rejected like creation is. */
    public void testPutMappingObjectInsideNestedRejected() throws IOException {
        startCluster();
        String indexName = createValidIndex();
        assertMappingUpdateRejected(
            indexName,
            nestedMapping("false", b -> {
                b.startObject("meta");
                b.startObject("properties");
                b.startObject("name").field("type", "keyword").endObject();
                b.endObject();
                b.endObject();
            }),
            "Object field [meta] inside nested field [n]"
        );
    }

    /** A VALID nested field added via PUT _mapping must still be accepted — the guard must not over-reject. */
    public void testPutMappingValidNestedAccepted() throws IOException {
        startCluster();
        String indexName = createValidIndex();
        AcknowledgedResponse response = client().admin()
            .indices()
            .preparePutMapping(indexName)
            .setSource(
                nestedMapping("false", b -> b.startObject("a").field("type", "keyword").endObject()),
                XContentType.JSON
            )
            .get();
        assertTrue(response.isAcknowledged());
    }
}
