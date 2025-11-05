/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.sort;

import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.NestedSortValue;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.ScriptSort;
import org.opensearch.protobufs.ScriptSortType;
import org.opensearch.protobufs.SortMode;
import org.opensearch.protobufs.SortOrder;
import org.opensearch.search.sort.ScriptSortBuilder;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverterRegistryImpl;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

/**
 * Tests for {@link ScriptSortProtoUtils}.
 */
public class ScriptSortProtoUtilsTests extends OpenSearchTestCase {

    private QueryBuilderProtoConverterRegistry registry;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        registry = new QueryBuilderProtoConverterRegistryImpl();
    }

    public void testFromProto_WithRequiredFields() {
        // Create a simple inline script
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['price'].value * params.factor")
            .setLang(
                ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
            )
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();

        ScriptSort scriptSort = ScriptSort.newBuilder().setScript(script).setType(ScriptSortType.SCRIPT_SORT_TYPE_NUMBER).build();

        ScriptSortBuilder result = ScriptSortProtoUtils.fromProto(scriptSort, registry);

        assertNotNull(result);
        assertEquals(ScriptSortBuilder.ScriptSortType.NUMBER, result.type());
        assertNotNull(result.script());
    }

    public void testFromProto_WithAllFields() {
        // Create a simple inline script
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['title'].value")
            .setLang(
                ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
            )
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();

        // Create nested sort
        NestedSortValue nestedSort = NestedSortValue.newBuilder().setPath("nested.field").build();

        ScriptSort scriptSort = ScriptSort.newBuilder()
            .setScript(script)
            .setType(ScriptSortType.SCRIPT_SORT_TYPE_STRING)
            .setOrder(SortOrder.SORT_ORDER_DESC)
            .setMode(SortMode.SORT_MODE_MIN)
            .setNested(nestedSort)
            .build();

        ScriptSortBuilder result = ScriptSortProtoUtils.fromProto(scriptSort, registry);

        assertNotNull(result);
        assertEquals(ScriptSortBuilder.ScriptSortType.STRING, result.type());
        assertEquals(org.opensearch.search.sort.SortOrder.DESC, result.order());
        assertEquals(org.opensearch.search.sort.SortMode.MIN, result.sortMode());
        assertNotNull(result.getNestedSort());
        assertEquals("nested.field", result.getNestedSort().getPath());
    }

    public void testFromProto_NullInput() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> { ScriptSortProtoUtils.fromProto(null, registry); }
        );
        assertEquals("ScriptSort cannot be null", exception.getMessage());
    }

    public void testFromProto_MissingScript() {
        ScriptSort scriptSort = ScriptSort.newBuilder().setType(ScriptSortType.SCRIPT_SORT_TYPE_NUMBER).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            ScriptSortProtoUtils.fromProto(scriptSort, registry);
        });
        assertEquals("ScriptSort must have a script", exception.getMessage());
    }

    public void testFromProto_MissingType() {
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['price'].value")
            .setLang(
                ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
            )
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();

        ScriptSort scriptSort = ScriptSort.newBuilder().setScript(script).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            ScriptSortProtoUtils.fromProto(scriptSort, registry);
        });
        assertEquals("ScriptSort must have a type", exception.getMessage());
    }

    public void testFromProto_WithDefaultOrder() {
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("Math.random()")
            .setLang(
                ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
            )
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();

        ScriptSort scriptSort = ScriptSort.newBuilder().setScript(script).setType(ScriptSortType.SCRIPT_SORT_TYPE_NUMBER).build();

        ScriptSortBuilder result = ScriptSortProtoUtils.fromProto(scriptSort, registry);

        assertNotNull(result);
        // Default order should be ASC (ScriptSortBuilder default)
        assertEquals(org.opensearch.search.sort.SortOrder.ASC, result.order());
        assertNull(result.sortMode());
        assertNull(result.getNestedSort());
    }

    public void testFromProto_UnspecifiedType() {
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['price'].value")
            .setLang(
                ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
            )
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();

        ScriptSort scriptSort = ScriptSort.newBuilder().setScript(script).setType(ScriptSortType.SCRIPT_SORT_TYPE_UNSPECIFIED).build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> {
            ScriptSortProtoUtils.fromProto(scriptSort, registry);
        });
        assertEquals("ScriptSortType must be specified", exception.getMessage());
    }
}
