/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.index.query.ScriptQueryBuilder;
import org.opensearch.protobufs.BuiltinScriptLanguage;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.ScriptQuery;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Unit tests for ScriptQueryBuilderProtoUtils.
 */
public class ScriptQueryBuilderProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithInlineScript() {
        // Test inline script with all features
        ScriptQuery scriptQuery = ScriptQuery.newBuilder()
            .setScript(
                Script.newBuilder()
                    .setInline(
                        InlineScript.newBuilder()
                            .setSource("doc['field'].value > 0")
                            .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                            .setParams(
                                org.opensearch.protobufs.ObjectMap.newBuilder()
                                    .putFields("multiplier", org.opensearch.protobufs.ObjectMap.Value.newBuilder().setDouble(2.5).build())
                                    .build()
                            )
                            .putOptions("content_type", "application/json")
                            .build()
                    )
                    .build()
            )
            .setBoost(2.0f)
            .setXName("test_query")
            .build();

        ScriptQueryBuilder result = ScriptQueryBuilderProtoUtils.fromProto(scriptQuery);

        assertNotNull(result);
        assertEquals(2.0f, result.boost(), 0.001f);
        assertEquals("test_query", result.queryName());
        assertNotNull(result.script());
        assertEquals("doc['field'].value > 0", result.script().getIdOrCode());
        assertEquals("painless", result.script().getLang());
        assertNotNull(result.script().getParams());
        assertEquals(2.5, result.script().getParams().get("multiplier"));
        assertNotNull(result.script().getOptions());
        assertEquals("application/json", result.script().getOptions().get("content_type"));
    }

    public void testFromProtoWithStoredScript() {
        ScriptQuery scriptQuery = ScriptQuery.newBuilder()
            .setScript(
                Script.newBuilder()
                    .setStored(
                        org.opensearch.protobufs.StoredScriptId.newBuilder()
                            .setId("my_stored_script")
                            .setParams(
                                org.opensearch.protobufs.ObjectMap.newBuilder()
                                    .putFields(
                                        "param1",
                                        org.opensearch.protobufs.ObjectMap.Value.newBuilder().setString("test_value").build()
                                    )
                                    .build()
                            )
                            .build()
                    )
                    .build()
            )
            .build();

        ScriptQueryBuilder result = ScriptQueryBuilderProtoUtils.fromProto(scriptQuery);

        assertNotNull(result);
        assertEquals("my_stored_script", result.script().getIdOrCode());
        assertNull(result.script().getLang());
        assertNotNull(result.script().getParams());
        assertEquals("test_value", result.script().getParams().get("param1"));
    }

    public void testFromProtoWithDifferentScriptLanguages() {
        // Test different script languages
        String[] languages = { "painless", "java", "mustache", "expression", "custom_lang" };
        BuiltinScriptLanguage[] builtinLangs = {
            BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS,
            BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_JAVA,
            BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_MUSTACHE,
            BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_EXPRESSION,
            null };

        for (int i = 0; i < languages.length; i++) {
            ScriptQuery.Builder scriptQueryBuilder = ScriptQuery.newBuilder();
            InlineScript.Builder inlineScriptBuilder = InlineScript.newBuilder().setSource("test_script_" + i);

            if (builtinLangs[i] != null) {
                inlineScriptBuilder.setLang(ScriptLanguage.newBuilder().setBuiltin(builtinLangs[i]));
            } else {
                inlineScriptBuilder.setLang(ScriptLanguage.newBuilder().setCustom(languages[i]));
            }

            scriptQueryBuilder.setScript(Script.newBuilder().setInline(inlineScriptBuilder.build()).build());

            ScriptQueryBuilder result = ScriptQueryBuilderProtoUtils.fromProto(scriptQueryBuilder.build());

            assertNotNull(result);
            assertEquals(languages[i], result.script().getLang());
            assertEquals("test_script_" + i, result.script().getIdOrCode());
        }
    }

    public void testFromProtoWithDefaults() {
        ScriptQuery scriptQuery = ScriptQuery.newBuilder()
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("true").build()).build())
            .build();

        ScriptQueryBuilder result = ScriptQueryBuilderProtoUtils.fromProto(scriptQuery);

        assertNotNull(result);
        assertEquals(1.0f, result.boost(), 0.001f);
        assertNull(result.queryName());
        assertNotNull(result.script());
        assertEquals("true", result.script().getIdOrCode());
    }

    public void testFromProtoWithEdgeCases() {
        ScriptQuery scriptQuery = ScriptQuery.newBuilder()
            .setScript(Script.newBuilder().setInline(InlineScript.newBuilder().setSource("true").build()).build())
            .setBoost(0.0f)
            .setXName("")
            .build();

        ScriptQueryBuilder result = ScriptQueryBuilderProtoUtils.fromProto(scriptQuery);

        assertNotNull(result);
        assertEquals(0.0f, result.boost(), 0.001f);
        assertEquals("", result.queryName());
    }

    public void testFromProtoWithNullInput() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptQueryBuilderProtoUtils.fromProto(null)
        );
        assertEquals("ScriptQuery cannot be null", exception.getMessage());
    }

    public void testFromProtoWithMissingScript() {
        ScriptQuery scriptQuery = ScriptQuery.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptQueryBuilderProtoUtils.fromProto(scriptQuery)
        );
        assertEquals("script must be provided with a [script] query", exception.getMessage());
    }
}
