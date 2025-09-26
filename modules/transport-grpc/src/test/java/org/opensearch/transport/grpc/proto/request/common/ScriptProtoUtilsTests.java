/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.common;

import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.StoredScriptId;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.script.Script.DEFAULT_SCRIPT_LANG;

public class ScriptProtoUtilsTests extends OpenSearchTestCase {

    public void testParseFromProtoRequestWithInlineScript() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    )
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be painless", "painless", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
    }

    public void testParseFromProtoRequestWithInlineScriptAndCustomLanguage() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setCustom("custom_lang"))
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be custom_lang", "custom_lang", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
    }

    public void testParseFromProtoRequestWithInlineScriptAndParams() {
        ObjectMap params = ObjectMap.newBuilder()
            .putFields("factor", ObjectMap.Value.newBuilder().setDouble(2.5).build())
            .putFields("name", ObjectMap.Value.newBuilder().setString("test").build())
            .build();

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * params.factor")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    )
                    .setParams(params)
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be painless", "painless", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * params.factor", script.getIdOrCode());
        assertEquals("Script params should have 2 entries", 2, script.getParams().size());
        assertEquals("Script param 'factor' should be 2.5", 2.5, script.getParams().get("factor"));
        assertEquals("Script param 'name' should be 'test'", "test", script.getParams().get("name"));
    }

    public void testParseFromProtoRequestWithInlineScriptAndOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("content_type", "application/json");

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    )
                    .putAllOptions(options)
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be painless", "painless", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", script.getIdOrCode());
        assertEquals("Script options should have 1 entry", 1, script.getOptions().size());
        assertEquals(
            "Script option 'content_type' should be 'application/json'",
            "application/json",
            script.getOptions().get("content_type")
        );
    }

    public void testParseFromProtoRequestWithInlineScriptAndInvalidOptions() {
        Map<String, String> options = new HashMap<>();
        options.put("content_type", "application/json");
        options.put("invalid_option", "value");

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    )
                    .putAllOptions(options)
                    .build()
            )
            .build();

        expectThrows(IllegalArgumentException.class, () -> ScriptProtoUtils.parseFromProtoRequest(protoScript));
    }

    public void testParseFromProtoRequestWithStoredScript() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setStored(StoredScriptId.newBuilder().setId("my-stored-script").build())
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be STORED", ScriptType.STORED, script.getType());
        assertNull("Script language should be null for stored scripts", script.getLang());
        assertEquals("Script id should match", "my-stored-script", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
        assertNull("Script options should be null for stored scripts", script.getOptions());
    }

    public void testParseFromProtoRequestWithStoredScriptAndParams() {
        ObjectMap params = ObjectMap.newBuilder()
            .putFields("factor", ObjectMap.Value.newBuilder().setDouble(2.5).build())
            .putFields("name", ObjectMap.Value.newBuilder().setString("test").build())
            .build();

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setStored(StoredScriptId.newBuilder().setId("my-stored-script").setParams(params).build())
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be STORED", ScriptType.STORED, script.getType());
        assertNull("Script language should be null for stored scripts", script.getLang());
        assertEquals("Script id should match", "my-stored-script", script.getIdOrCode());
        assertEquals("Script params should have 2 entries", 2, script.getParams().size());
        assertEquals("Script param 'factor' should be 2.5", 2.5, script.getParams().get("factor"));
        assertEquals("Script param 'name' should be 'test'", "test", script.getParams().get("name"));
    }

    public void testParseFromProtoRequestWithNoScriptType() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder().build();

        expectThrows(UnsupportedOperationException.class, () -> ScriptProtoUtils.parseFromProtoRequest(protoScript));
    }

    public void testParseScriptLanguageWithExpressionLanguage() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_EXPRESSION)
                    )
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be expression", "expression", script.getLang());
    }

    public void testParseScriptLanguageWithJavaLanguage() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder().setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_JAVA)
                    )
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be java", "java", script.getLang());
    }

    public void testParseScriptLanguageWithMustacheLanguage() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_MUSTACHE)
                    )
                    .build()
            )
            .build();

        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be mustache", "mustache", script.getLang());
    }

    public void testParseScriptLanguageWithUnspecifiedLanguage() {
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInline(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder()
                            .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_UNSPECIFIED)
                    )
                    .build()
            )
            .build();

        assertEquals("uses default language", DEFAULT_SCRIPT_LANG, ScriptProtoUtils.parseFromProtoRequest(protoScript).getLang());
    }
}
