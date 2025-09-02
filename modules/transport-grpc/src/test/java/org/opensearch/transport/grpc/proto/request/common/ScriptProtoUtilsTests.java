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
import org.opensearch.protobufs.ScriptLanguage.BuiltinScriptLanguage;
import org.opensearch.protobufs.StoredScriptId;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.Map;

import static org.opensearch.script.Script.DEFAULT_SCRIPT_LANG;

public class ScriptProtoUtilsTests extends OpenSearchTestCase {

    public void testParseFromProtoRequestWithInlineScript() {
        // Create a protobuf Script with an inline script
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be painless", "painless", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
    }

    public void testParseFromProtoRequestWithInlineScriptAndCustomLanguage() {
        // Create a protobuf Script with an inline script and custom language
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setStringValue("custom_lang"))
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be custom_lang", "custom_lang", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
    }

    public void testParseFromProtoRequestWithInlineScriptAndParams() {
        // Create a protobuf Script with an inline script and parameters
        ObjectMap params = ObjectMap.newBuilder()
            .putFields("factor", ObjectMap.Value.newBuilder().setDouble(2.5).build())
            .putFields("name", ObjectMap.Value.newBuilder().setString("test").build())
            .build();

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * params.factor")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .setParams(params)
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, script.getType());
        assertEquals("Script language should be painless", "painless", script.getLang());
        assertEquals("Script source should match", "doc['field'].value * params.factor", script.getIdOrCode());
        assertEquals("Script params should have 2 entries", 2, script.getParams().size());
        assertEquals("Script param 'factor' should be 2.5", 2.5, script.getParams().get("factor"));
        assertEquals("Script param 'name' should be 'test'", "test", script.getParams().get("name"));
    }

    public void testParseFromProtoRequestWithInlineScriptAndOptions() {
        // Create a protobuf Script with an inline script and options
        Map<String, String> options = new HashMap<>();
        options.put("content_type", "application/json");

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .putAllOptions(options)
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
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
        // Create a protobuf Script with an inline script and invalid options
        Map<String, String> options = new HashMap<>();
        options.put("content_type", "application/json");
        options.put("invalid_option", "value");

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS))
                    .putAllOptions(options)
                    .build()
            )
            .build();

        // Parse the protobuf Script, should throw IllegalArgumentException
        expectThrows(IllegalArgumentException.class, () -> ScriptProtoUtils.parseFromProtoRequest(protoScript));
    }

    public void testParseFromProtoRequestWithStoredScript() {
        // Create a protobuf Script with a stored script
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setStoredScriptId(StoredScriptId.newBuilder().setId("my-stored-script").build())
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be STORED", ScriptType.STORED, script.getType());
        assertNull("Script language should be null for stored scripts", script.getLang());
        assertEquals("Script id should match", "my-stored-script", script.getIdOrCode());
        assertTrue("Script params should be empty", script.getParams().isEmpty());
        assertNull("Script options should be null for stored scripts", script.getOptions());
    }

    public void testParseFromProtoRequestWithStoredScriptAndParams() {
        // Create a protobuf Script with a stored script and parameters
        ObjectMap params = ObjectMap.newBuilder()
            .putFields("factor", ObjectMap.Value.newBuilder().setDouble(2.5).build())
            .putFields("name", ObjectMap.Value.newBuilder().setString("test").build())
            .build();

        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setStoredScriptId(StoredScriptId.newBuilder().setId("my-stored-script").setParams(params).build())
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script type should be STORED", ScriptType.STORED, script.getType());
        assertNull("Script language should be null for stored scripts", script.getLang());
        assertEquals("Script id should match", "my-stored-script", script.getIdOrCode());
        assertEquals("Script params should have 2 entries", 2, script.getParams().size());
        assertEquals("Script param 'factor' should be 2.5", 2.5, script.getParams().get("factor"));
        assertEquals("Script param 'name' should be 'test'", "test", script.getParams().get("name"));
    }

    public void testParseFromProtoRequestWithNoScriptType() {
        // Create a protobuf Script with no script type
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder().build();

        // Parse the protobuf Script, should throw UnsupportedOperationException
        expectThrows(UnsupportedOperationException.class, () -> ScriptProtoUtils.parseFromProtoRequest(protoScript));
    }

    public void testParseScriptLanguageWithExpressionLanguage() {
        // Create a protobuf Script with expression language
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_EXPRESSION))
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be expression", "expression", script.getLang());
    }

    public void testParseScriptLanguageWithJavaLanguage() {
        // Create a protobuf Script with java language
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_JAVA))
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be java", "java", script.getLang());
    }

    public void testParseScriptLanguageWithMustacheLanguage() {
        // Create a protobuf Script with mustache language
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_MUSTACHE))
                    .build()
            )
            .build();

        // Parse the protobuf Script
        Script script = ScriptProtoUtils.parseFromProtoRequest(protoScript);

        // Verify the result
        assertNotNull("Script should not be null", script);
        assertEquals("Script language should be mustache", "mustache", script.getLang());
    }

    public void testParseScriptLanguageWithUnspecifiedLanguage() {
        // Create a protobuf Script with unspecified language
        org.opensearch.protobufs.Script protoScript = org.opensearch.protobufs.Script.newBuilder()
            .setInlineScript(
                InlineScript.newBuilder()
                    .setSource("doc['field'].value * 2")
                    .setLang(
                        ScriptLanguage.newBuilder().setBuiltinScriptLanguage(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_UNSPECIFIED)
                    )
                    .build()
            )
            .build();

        assertEquals("uses default language", DEFAULT_SCRIPT_LANG, ScriptProtoUtils.parseFromProtoRequest(protoScript).getLang());
    }
}
