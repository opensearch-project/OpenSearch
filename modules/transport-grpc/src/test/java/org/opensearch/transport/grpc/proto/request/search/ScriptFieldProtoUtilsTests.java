/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.ScriptField;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.StoredScriptId;
import org.opensearch.script.Script;
import org.opensearch.script.ScriptType;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ScriptFieldProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithInlineScript() throws IOException {
        // Create a protobuf ScriptField with inline script
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['field'].value * 2")
            .setLang(
                ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).setIgnoreFailure(true).build();

        // Call the method under test
        SearchSourceBuilder.ScriptField result = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_script_field",
            scriptField
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", result);
        assertEquals("Field name should match", "test_script_field", result.fieldName());
        assertTrue("IgnoreFailure should be true", result.ignoreFailure());

        Script resultScript = result.script();
        assertNotNull("Script should not be null", resultScript);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, resultScript.getType());
        assertEquals("Script language should be painless", "painless", resultScript.getLang());
        assertEquals("Script source should match", "doc['field'].value * 2", resultScript.getIdOrCode());
        assertEquals("Script params should be empty", Collections.emptyMap(), resultScript.getParams());
    }

    public void testFromProtoWithStoredScript() throws IOException {
        // Create a protobuf ScriptField with stored script
        StoredScriptId storedScriptId = StoredScriptId.newBuilder().setId("my_stored_script").build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setStored(storedScriptId).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).build();

        // Call the method under test
        SearchSourceBuilder.ScriptField result = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_stored_script",
            scriptField
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", result);
        assertEquals("Field name should match", "test_stored_script", result.fieldName());
        assertFalse("IgnoreFailure should be false by default", result.ignoreFailure());

        Script resultScript = result.script();
        assertNotNull("Script should not be null", resultScript);
        assertEquals("Script type should be STORED", ScriptType.STORED, resultScript.getType());
        assertEquals("Script id should match", "my_stored_script", resultScript.getIdOrCode());
        assertEquals("Script params should be empty", Collections.emptyMap(), resultScript.getParams());
    }

    public void testFromProtoWithScriptParams() throws IOException {
        // Create a map of script parameters
        Map<String, String> paramsMap = new HashMap<>();
        paramsMap.put("factor", "2");
        paramsMap.put("field", "price");

        // Create ObjectMap for script parameters
        ObjectMap.Builder objectMapBuilder = ObjectMap.newBuilder();
        for (Map.Entry<String, String> entry : paramsMap.entrySet()) {
            objectMapBuilder.putFields(entry.getKey(), ObjectMap.Value.newBuilder().setString(entry.getValue()).build());
        }

        // Create a protobuf ScriptField with inline script and parameters
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc[params.field].value * params.factor")
            .setLang(ScriptLanguage.newBuilder().setCustom("painless").build())
            .setParams(objectMapBuilder.build())
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).build();

        // Call the method under test
        SearchSourceBuilder.ScriptField result = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_script_with_params",
            scriptField
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", result);
        assertEquals("Field name should match", "test_script_with_params", result.fieldName());

        Script resultScript = result.script();
        assertNotNull("Script should not be null", resultScript);
        assertEquals("Script type should be INLINE", ScriptType.INLINE, resultScript.getType());
        assertEquals("Script language should be painless", "painless", resultScript.getLang());
        assertEquals("Script source should match", "doc[params.field].value * params.factor", resultScript.getIdOrCode());

        Map<String, Object> expectedParams = new HashMap<>();
        expectedParams.put("factor", "2");
        expectedParams.put("field", "price");
        assertEquals("Script params should match", expectedParams, resultScript.getParams());
    }

    public void testFromProtoWithCustomLanguage() throws IOException {
        // Create a protobuf ScriptField with custom language
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("custom script code")
            .setLang(ScriptLanguage.newBuilder().setCustom("mylang").build())
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).build();

        // Call the method under test
        SearchSourceBuilder.ScriptField result = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_custom_lang",
            scriptField
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", result);

        Script resultScript = result.script();
        assertNotNull("Script should not be null", resultScript);
        assertEquals("Script language should match custom language", "mylang", resultScript.getLang());
    }

    public void testFromProtoWithScriptOptions() throws IOException {
        // Create a map of script options
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("content_type", "application/json");

        // Create a protobuf ScriptField with inline script and options
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['field'].value")
            .setLang(
                ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .putAllOptions(optionsMap)
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).build();

        // Call the method under test
        SearchSourceBuilder.ScriptField result = SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto(
            "test_script_options",
            scriptField
        );

        // Verify the result
        assertNotNull("ScriptField should not be null", result);

        Script resultScript = result.script();
        assertNotNull("Script should not be null", resultScript);
        assertEquals("Script options should match", optionsMap, resultScript.getOptions());
    }

    public void testFromProtoWithInvalidScriptOptions() throws IOException {
        // Create a map of invalid script options (more than just content_type)
        Map<String, String> optionsMap = new HashMap<>();
        optionsMap.put("content_type", "application/json");
        optionsMap.put("invalid_option", "value");

        // Create a protobuf ScriptField with inline script and invalid options
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['field'].value")
            .setLang(
                ScriptLanguage.newBuilder()
                    .setBuiltin(org.opensearch.protobufs.BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS)
                    .build()
            )
            .putAllOptions(optionsMap)
            .build();

        org.opensearch.protobufs.Script script = org.opensearch.protobufs.Script.newBuilder().setInline(inlineScript).build();

        ScriptField scriptField = ScriptField.newBuilder().setScript(script).build();

        // Call the method under test, should throw IllegalArgumentException
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> SearchSourceBuilderProtoUtils.ScriptFieldProtoUtils.fromProto("test_invalid_options", scriptField)
        );

        assertTrue(
            "Exception message should mention illegal compiler options",
            exception.getMessage().contains("illegal compiler options")
        );
    }
}
