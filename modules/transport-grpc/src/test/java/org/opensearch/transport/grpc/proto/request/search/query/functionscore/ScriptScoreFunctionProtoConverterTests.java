/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search.query.functionscore;

import org.opensearch.index.query.functionscore.ScoreFunctionBuilder;
import org.opensearch.index.query.functionscore.ScriptScoreFunctionBuilder;
import org.opensearch.protobufs.BuiltinScriptLanguage;
import org.opensearch.protobufs.FunctionScoreContainer;
import org.opensearch.protobufs.InlineScript;
import org.opensearch.protobufs.ObjectMap;
import org.opensearch.protobufs.ObjectMap.Value;
import org.opensearch.protobufs.Script;
import org.opensearch.protobufs.ScriptLanguage;
import org.opensearch.protobufs.ScriptScoreFunction;
import org.opensearch.protobufs.StoredScriptId;
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ScriptScoreFunctionProtoConverterTests extends OpenSearchTestCase {

    private ScriptScoreFunctionProtoConverter converter;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        converter = new ScriptScoreFunctionProtoConverter();
    }

    public void testGetHandledFunctionCase() {
        assertEquals(FunctionScoreContainer.FunctionScoreContainerCase.SCRIPT_SCORE, converter.getHandledFunctionCase());
    }

    public void testFromProtoWithInlineScript() {
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['score'].value * params.factor")
            .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS).build())
            .setParams(ObjectMap.newBuilder().putFields("factor", Value.newBuilder().setDouble(2.0).build()).build())
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().setScript(script).build();
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ScriptScoreFunctionBuilder.class));
        ScriptScoreFunctionBuilder scriptFunction = (ScriptScoreFunctionBuilder) result;
        org.opensearch.script.Script openSearchScript = scriptFunction.getScript();
        assertEquals(org.opensearch.script.ScriptType.INLINE, openSearchScript.getType());
        assertEquals("painless", openSearchScript.getLang());
        assertEquals("doc['score'].value * params.factor", openSearchScript.getIdOrCode());
        assertEquals(2.0, openSearchScript.getParams().get("factor"));
    }

    public void testFromProtoWithStoredScript() {
        StoredScriptId storedScriptId = StoredScriptId.newBuilder()
            .setId("my_script_id")
            .setParams(ObjectMap.newBuilder().putFields("factor", Value.newBuilder().setDouble(1.5).build()).build())
            .build();

        Script script = Script.newBuilder().setStored(storedScriptId).build();
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().setScript(script).build();
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).setWeight(2.0f).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ScriptScoreFunctionBuilder.class));
        ScriptScoreFunctionBuilder scriptFunction = (ScriptScoreFunctionBuilder) result;
        org.opensearch.script.Script openSearchScript = scriptFunction.getScript();
        assertEquals(org.opensearch.script.ScriptType.STORED, openSearchScript.getType());
        assertEquals(null, openSearchScript.getLang()); // Stored scripts don't have language in protobuf
        assertEquals("my_script_id", openSearchScript.getIdOrCode());
        assertEquals(1.5, openSearchScript.getParams().get("factor"));
    }

    public void testFromProtoWithMinimalScript() {
        InlineScript inlineScript = InlineScript.newBuilder().setSource("doc['score'].value").build();

        Script script = Script.newBuilder().setInline(inlineScript).build();
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().setScript(script).build();
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).build();

        ScoreFunctionBuilder<?> result = converter.fromProto(container);

        assertThat(result, instanceOf(ScriptScoreFunctionBuilder.class));
        ScriptScoreFunctionBuilder scriptFunction = (ScriptScoreFunctionBuilder) result;
        org.opensearch.script.Script openSearchScript = scriptFunction.getScript();
        assertEquals(org.opensearch.script.ScriptType.INLINE, openSearchScript.getType());
        assertEquals("doc['score'].value", openSearchScript.getIdOrCode());
    }

    public void testFromProtoWithNullContainer() {
        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(null));
        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a ScriptScoreFunction"));
    }

    public void testFromProtoWithWrongFunctionType() {
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder()
            .setWeight(1.0f) // Only weight, no specific function
            .build();

        IllegalArgumentException exception = expectThrows(IllegalArgumentException.class, () -> converter.fromProto(container));
        assertThat(exception.getMessage(), containsString("FunctionScoreContainer must contain a ScriptScoreFunction"));
    }

    public void testFromProtoWithNullScript() {
        // Test that setting null script throws NullPointerException at protobuf level
        NullPointerException exception = expectThrows(
            NullPointerException.class,
            () -> ScriptScoreFunction.newBuilder().setScript((org.opensearch.protobufs.Script) null)
        );
        // This is expected behavior - protobuf doesn't allow null values
    }

    public void testFromProtoWithEmptyScript() {
        Script script = Script.newBuilder().build();
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().setScript(script).build();
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).build();

        UnsupportedOperationException exception = expectThrows(UnsupportedOperationException.class, () -> converter.fromProto(container));
        assertThat(exception.getMessage(), containsString("No valid script type detected"));
    }
}
