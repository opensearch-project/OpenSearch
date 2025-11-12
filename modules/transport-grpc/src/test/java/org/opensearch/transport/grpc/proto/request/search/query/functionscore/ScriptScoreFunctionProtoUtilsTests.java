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
import org.opensearch.test.OpenSearchTestCase;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.instanceOf;

public class ScriptScoreFunctionProtoUtilsTests extends OpenSearchTestCase {

    public void testFromProtoWithScript() {
        InlineScript inlineScript = InlineScript.newBuilder()
            .setSource("doc['score'].value * params.factor")
            .setLang(ScriptLanguage.newBuilder().setBuiltin(BuiltinScriptLanguage.BUILTIN_SCRIPT_LANGUAGE_PAINLESS).build())
            .setParams(ObjectMap.newBuilder().putFields("factor", Value.newBuilder().setDouble(2.0).build()).build())
            .build();

        Script script = Script.newBuilder().setInline(inlineScript).build();
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().setScript(script).build();
        FunctionScoreContainer container = FunctionScoreContainer.newBuilder().setScriptScore(scriptScoreFunction).setWeight(1.5f).build();

        ScoreFunctionBuilder<?> result = ScriptScoreFunctionProtoUtils.fromProto(scriptScoreFunction);

        assertThat(result, instanceOf(ScriptScoreFunctionBuilder.class));
        ScriptScoreFunctionBuilder scriptFunction = (ScriptScoreFunctionBuilder) result;
        org.opensearch.script.Script openSearchScript = scriptFunction.getScript();
        assertEquals(org.opensearch.script.ScriptType.INLINE, openSearchScript.getType());
        assertEquals("painless", openSearchScript.getLang());
        assertEquals("doc['score'].value * params.factor", openSearchScript.getIdOrCode());
        assertEquals(2.0, openSearchScript.getParams().get("factor"));
    }

    public void testFromProtoWithNullScriptScoreFunction() {
        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptScoreFunctionProtoUtils.fromProto(null)
        );
        assertThat(exception.getMessage(), containsString("ScriptScoreFunction cannot be null"));
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

        UnsupportedOperationException exception = expectThrows(
            UnsupportedOperationException.class,
            () -> ScriptScoreFunctionProtoUtils.fromProto(scriptScoreFunction)
        );
        assertThat(exception.getMessage(), containsString("No valid script type detected"));
    }

    public void testFromProtoWithoutScript() {
        ScriptScoreFunction scriptScoreFunction = ScriptScoreFunction.newBuilder().build();

        IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> ScriptScoreFunctionProtoUtils.fromProto(scriptScoreFunction)
        );
        assertThat(exception.getMessage(), containsString("ScriptScoreFunction must have a script"));
    }
}
