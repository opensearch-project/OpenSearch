/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.painless;

import org.opensearch.common.settings.Settings;
import org.opensearch.painless.action.PainlessExecuteAction;
import org.opensearch.painless.spi.Allowlist;
import org.opensearch.painless.spi.AllowlistLoader;
import org.opensearch.script.ScoreScript;
import org.opensearch.script.ScriptContext;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class LateInteractionScoreScriptTests extends ScriptTestCase {

    private static final PainlessScriptEngine SCORE_SCRIPT_ENGINE;

    static {
        Map<ScriptContext<?>, List<Allowlist>> contexts = new HashMap<>();
        List<Allowlist> allowlists = new ArrayList<>(Allowlist.BASE_ALLOWLISTS);
        allowlists.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.opensearch.score.txt"));
        allowlists.add(AllowlistLoader.loadFromResourceFiles(Allowlist.class, "org.opensearch.painless.vector_functions.txt"));
        contexts.put(ScoreScript.CONTEXT, allowlists);
        contexts.put(PainlessExecuteAction.PainlessTestScript.CONTEXT, allowlists);
        SCORE_SCRIPT_ENGINE = new PainlessScriptEngine(Settings.EMPTY, contexts);
    }

    @Override
    protected PainlessScriptEngine getEngine() {
        return SCORE_SCRIPT_ENGINE;
    }

    public void testLateInteractionScoreInScript() {
        String script = "lateInteractionScore(params.query_vector, 'my_vector', params._source)";

        // Create query vectors
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv1 = new ArrayList<>();
        qv1.add(0.1);
        qv1.add(0.2);
        queryVectors.add(qv1);

        // Create document vectors
        List<List<Double>> docVectors = new ArrayList<>();
        List<Double> dv1 = new ArrayList<>();
        dv1.add(0.3);
        dv1.add(0.4);
        docVectors.add(dv1);

        // Create document source
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", docVectors);

        // Create parameters
        Map<String, Object> params = new HashMap<>();
        params.put("query_vector", queryVectors);
        params.put("_source", doc);

        // Expected result: 0.1*0.3 + 0.2*0.4 = 0.11
        double expected = 0.11;

        // Use PainlessTestScript context instead of ScoreScript for testing
        PainlessExecuteAction.PainlessTestScript.Factory factory = getEngine().compile(
            "test",
            script,
            PainlessExecuteAction.PainlessTestScript.CONTEXT,
            Collections.emptyMap()
        );

        PainlessExecuteAction.PainlessTestScript testScript = factory.newInstance(params);
        double result = ((Number) testScript.execute()).doubleValue();

        assertEquals(expected, result, 0.001);
    }

    public void testLateInteractionScoreWithInlineScript() {
        String script = """
                if (params.query_vector == null) {
                  return 0.0;
                }

                double totalMaxSim = 0.0;
                def queryVectors = params.query_vector;

                for (int i = 0; i < queryVectors.length; i++) {
                  def q_vec = queryVectors[i];

                  double maxDocTokenSim = 0.0;

                  if (params._source.my_vector == null || params._source.my_vector.length == 0) {
                    continue;
                  }

                  for (int j = 0; j < params._source.my_vector.length; j++) {
                    def doc_token_vec = params._source.my_vector[j];

                    double currentSim = 0.0;
                    if (q_vec.length == doc_token_vec.length) {
                      for (int k = 0; k < q_vec.length; k++) {
                        currentSim += q_vec[k] * doc_token_vec[k];
                      }
                    } else {
                      currentSim = 0.0;
                    }

                    if (currentSim > maxDocTokenSim) {
                      maxDocTokenSim = currentSim;
                    }
                  }
                  totalMaxSim += maxDocTokenSim;
                }
                return totalMaxSim;
            """;

        // Create query vectors
        List<List<Double>> queryVectors = new ArrayList<>();
        List<Double> qv1 = new ArrayList<>();
        qv1.add(0.1);
        qv1.add(0.2);
        queryVectors.add(qv1);

        // Create document vectors
        List<List<Double>> docVectors = new ArrayList<>();
        List<Double> dv1 = new ArrayList<>();
        dv1.add(0.3);
        dv1.add(0.4);
        docVectors.add(dv1);

        // Create document source
        Map<String, Object> doc = new HashMap<>();
        doc.put("my_vector", docVectors);

        // Create parameters
        Map<String, Object> params = new HashMap<>();
        params.put("query_vector", queryVectors);
        params.put("_source", doc);

        // Expected result: 0.1*0.3 + 0.2*0.4 = 0.11
        double expected = 0.11;

        // Use PainlessTestScript context instead of ScoreScript for testing
        PainlessExecuteAction.PainlessTestScript.Factory factory = getEngine().compile(
            "test",
            script,
            PainlessExecuteAction.PainlessTestScript.CONTEXT,
            Collections.emptyMap()
        );

        PainlessExecuteAction.PainlessTestScript testScript = factory.newInstance(params);
        double result = ((Number) testScript.execute()).doubleValue();

        assertEquals(expected, result, 0.001);
    }
}
