/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.search.IndexSearcher;
import org.graalvm.polyglot.Value;
import org.opensearch.script.ScoreScript;
import org.opensearch.search.lookup.SearchLookup;

public class PythonScoreScript {
    private static final Logger logger = LogManager.getLogger();

    public static ScoreScript.Factory newScoreScriptFactory(String code) {
        return new ScoreScript.Factory() {

            @Override
            public boolean isResultDeterministic() {
                return true;
            }

            @Override
            public ScoreScript.LeafFactory newFactory(
                    Map<String, Object> params, SearchLookup lookup, IndexSearcher indexSearcher) {
                return newScoreScript(code, params, lookup, indexSearcher);
            }
        };
    }

    private static ScoreScript.LeafFactory newScoreScript(
            String code,
            Map<String, Object> params,
            SearchLookup lookup,
            IndexSearcher indexSearcher) {
        return new PythonScoreScriptLeafFactory(code, params, lookup, indexSearcher);
    }

    private static class PythonScoreScriptLeafFactory implements ScoreScript.LeafFactory {
        private final String code;
        private final Map<String, Object> params;
        private final SearchLookup lookup;
        private final IndexSearcher indexSearcher;

        private PythonScoreScriptLeafFactory(
                String code,
                Map<String, Object> params,
                SearchLookup lookup,
                IndexSearcher indexSearcher) {
            this.code = code;
            this.params = params;
            this.lookup = lookup;
            this.indexSearcher = indexSearcher;
        }

        @Override
        public boolean needs_score() {
            return true;
        }

        @Override
        public ScoreScript newInstance(LeafReaderContext ctx) throws IOException {
            return new ScoreScript(params, lookup, indexSearcher, ctx) {
                @Override
                public double execute(ExplanationHolder explanation) {
                    if (explanation != null) {
                        explanation.set(
                                "Use user-provided Python expression to calculate the score of the"
                                        + " document");
                    }

                    logger.info(
                            "Code is expression: {}", PythonScriptUtility.isCodeAnExpression(code));
                    //                    if (!PythonScriptUtility.isCodeAnExpression(code)) {
                    //                        //TODO: Convert to ScriptException @see
                    // ExpressionScriptEngine.java#L444
                    //                        throw new GeneralScriptException("Python score script
                    // must be an expression, but got " + code);
                    //                    }

                    Set<String> accessedDocFields =
                            PythonScriptUtility.extractAccessedDocFields(code);
                    Map<String, Object> docParams = new HashMap<>();
                    for (String field : accessedDocFields) {
                        docParams.put(field, getDoc().get(field));
                    }

                    return executePython(code, params, docParams, get_score());
                }
            };
        }

        private static double executePython(
                String code, Map<String, ?> params, Map<String, ?> doc, double score) {
            Value evaluatedVal = ExecutionUtils.executePython(code, params, doc, score);
            if (evaluatedVal == null) {
                return 0;
            }
            return evaluatedVal.asDouble();
        }
    }
}
