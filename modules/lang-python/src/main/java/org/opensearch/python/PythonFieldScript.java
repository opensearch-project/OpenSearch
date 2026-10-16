/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.io.IOException;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.LeafReaderContext;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.SandboxPolicy;
import org.graalvm.polyglot.Value;
import org.opensearch.script.FieldScript;
import org.opensearch.search.lookup.SearchLookup;

public class PythonFieldScript {
    private static final Logger logger = LogManager.getLogger();

    public static FieldScript.Factory newFieldScriptFactory(String code) {
        return new FieldScript.Factory() {
            @Override
            public boolean isResultDeterministic() {
                return true;
            }

            @Override
            public FieldScript.LeafFactory newFactory(
                    Map<String, Object> params, SearchLookup lookup) {
                return newFieldScript(code, params, lookup);
            }
        };
    }

    private static FieldScript.LeafFactory newFieldScript(
            String code, Map<String, Object> params, SearchLookup lookup) {
        logger.info("Executing python code: {}", code);
        logger.info("Params: {}", params.toString());
        logger.info("Lookup: {}", lookup.toString());
        return new PythonFieldScriptLeafFactory(code, params, lookup);
    }

    private static class PythonFieldScriptLeafFactory implements FieldScript.LeafFactory {
        private final String code;
        private Map<String, Object> params;
        private SearchLookup lookup;

        private PythonFieldScriptLeafFactory(
                String code, Map<String, Object> params, SearchLookup lookup) {
            this.code = code;
            this.params = params;
            this.lookup = lookup;
        }

        @Override
        public FieldScript newInstance(LeafReaderContext ctx) throws IOException {
            return new FieldScript(params, lookup, ctx) {
                @Override
                public Object execute() {
                    runPython(code);
                    return 0.0d;
                }
            };
        }
    }

    private static Void runPython(String code) {
        try (Context context =
                Context.newBuilder("python")
                        .sandbox(SandboxPolicy.TRUSTED)
                        .allowAllAccess(false)
                        .build()) {

            // TODO: check whether code has a field `result`
            context.eval("python", code);
            logger.info("Eval succeeded");
            Value result = context.getBindings("python").getMember("result");

            logger.info("Result {}", result.asInt());
        } catch (Exception e) {
            logger.error("Failed to run python code", e);
        }
        return null;
    }
}
