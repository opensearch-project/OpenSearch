/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graalvm.polyglot.Value;
import org.opensearch.script.ScriptFactory;
import org.opensearch.script.TemplateScript;

public class PythonTemplateScript {
    private static final Logger logger = LogManager.getLogger();

    public static TemplateScriptFactory newTemplateScriptFactory(String code) {
        return new TemplateScriptFactory(code);
    }

    public static class TemplateScriptFactory implements TemplateScript.Factory, ScriptFactory {
        private final String code;

        TemplateScriptFactory(String code) {
            this.code = code;
        }

        @Override
        public TemplateScript newInstance(Map<String, Object> params) {
            return new TemplateScript(params) {
                @Override
                public String execute() {
                    logger.debug("Executing template script with code: {}", code);
                    return executePython(code, params);
                }
            };
        }

        @Override
        public boolean isResultDeterministic() {
            return true;
        }

        private static String executePython(String code, Map<String, ?> params) {
            Value result = ExecutionUtils.executePython(code, params, null, null);
            if (result == null) {
                logger.warn("Did not get any result from Python execution");
                return "";
            }
            return result.asString();
        }
    }
}
