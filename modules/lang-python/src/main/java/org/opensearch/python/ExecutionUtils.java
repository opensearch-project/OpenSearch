/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.python;

import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.graalvm.polyglot.Context;
import org.graalvm.polyglot.HostAccess;
import org.graalvm.polyglot.SandboxPolicy;
import org.graalvm.polyglot.Value;
import org.graalvm.python.embedding.GraalPyResources;

public class ExecutionUtils {
    private static final Logger logger = LogManager.getLogger();

    public static Value executePython(
            String code, Map<String, ?> params, Map<String, ?> doc, Double score) {
        // A working context without capabilities to import packages:
        // Context context = Context.newBuilder("python")
        //            .sandbox(SandboxPolicy.TRUSTED)
        //            .allowHostAccess(HostAccess.ALL).build()
        try (Context context =
                GraalPyResources.contextBuilder()
                        .sandbox(SandboxPolicy.TRUSTED)
                        .allowHostAccess(HostAccess.ALL)
                        // The following 2 options are necessary for importing 3-rd party libraries
                        // that load native libraries
                        .allowExperimentalOptions(true)
                        .option("python.IsolateNativeModules", "true")
                        .build()) {

            if (params != null) {
                logger.debug("Params: {}", params.toString());
                context.getBindings("python").putMember("params", params);
            }
            if (doc != null) {
                logger.debug("Doc: {}", doc.toString());
                context.getBindings("python").putMember("doc", doc);
            }
            if (score != null) {
                logger.debug("Score: {}", score);
                context.getBindings("python").putMember("_score", score);
            }
            return context.eval("python", code);
        } catch (Exception e) {
            logger.error("Failed to run python code", e);
            return null;
        }
    }
}
