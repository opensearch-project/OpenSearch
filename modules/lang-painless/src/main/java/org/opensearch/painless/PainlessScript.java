/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.painless;

import org.opensearch.script.ScriptException;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.Map;

/**
 * Abstract superclass on top of which all Painless scripts are built.
 */
public interface PainlessScript {

    /**
     * @return The name of the script retrieved from a static variable generated
     * during compilation of a Painless script.
     */
    String getName();

    /**
     * @return The source for a script retrieved from a static variable generated
     * during compilation of a Painless script.
     */
    String getSource();

    /**
     * @return The {@link BitSet} tracking the boundaries for statements necessary
     * for good exception messages.
     */
    BitSet getStatements();

    /**
     * Adds stack trace and other useful information to exceptions thrown
     * from a Painless script.
     * @param originalThrowable The throwable to build an exception around.
     * @return The generated ScriptException.
     */
    default ScriptException convertToScriptException(Throwable originalThrowable, Map<String, List<String>> extraMetadata) {
        final Throwable unwrapped = switch (originalThrowable) {
            case DefBootstrap.WrappedCheckedException w -> w.getCause();
            default -> originalThrowable;
        };
        // create a script stack: this is just the script portion
        List<String> scriptStack = new ArrayList<>();
        ScriptException.Position pos = null;
        for (StackTraceElement element : unwrapped.getStackTrace()) {
            if (WriterConstants.CLASS_NAME.equals(element.getClassName())) {
                // found the script portion
                int originalOffset = element.getLineNumber();
                if (originalOffset == -1) {
                    scriptStack.add("<<< unknown portion of script >>>");
                } else {
                    int offset = --originalOffset; // offset is 1 based, line numbers must be!
                    int startOffset = getPreviousStatement(offset);
                    if (startOffset == -1) {
                        assert false; // should never happen unless we hit exc in ctor prologue...
                        startOffset = 0;
                    }
                    int endOffset = getNextStatement(startOffset);
                    if (endOffset == -1) {
                        endOffset = getSource().length();
                    }
                    // TODO: if this is still too long, truncate and use ellipses
                    String snippet = getSource().substring(startOffset, endOffset);
                    scriptStack.add(snippet);
                    StringBuilder pointer = new StringBuilder();
                    for (int i = startOffset; i < offset; i++) {
                        pointer.append(' ');
                    }
                    pointer.append("^---- HERE");
                    scriptStack.add(pointer.toString());
                    pos = new ScriptException.Position(originalOffset, startOffset, endOffset);
                }
                break;
                // but filter our own internal stacks (e.g. indy bootstrap)
            } else if (!shouldFilter(element)) {
                scriptStack.add(element.toString());
            }
        }
        ScriptException scriptException = new ScriptException(
            "runtime error",
            unwrapped,
            scriptStack,
            getName(),
            PainlessScriptEngine.NAME,
            pos
        );
        for (Map.Entry<String, List<String>> entry : extraMetadata.entrySet()) {
            scriptException.addMetadata(entry.getKey(), entry.getValue());
        }
        return scriptException;
    }

    /** returns true for methods that are part of the runtime */
    default boolean shouldFilter(StackTraceElement element) {
        return element.getClassName().startsWith("org.opensearch.painless.")
            || element.getClassName().startsWith("java.lang.invoke.")
            || element.getClassName().startsWith("sun.invoke.");
    }

    /**
     * Finds the start of the first statement boundary that is on or before {@code offset}. If one is not found, {@code -1} is returned.
     */
    default int getPreviousStatement(int offset) {
        return getStatements().previousSetBit(offset);
    }

    /**
     * Finds the start of the first statement boundary that is after {@code offset}. If one is not found, {@code -1} is returned.
     */
    default int getNextStatement(int offset) {
        return getStatements().nextSetBit(offset + 1);
    }
}
