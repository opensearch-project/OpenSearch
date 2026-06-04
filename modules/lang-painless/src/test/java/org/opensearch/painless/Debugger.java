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

import org.opensearch.painless.action.PainlessExecuteAction.PainlessTestScript;
import org.opensearch.painless.lookup.PainlessLookup;
import org.opensearch.painless.lookup.PainlessLookupBuilder;
import org.opensearch.painless.spi.Allowlist;
import org.objectweb.asm.util.Textifier;

import java.io.PrintWriter;
import java.io.StringWriter;

/** quick and dirty tools for debugging */
final class Debugger {

    /** compiles source to bytecode, and returns debugging output */
    static String toString(final String source) {
        return toString(PainlessTestScript.class, source, new CompilerSettings());
    }

    private static final PainlessLookup LOOKUP = PainlessLookupBuilder.buildFromAllowlists(Allowlist.BASE_ALLOWLISTS);

    /** compiles to bytecode, and returns debugging output */
    static String toString(Class<?> iface, String source, CompilerSettings settings) {
        StringWriter output = new StringWriter();
        PrintWriter outputWriter = new PrintWriter(output);
        Textifier textifier = new Textifier();
        try {
            new Compiler(iface, null, null, LOOKUP).compile("<debugging>", source, settings, textifier);
        } catch (RuntimeException e) {
            textifier.print(outputWriter);
            e.addSuppressed(new Exception("current bytecode: \n" + output));
            throw e;
        }

        textifier.print(outputWriter);
        return output.toString();
    }
}
