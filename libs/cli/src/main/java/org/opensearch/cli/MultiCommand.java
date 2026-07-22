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

package org.opensearch.cli;

import org.opensearch.common.util.io.IOUtils;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * A CLI tool made up of multiple subcommands.
 *
 * Behavior:
 *  - leading positional selects the subcommand
 *  - remaining positionals are forwarded to that subcommand
 *  - -E key=value pairs are forwarded to the subcommand as "-Ekey=value"
 *  - prints a "Commands" list in additional help
 */
@picocli.CommandLine.Command(name = "", // the actual name is typically set by the launcher
    mixinStandardHelpOptions = true, usageHelpAutoWidth = true, description = "Run one of the available sub-commands.")
public class MultiCommand extends Command {

    protected final Map<String, Command> subcommands = new LinkedHashMap<>();

    /** First positional: the subcommand name */
    @Parameters(index = "0", arity = "0..1", paramLabel = "command", description = "The subcommand to run")
    private String subcommandName;

    /** Remaining positionals: args for the subcommand */
    @Parameters(index = "1..*", arity = "0..*", paramLabel = "args", description = "Arguments passed to the subcommand")
    private List<String> remainingArgs = new ArrayList<>();

    /** -E key=value settings to forward to the subcommand */
    @Option(names = "-E", paramLabel = "key=value", arity = "1..*", description = "Configure a setting (may be specified multiple times)")
    private List<String> settings = new ArrayList<>();

    /**
     * Construct the multi-command with the specified command description and runnable to execute before main is invoked.
     *
     * @param description the multi-command description
     * @param beforeMain  the before-main runnable
     */
    public MultiCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }
        terminal.println("Commands");
        terminal.println("--------");
        for (Map.Entry<String, Command> sub : subcommands.entrySet()) {
            terminal.println(sub.getKey() + " - " + sub.getValue().description);
        }
        terminal.println("");
    }

    @Override
    protected void execute(Terminal terminal) throws Exception {
        if (subcommands.isEmpty()) {
            throw new IllegalStateException("No subcommands configured");
        }

        if (subcommandName == null || subcommandName.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing command");
        }

        Command sub = subcommands.get(subcommandName);
        if (sub == null) {
            throw new UserException(ExitCodes.USAGE, "Unknown command [" + subcommandName + "]");
        }

        // Rebuild argv for the subcommand:
        // <remainingArgs> plus "-Ekey=value" for each -E occurrence
        List<String> childArgs = new ArrayList<>(remainingArgs);

        for (String kv : settings) {
            if (kv == null || kv.isEmpty() || kv.indexOf('=') < 1) {
                throw new UserException(ExitCodes.USAGE, "Invalid -E setting [" + kv + "], expected key=value");
            }
            childArgs.add("-E" + kv);
        }

        // Delegate to the subcommand, preserving our error-handling contract
        sub.mainWithoutErrorHandling(childArgs.toArray(new String[0]), terminal);
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(subcommands.values());
    }
}
