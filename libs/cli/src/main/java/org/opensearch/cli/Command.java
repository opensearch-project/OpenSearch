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

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;

import picocli.CommandLine;
import picocli.CommandLine.Option;

/**
 * An action to execute within a cli.
 *
 * Subclasses should annotate themselves with {@code @CommandLine.Command}
 * and declare their own {@code @Option}/{@code @Parameters} fields.
 * This base provides common flags (-h/--help, -s/--silent, -v/--verbose),
 * help printing, a shutdown hook, and a stable main() entrypoint.
 */
public abstract class Command implements Closeable {

    /** A description of the command, used in the help output. */
    protected final String description;

    private final Runnable beforeMain;

    /** Common options inherited by subclasses */
    @Option(names = { "-h", "--help" }, usageHelp = true, description = "Show help")
    protected boolean help;

    @Option(names = { "-s", "--silent" }, description = "Show minimal output")
    protected boolean silent;

    @Option(names = { "-v", "--verbose" }, description = "Show verbose output", negatable = false)
    protected boolean verbose;

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked.
     *
     * @param description the command description
     * @param beforeMain  the before-main runnable
     */
    public Command(final String description, final Runnable beforeMain) {
        this.description = description;
        this.beforeMain = beforeMain;
    }

    private Thread shutdownHookThread;

    /** Parses options for this command from args and executes it with error handling. */
    public final int main(String[] args, Terminal terminal) throws Exception {
        if (addShutdownHook()) {
            shutdownHookThread = new Thread(() -> {
                try {
                    this.close();
                } catch (final IOException e) {
                    try (StringWriter sw = new StringWriter(); PrintWriter pw = new PrintWriter(sw)) {
                        e.printStackTrace(pw);
                        terminal.errorPrintln(sw.toString());
                    } catch (final IOException impossible) {
                        // StringWriter#close declares a checked IOException from the Closeable interface but the Javadocs for StringWriter
                        // say that an exception here is impossible
                        throw new AssertionError(impossible);
                    }
                }
            });
            Runtime.getRuntime().addShutdownHook(shutdownHookThread);
        }

        beforeMain.run();

        try {
            mainWithoutErrorHandling(args, terminal);
        } catch (CommandLine.ParameterException e) {
            // print help to stderr on parse/validation exceptions
            printHelp(terminal, true, e.getCommandLine());
            terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
            return ExitCodes.USAGE;
        } catch (UserException e) {
            if (e.exitCode == ExitCodes.USAGE) {
                // try to show usage on user error
                printHelp(terminal, true, null);
            }
            if (e.getMessage() != null) {
                terminal.errorPrintln(Terminal.Verbosity.SILENT, "ERROR: " + e.getMessage());
            }
            return e.exitCode;
        }
        return ExitCodes.OK;
    }

    /**
     * Executes the command, but lets all errors bubble up.
     */
    protected void mainWithoutErrorHandling(String[] args, Terminal terminal) throws Exception {
        CommandLine cmd = new CommandLine(this);
        // Route picocli usage to our Terminal writers
        cmd.setOut(new PrintWriter(terminal.getWriter(), true));
        cmd.setErr(new PrintWriter(terminal.getErrorWriter(), true));

        // Parse the args (this populates @Option/@Parameters fields on 'this')
        CommandLine.ParseResult result = cmd.parseArgs(args);

        // If help was requested via -h/--help, print help and return
        if (cmd.isUsageHelpRequested()) {
            printHelp(terminal, false, cmd);
            return;
        }

        // Set terminal verbosity
        if (silent) {
            terminal.setVerbosity(Terminal.Verbosity.SILENT);
        } else if (verbose) {
            terminal.setVerbosity(Terminal.Verbosity.VERBOSE);
        } else {
            terminal.setVerbosity(Terminal.Verbosity.NORMAL);
        }

        // Let subclasses run
        execute(terminal);
    }

    /** Prints a help message for the command to the terminal. */
    private void printHelp(Terminal terminal, boolean toStdError, CommandLine cmd) throws IOException {
        if (toStdError) {
            terminal.errorPrintln(description);
            terminal.errorPrintln("");
            if (cmd != null) {
                cmd.usage(new PrintWriter(terminal.getErrorWriter(), true));
            }
        } else {
            terminal.println(description);
            terminal.println("");
            printAdditionalHelp(terminal);
            if (cmd != null) {
                cmd.usage(new PrintWriter(terminal.getWriter(), true));
            }
        }
    }

    /** Prints additional help information, specific to the command */
    protected void printAdditionalHelp(Terminal terminal) {}

    @SuppressForbidden(reason = "Allowed to exit explicitly from #main()")
    protected static void exit(int status) {
        System.exit(status);
    }

    /**
     * Executes this command.
     *
     * Any runtime user errors (like an input file that does not exist), should throw a {@link UserException}.
     */
    protected abstract void execute(Terminal terminal) throws Exception;

    /**
     * Return whether or not to install the shutdown hook to cleanup resources on exit. This method should only be overridden in test
     * classes.
     *
     * @return whether or not to install the shutdown hook
     */
    protected boolean addShutdownHook() {
        return true;
    }

    /** Gets the shutdown hook thread if it exists **/
    Thread getShutdownHookThread() {
        return shutdownHookThread;
    }

    @Override
    public void close() throws IOException {
        // default no-op
    }
}
