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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.bootstrap;

import org.opensearch.Build;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.cli.EnvironmentAwareCommand;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.env.Environment;
import org.opensearch.monitor.jvm.JvmInfo;
import org.opensearch.node.NodeValidationException;

import java.io.IOException;
import java.nio.file.Path;
import java.security.Security;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;

import picocli.CommandLine.Command;
import picocli.CommandLine.Option;
import picocli.CommandLine.Parameters;

/**
 * Starts OpenSearch.
 *
 * @opensearch.internal
 */
@Command(name = "opensearch", description = "Starts OpenSearch", mixinStandardHelpOptions = true, usageHelpAutoWidth = true)
class OpenSearch extends EnvironmentAwareCommand {

    @Option(names = { "-V", "--version" }, description = "Prints OpenSearch version information and exits")
    boolean printVersion;

    @Option(names = { "-d", "--daemonize" }, description = "Starts OpenSearch in the background")
    boolean daemonize;

    @Option(names = { "-p", "--pidfile" }, paramLabel = "PATH", description = "Creates a pid file in the specified path on start")
    Path pidFile;

    @Option(names = { "-q", "--quiet" }, description = "Turns off standard output/error streams logging in console")
    boolean quiet;

    // collect any unexpected positionals so we can reproduce the original error message
    @Parameters(arity = "0..*", hidden = true)
    List<String> positionals = new ArrayList<>();

    // visible for testing
    OpenSearch() {
        // We configure logging later, so disable the default logging setup by passing a no-op beforeMain.
        super("Starts OpenSearch", () -> {});
    }

    /**
     * Main entry point for starting OpenSearch.
     */
    @SuppressWarnings("removal")
    public static void main(final String[] args) throws Exception {
        overrideDnsCachePolicyProperties();

        LogConfigurator.registerErrorListener();
        final OpenSearch opensearch = new OpenSearch();
        int status = main(args, opensearch, Terminal.DEFAULT);
        if (status != ExitCodes.OK) {
            final String basePath = System.getProperty("opensearch.logs.base_path");
            // It's possible to fail before logging has been configured, in which case there's no point
            // suggesting that the user look in the log file.
            if (basePath != null) {
                Terminal.DEFAULT.errorPrintln(
                    "ERROR: OpenSearch did not exit normally - check the logs at "
                        + basePath
                        + System.getProperty("file.separator")
                        + System.getProperty("opensearch.logs.cluster_name")
                        + ".log"
                );
            }
            exit(status);
        }
    }

    private static void overrideDnsCachePolicyProperties() {
        for (final String property : new String[] { "networkaddress.cache.ttl", "networkaddress.cache.negative.ttl" }) {
            final String overrideProperty = "opensearch." + property;
            final String overrideValue = System.getProperty(overrideProperty);
            if (overrideValue != null) {
                try {
                    // round-trip the property to an integer and back to a string to ensure that it parses properly
                    Security.setProperty(property, Integer.toString(Integer.valueOf(overrideValue)));
                } catch (final NumberFormatException e) {
                    throw new IllegalArgumentException("failed to parse [" + overrideProperty + "] with value [" + overrideValue + "]", e);
                }
            }
        }
    }

    static int main(final String[] args, final OpenSearch opensearch, final Terminal terminal) throws Exception {
        return opensearch.main(args, terminal);
    }

    @Override
    protected void execute(final Terminal terminal, final Environment env) throws UserException {
        if (positionals.isEmpty() == false) {
            throw new UserException(ExitCodes.USAGE, "Positional arguments not allowed, found " + positionals);
        }

        if (printVersion) {
            final String versionOutput = String.format(
                Locale.ROOT,
                "Version: %s, Build: %s/%s/%s, JVM: %s",
                Build.CURRENT.getQualifiedVersion(),
                Build.CURRENT.type().displayName(),
                Build.CURRENT.hash(),
                Build.CURRENT.date(),
                JvmInfo.jvmInfo().version()
            );
            terminal.println(versionOutput);
            return;
        }

        // a misconfigured java.io.tmpdir can cause hard-to-diagnose problems later, so reject it immediately
        try {
            env.validateTmpDir();
        } catch (IOException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }

        try {
            init(daemonize, pidFile, quiet, env);
        } catch (NodeValidationException e) {
            throw new UserException(ExitCodes.CONFIG, e.getMessage());
        }
    }

    void init(final boolean daemonize, final Path pidFile, final boolean quiet, final Environment initialEnv)
        throws NodeValidationException, UserException {
        try {
            Bootstrap.init(!daemonize, pidFile, quiet, initialEnv);
        } catch (BootstrapException | RuntimeException e) {
            // format exceptions to the console in a special way
            // to avoid 2MB stacktraces from guice, etc.
            throw new StartupException(e);
        }
    }

    /**
     * Required method that's called by Apache Commons procrun when
     * running as a service on Windows, when the service is stopped.
     * <p>
     * http://commons.apache.org/proper/commons-daemon/procrun.html
     * <p>
     * NOTE: If this method is renamed and/or moved, make sure to
     * update opensearch-service.bat!
     */
    static void close(String[] args) throws IOException {
        Bootstrap.stop();
    }
}
