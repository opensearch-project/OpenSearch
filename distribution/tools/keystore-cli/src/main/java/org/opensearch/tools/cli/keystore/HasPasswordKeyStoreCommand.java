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

package org.opensearch.tools.cli.keystore;

import joptsimple.OptionSet;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

import java.nio.file.Path;

/**
 * KeyStore command that checks if the keystore exists and is password-protected.
 * Exits with a non-zero status code if the keystore is missing or not password-protected.
 *
 * @opensearch.internal
 */
public class HasPasswordKeyStoreCommand extends KeyStoreAwareCommand {

    static final int NO_PASSWORD_EXIT_CODE = 1;

    /**
     * Creates a new HasPasswordKeyStoreCommand.
     * This command checks for the existence of a password-protected keystore
     * and exits with {@link #NO_PASSWORD_EXIT_CODE} if the keystore is missing
     * or not password-protected.
     */
    HasPasswordKeyStoreCommand() {
        super(
            "Succeeds if the keystore exists and is password-protected, " + "fails with exit code " + NO_PASSWORD_EXIT_CODE + " otherwise."
        );
    }

    /**
     * Executes the password check command by verifying if the keystore exists
     * and is password-protected.
     *
     * @param terminal The terminal for user interaction and output
     * @param options The command-line options provided
     * @param env The environment settings containing configuration directory
     * @throws UserException with {@link #NO_PASSWORD_EXIT_CODE} if the keystore
     *         is missing or not password-protected
     * @throws Exception if there are other errors during execution
     */
    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final Path configFile = env.configDir();
        final KeyStoreWrapper keyStore = KeyStoreWrapper.load(configFile);

        // We handle error printing here so we can respect the "--silent" flag
        // We have to throw an exception to get a nonzero exit code
        if (keyStore == null) {
            terminal.errorPrintln(Terminal.Verbosity.NORMAL, "ERROR: OpenSearch keystore not found");
            throw new UserException(NO_PASSWORD_EXIT_CODE, null);
        }
        if (keyStore.hasPassword() == false) {
            terminal.errorPrintln(Terminal.Verbosity.NORMAL, "ERROR: Keystore is not password-protected");
            throw new UserException(NO_PASSWORD_EXIT_CODE, null);
        }

        terminal.println(Terminal.Verbosity.NORMAL, "Keystore is password-protected");
    }
}
