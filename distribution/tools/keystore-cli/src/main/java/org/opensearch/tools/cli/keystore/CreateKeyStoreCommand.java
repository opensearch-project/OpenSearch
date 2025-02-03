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

package org.opensearch.tools.cli.keystore;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;

/**
 * A sub-command for the keystore cli to create a new keystore.
 */
class CreateKeyStoreCommand extends KeyStoreAwareCommand {

    private final OptionSpec<Void> passwordOption;

    CreateKeyStoreCommand() {
        super("Creates a new opensearch keystore");
        this.passwordOption = parser.acceptsAll(Arrays.asList("p", "password"), "Prompt for password to encrypt the keystore");
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        try (SecureString password = options.has(passwordOption) ? readPassword(terminal, true) : new SecureString(new char[0])) {
            Path keystoreFile = KeyStoreWrapper.keystorePath(env.configDir());
            if (Files.exists(keystoreFile)) {
                if (terminal.promptYesNo("An opensearch keystore already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without creating keystore.");
                    return;
                }
            }
            KeyStoreWrapper keystore = KeyStoreWrapper.create();
            keystore.save(env.configDir(), password.getChars());
            terminal.println("Created opensearch keystore in " + KeyStoreWrapper.keystorePath(env.configDir()));
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.IO_ERROR, "Error creating the opensearch keystore.");
        }
    }
}
