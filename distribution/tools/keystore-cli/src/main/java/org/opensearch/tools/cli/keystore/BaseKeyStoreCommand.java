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
import joptsimple.OptionSpec;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;

import java.nio.file.Path;

/**
 * Base settings class for key store commands.
 *
 * @opensearch.internal
 */
public abstract class BaseKeyStoreCommand extends KeyStoreAwareCommand {

    private KeyStoreWrapper keyStore;
    private SecureString keyStorePassword;
    private final boolean keyStoreMustExist;

    /**
     * Option to force operations without prompting for confirmation.
     * When specified, operations proceed without asking for user input.
     */
    protected OptionSpec<Void> forceOption;

    /**
     * Creates a new BaseKeyStoreCommand with the specified description and existence requirement.
     *
     * @param description The description of the command
     * @param keyStoreMustExist If true, the keystore must exist before executing the command.
     *                         If false, a new keystore may be created if none exists.
     */
    public BaseKeyStoreCommand(String description, boolean keyStoreMustExist) {
        super(description);
        this.keyStoreMustExist = keyStoreMustExist;
    }

    /**
     * Executes the keystore command by loading/creating the keystore and handling password management.
     * If the keystore doesn't exist and keyStoreMustExist is false, prompts to create a new one
     * unless the force option is specified.
     *
     * @param terminal The terminal to use for user interaction
     * @param options The command-line options provided
     * @param env The environment settings
     * @throws Exception if there are errors during keystore operations
     * @throws UserException if the keystore is required but doesn't exist
     */
    @Override
    protected final void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        try {
            final Path configFile = env.configDir();
            keyStore = KeyStoreWrapper.load(configFile);
            if (keyStore == null) {
                if (keyStoreMustExist) {
                    throw new UserException(
                        ExitCodes.DATA_ERROR,
                        "OpenSearch keystore not found at ["
                            + KeyStoreWrapper.keystorePath(env.configDir())
                            + "]. Use 'create' command to create one."
                    );
                } else if (options.has(forceOption) == false) {
                    if (terminal.promptYesNo("The opensearch keystore does not exist. Do you want to create it?", false) == false) {
                        terminal.println("Exiting without creating keystore.");
                        return;
                    }
                }
                keyStorePassword = new SecureString(new char[0]);
                keyStore = KeyStoreWrapper.create();
                keyStore.save(configFile, keyStorePassword.getChars());
            } else {
                keyStorePassword = keyStore.hasPassword() ? readPassword(terminal, false) : new SecureString(new char[0]);
                keyStore.decrypt(keyStorePassword.getChars());
            }
            executeCommand(terminal, options, env);
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        } finally {
            if (keyStorePassword != null) {
                keyStorePassword.close();
            }
        }
    }

    /**
     * Gets the current keystore instance.
     *
     * @return The current {@link KeyStoreWrapper} instance being operated on
     */
    protected KeyStoreWrapper getKeyStore() {
        return keyStore;
    }

    /**
     * Gets the password for the current keystore.
     *
     * @return The {@link SecureString} containing the keystore password
     */
    protected SecureString getKeyStorePassword() {
        return keyStorePassword;
    }

    /**
     * Executes the specific keystore command implementation.
     * This is called after the keystore password has been read and the keystore
     * is decrypted and loaded. The keystore and keystore passwords are available using
     * {@link #getKeyStore()} and {@link #getKeyStorePassword()} respectively.
     *
     * @param terminal The terminal to use for user interaction
     * @param options The command line options that were specified
     * @param env The environment configuration
     * @throws Exception if there is an error executing the command
     */
    protected abstract void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception;
}
