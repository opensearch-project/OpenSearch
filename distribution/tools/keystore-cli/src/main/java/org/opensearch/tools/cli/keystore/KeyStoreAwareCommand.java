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
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.cli.EnvironmentAwareCommand;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;

/**
 * An {@link EnvironmentAwareCommand} that needs to access the opensearch keystore, possibly
 * decrypting it if it is password protected.
 *
 * @opensearch.internal
 */
public abstract class KeyStoreAwareCommand extends EnvironmentAwareCommand {

    /**
     * Creates a new KeyStoreAwareCommand with the given description.
     *
     * @param description A description of the command's purpose and functionality
     */
    public KeyStoreAwareCommand(String description) {
        super(description);
    }

    /**
     * Reads the keystore password from the {@link Terminal}, prompting for verification where applicable and returns it as a
     * {@link SecureString}.
     *
     * @param terminal         the terminal to use for user inputs
     * @param withVerification whether the user should be prompted for password verification
     * @return a SecureString with the password the user entered
     * @throws UserException If the user is prompted for verification and enters a different password
     */
    protected static SecureString readPassword(Terminal terminal, boolean withVerification) throws UserException {
        final char[] passwordArray;
        if (withVerification) {
            passwordArray = terminal.readSecret(
                "Enter new password for the opensearch keystore (empty for no password): ",
                EnvironmentAwareCommand.MAX_PASSPHRASE_LENGTH
            );
            char[] passwordVerification = terminal.readSecret("Enter same password again: ", EnvironmentAwareCommand.MAX_PASSPHRASE_LENGTH);
            if (Arrays.equals(passwordArray, passwordVerification) == false) {
                throw new UserException(ExitCodes.DATA_ERROR, "Passwords are not equal, exiting.");
            }
            Arrays.fill(passwordVerification, '\u0000');
        } else {
            passwordArray = terminal.readSecret("Enter password for the opensearch keystore : ");
        }
        return new SecureString(passwordArray);
    }

    /**
     * Decrypts the provided keystore using a password obtained from the terminal.
     * If the keystore is password-protected, prompts the user to enter the password.
     * If not password-protected, uses an empty password.
     *
     * @param keyStore The keystore to decrypt
     * @param terminal The terminal to use for password input
     * @throws UserException If there is an error with the provided password
     * @throws GeneralSecurityException If there is an error during decryption
     * @throws IOException If there is an error reading from the terminal
     */
    protected static void decryptKeyStore(KeyStoreWrapper keyStore, Terminal terminal) throws UserException, GeneralSecurityException,
        IOException {
        try (SecureString keystorePassword = keyStore.hasPassword() ? readPassword(terminal, false) : new SecureString(new char[0])) {
            keyStore.decrypt(keystorePassword.getChars());
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        }
    }

    /**
     * Executes the keystore command with the given parameters.
     *
     * @param terminal The terminal to use for user interaction
     * @param options The command-line options provided
     * @param env The environment settings
     * @throws Exception if there are any errors during execution
     */
    protected abstract void execute(Terminal terminal, OptionSet options, Environment env) throws Exception;
}
