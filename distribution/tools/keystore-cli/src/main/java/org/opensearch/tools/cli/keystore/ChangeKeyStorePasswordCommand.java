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
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.core.common.settings.SecureString;
import org.opensearch.env.Environment;

/**
 * A sub-command for the keystore cli which changes the password.
 *
 * @opensearch.internal
 */
public class ChangeKeyStorePasswordCommand extends BaseKeyStoreCommand {

    ChangeKeyStorePasswordCommand() {
        super("Changes the password of a keystore", true);
    }

    /**
     * Executes the password change command by prompting for a new password
     * and saving the keystore with the updated password.
     * <p>
     * This implementation will:
     * 1. Prompt for a new password with verification
     * 2. Save the keystore with the new password
     * 3. Display a success message upon completion
     *
     * @param terminal The terminal to use for user interaction
     * @param options The command-line options provided
     * @param env The environment settings containing configuration directory
     * @throws Exception if there are errors during password change
     * @throws UserException if there are security-related errors
     */
    @Override
    protected void executeCommand(Terminal terminal, OptionSet options, Environment env) throws Exception {
        try (SecureString newPassword = readPassword(terminal, true)) {
            final KeyStoreWrapper keyStore = getKeyStore();
            keyStore.save(env.configDir(), newPassword.getChars());
            terminal.println("OpenSearch keystore password changed successfully.");
        } catch (SecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        }
    }
}
