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
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

/**
 * A sub-command for the keystore CLI that enables upgrading the keystore format.
 *
 * @opensearch.internal
 */
public class UpgradeKeyStoreCommand extends BaseKeyStoreCommand {

    /**
     * Creates a new UpgradeKeyStoreCommand instance.
     * Initializes a command that requires an existing keystore to upgrade its format
     * to the latest version. This command will fail if the keystore doesn't exist.
     */
    UpgradeKeyStoreCommand() {
        super("Upgrade the keystore format", true);
    }

    /**
     * Executes the keystore upgrade command by upgrading the format of an existing keystore.
     * Uses the current keystore and its password to create a new keystore with an upgraded format
     * in the same location.
     *
     * @param terminal The terminal for user interaction and output messages
     * @param options The command-line options provided
     * @param env The environment settings containing the configuration directory
     * @throws Exception if there are any errors during the upgrade process,
     *                  such as IO errors or encryption/decryption issues
     */
    @Override
    protected void executeCommand(final Terminal terminal, final OptionSet options, final Environment env) throws Exception {
        KeyStoreWrapper.upgrade(getKeyStore(), env.configDir(), getKeyStorePassword().getChars());
    }

}
