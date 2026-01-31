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

import org.opensearch.cli.Terminal;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

import picocli.CommandLine.Command;

/**
 * A sub-command for the keystore CLI that enables upgrading the keystore format.
 *
 * @opensearch.internal
 */
@Command(name = "upgrade", description = "Upgrade the keystore format to the latest version", mixinStandardHelpOptions = true, usageHelpAutoWidth = true)
public class UpgradeKeyStoreCommand extends BaseKeyStoreCommand {

    /**
     * Creates a new UpgradeKeyStoreCommand instance.
     * Requires an existing keystore to upgrade its format.
     */
    public UpgradeKeyStoreCommand() {
        super("Upgrade the keystore format", true);
    }

    /**
     * Executes the keystore upgrade command by upgrading the format of an existing keystore.
     * Uses the current keystore and its password to create a new keystore with an upgraded format
     * in the same location.
     *
     * @param terminal The terminal for user interaction and output messages
     * @param env The environment settings containing the configuration directory
     * @throws Exception if there are any errors during the upgrade process
     */
    @Override
    protected void executeCommand(final Terminal terminal, final Environment env) throws Exception {
        KeyStoreWrapper.upgrade(getKeyStore(), env.configDir(), getKeyStorePassword().getChars());
        terminal.println("OpenSearch keystore upgraded successfully.");
    }
}
