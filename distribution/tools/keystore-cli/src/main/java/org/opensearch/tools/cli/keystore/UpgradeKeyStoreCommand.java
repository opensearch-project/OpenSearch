/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
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
