/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.Terminal;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.KeystoreWrapperUtil;
import org.opensearch.core.common.settings.SecureString;

import java.io.InputStream;

/**
 * Imports the secure Keystore settings from an existing elasticsearch installation.
 */
class ImportKeystoreTask implements UpgradeTask {
    private static final String OPENSEARCH_KEYSTORE_FILENAME = "opensearch.keystore";
    private static final String ES_KEYSTORE_FILENAME = "elasticsearch.keystore";

    @Override
    public void accept(final Tuple<TaskInput, Terminal> input) {
        final TaskInput taskInput = input.v1();
        final Terminal terminal = input.v2();
        SecureString keyStorePassword = new SecureString(new char[0]);
        try {
            terminal.println("Importing keystore settings ...");
            final KeyStoreWrapper esKeystore = KeyStoreWrapper.load(taskInput.getEsConfig(), ES_KEYSTORE_FILENAME);
            if (esKeystore == null) {
                terminal.println("No elasticsearch keystore settings to import.");
                return;
            }
            KeyStoreWrapper openSearchKeystore = KeyStoreWrapper.load(
                taskInput.getOpenSearchConfig().resolve(OPENSEARCH_KEYSTORE_FILENAME)
            );
            if (openSearchKeystore == null) {
                openSearchKeystore = KeyStoreWrapper.create();
            }
            if (esKeystore.hasPassword()) {
                final char[] passwordArray = terminal.readSecret("Enter password for the elasticsearch keystore : ");
                keyStorePassword = new SecureString(passwordArray);
            }
            esKeystore.decrypt(keyStorePassword.getChars());
            for (String setting : esKeystore.getSettingNames()) {
                if (setting.equals("keystore.seed")) {
                    continue;
                }
                if (!openSearchKeystore.getSettingNames().contains(setting)) {
                    InputStream settingIS = esKeystore.getFile(setting);
                    byte[] bytes = new byte[settingIS.available()];
                    settingIS.read(bytes);
                    KeystoreWrapperUtil.saveSetting(openSearchKeystore, setting, bytes);
                }
            }
            openSearchKeystore.save(taskInput.getOpenSearchConfig(), keyStorePassword.getChars());
            terminal.println("Success!" + System.lineSeparator());
        } catch (Exception e) {
            throw new RuntimeException("Error importing keystore settings from elasticsearch, " + e);
        } finally {
            keyStorePassword.close();
        }
    }
}
