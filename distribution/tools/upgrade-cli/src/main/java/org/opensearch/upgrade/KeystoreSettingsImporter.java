/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrade;

import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.common.settings.KeystoreWrapperUtil;
import org.opensearch.common.settings.SecureString;

import java.io.IOException;
import java.io.InputStream;
import java.security.GeneralSecurityException;

/**
 * Imports the secure Keystore settings from an existing elasticsearch installation.
 */
class KeystoreSettingsImporter implements ConfigImporter {
    ImportConfigOptions options;

    KeystoreSettingsImporter(ImportConfigOptions options) {
        this.options = options;
    }

    @Override
    public void doImport(final Terminal terminal) throws UserException {
        terminal.println("Importing keystore settings from elasticsearch...");
        SecureString keyStorePassword = null;
        try {
            final KeyStoreWrapper esKeystore = KeyStoreWrapper.load(options.getEsConfig(), ImportConfigOptions.ES_KEYSTORE_FILENAME);
            if (esKeystore == null) {
                terminal.println("No elasticsearch keystore settings to import.");
                return;
            }
            KeyStoreWrapper openSearchKeystore = KeyStoreWrapper.load(options.getOpenSearchKeystore());
            if (openSearchKeystore == null) {
                openSearchKeystore = KeyStoreWrapper.create();
            }
            keyStorePassword = esKeystore.hasPassword() ? readEsKeystorePassword(terminal) : new SecureString(new char[0]);
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
            openSearchKeystore.save(options.getOpenSearchConfig(), keyStorePassword.getChars());
        } catch (IOException e) {
            throw new UserException(ExitCodes.IO_ERROR, e.getMessage());
        } catch (SecurityException | GeneralSecurityException e) {
            throw new UserException(ExitCodes.DATA_ERROR, e.getMessage());
        } catch (Exception e) {
            throw new UserException(ExitCodes.CANT_CREATE, e.getMessage());
        } finally {
            if (keyStorePassword != null) {
                keyStorePassword.close();
            }
        }
    }

    private SecureString readEsKeystorePassword(Terminal terminal) {
        final char[] passwordArray = terminal.readSecret("Enter password for the elasticsearch keystore : ");
        return new SecureString(passwordArray);
    }
}
