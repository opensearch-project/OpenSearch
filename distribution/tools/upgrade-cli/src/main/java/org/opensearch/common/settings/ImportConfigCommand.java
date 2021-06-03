/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.settings;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.KeyStoreAwareCommand;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.env.Environment;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.util.List;

/**
 * Imports config from an existing Elasticsearch installation.
 */
public class ImportConfigCommand extends KeyStoreAwareCommand {
    private final String ES_KEYSTORE_FILENAME = "elasticsearch.keystore";

    private final OptionSpec<String> arguments;

    public ImportConfigCommand() {
        super("Import config from an existing Elasticsearch installation.");
        arguments = parser.nonOptions("Path to Elasticsearch config directory.");
    }

    @Override
    @SuppressForbidden(reason = "We need to read external es config files")
    protected void execute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        final List<String> argumentValues = arguments.values(options);
        String esConfigPath;
        if (argumentValues.size() == 0) {
            esConfigPath = System.getenv("ES_PATH_CONF");
            if (esConfigPath == null) {
                throw new UserException(
                    ExitCodes.USAGE,
                    "Missing ES_PATH_CONF environment variable, provide path to Elasticsearch config directory."
                );
            }
        } else if (argumentValues.size() == 1) {
            esConfigPath = argumentValues.get(0);
        } else {
            throw new UserException(ExitCodes.USAGE, "Invalid number of arguments.");
        }
        importKeystoreSettings(new File(esConfigPath).toPath(), terminal, env);
        terminal.println("Done!");
    }

    private void importKeystoreSettings(Path esConfigPath, Terminal terminal, Environment env) throws UserException {
        KeyStoreWrapper openSearchKeystore = null;
        KeyStoreWrapper esKeystore = null;
        SecureString keyStorePassword = null;
        try {
            openSearchKeystore = KeyStoreWrapper.load(env.configFile());
            if (openSearchKeystore != null) {
                terminal.println(Terminal.Verbosity.VERBOSE, "OpenSearch keystore file exists, will add settings to it.");
            } else {
                terminal.println(Terminal.Verbosity.VERBOSE, "Creating OpenSearch keystore file.");
                openSearchKeystore = KeyStoreWrapper.create();
            }

            final Path keystoreFile = esConfigPath.resolve(ES_KEYSTORE_FILENAME);
            if (!Files.exists(keystoreFile)) {
                terminal.println("Elasticsearch keystore file not found.");
                return;
            }
            terminal.println("Elasticsearch keystore file found, importing contents.");

            esKeystore = KeyStoreWrapper.load(esConfigPath, ES_KEYSTORE_FILENAME);
            if (esKeystore == null) {
                terminal.errorPrint(Terminal.Verbosity.VERBOSE, "Unable to import keystore contents, please manually import those.");
                return;
            }
            keyStorePassword = esKeystore.hasPassword() ? readEsKeystorePassword(terminal) : new SecureString(new char[0]);
            esKeystore.decrypt(keyStorePassword.getChars());

            for (String setting : esKeystore.getSettingNames()) {
                if (setting.equals("keystore.seed")) {
                    continue;
                }
                if (openSearchKeystore.getSettingNames().contains(setting)) {
                    if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                        terminal.println("Skipping import of setting: " + setting);
                        continue;
                    }
                }
                terminal.println("Importing keystore setting : " + setting);
                InputStream settingIS = esKeystore.getFile(setting);
                byte[] bytes = new byte[settingIS.available()];
                settingIS.read(bytes);
                openSearchKeystore.setFile(setting, bytes);
            }

            openSearchKeystore.save(env.configFile(), keyStorePassword.getChars());
            terminal.println("All keystore settings imported successfully!");
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
