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

import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * A subcommand for the keystore CLI which adds a file setting.
 */
@Command(name = "add-file", description = "Add a file setting to the keystore (provide pairs: <setting> <path> ...)", mixinStandardHelpOptions = true, usageHelpAutoWidth = true)
class AddFileKeyStoreCommand extends BaseKeyStoreCommand {

    /**
     * Expect positional arguments in pairs: setting path [setting path]...
     * We collect them raw and validate the pairing ourselves to preserve original behavior/messages.
     */
    @Parameters(arity = "0..*", paramLabel = "setting path", description = "Pairs of <setting> <path> to add")
    private List<String> argPairs = new ArrayList<>();

    AddFileKeyStoreCommand() {
        // BaseKeyStoreCommand already exposes --force/-f and handles keystore creation/prompting.
        super("Add a file setting to the keystore", false);
    }

    @Override
    protected void executeCommand(Terminal terminal, Environment env) throws Exception {
        if (argPairs.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Missing setting name");
        }
        if (argPairs.size() % 2 != 0) {
            throw new UserException(ExitCodes.USAGE, "settings and filenames must come in pairs");
        }

        final KeyStoreWrapper keyStore = getKeyStore();

        for (int i = 0; i < argPairs.size(); i += 2) {
            final String setting = argPairs.get(i);

            if (keyStore.getSettingNames().contains(setting) && !force) {
                if (terminal.promptYesNo("Setting " + setting + " already exists. Overwrite?", false) == false) {
                    terminal.println("Exiting without modifying keystore.");
                    return;
                }
            }

            final Path file = getPath(argPairs.get(i + 1));
            if (Files.exists(file) == false) {
                throw new UserException(ExitCodes.IO_ERROR, "File [" + file.toString() + "] does not exist");
            }

            keyStore.setFile(setting, Files.readAllBytes(file));
        }

        keyStore.save(env.configDir(), getKeyStorePassword().getChars());
    }

    @SuppressForbidden(reason = "file arg for cli")
    private Path getPath(String file) {
        return PathUtils.get(file);
    }
}
