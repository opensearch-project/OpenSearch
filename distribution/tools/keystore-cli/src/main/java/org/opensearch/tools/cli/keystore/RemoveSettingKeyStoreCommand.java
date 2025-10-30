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
import org.opensearch.common.settings.KeyStoreWrapper;
import org.opensearch.env.Environment;

import java.util.List;

import picocli.CommandLine.Command;
import picocli.CommandLine.Parameters;

/**
 * A subcommand for the keystore CLI to remove a setting.
 */
@Command(name = "remove", description = "Remove one or more settings from the keystore", mixinStandardHelpOptions = true, usageHelpAutoWidth = true)
class RemoveSettingKeyStoreCommand extends BaseKeyStoreCommand {

    @Parameters(arity = "1..*", paramLabel = "setting", description = "Setting names to remove from the keystore")
    private List<String> settings;

    RemoveSettingKeyStoreCommand() {
        super("Remove settings from the keystore", true);
    }

    @Override
    protected void executeCommand(Terminal terminal, Environment env) throws Exception {
        if (settings == null || settings.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove");
        }

        final KeyStoreWrapper keyStore = getKeyStore();
        for (String setting : settings) {
            if (keyStore.getSettingNames().contains(setting) == false) {
                throw new UserException(ExitCodes.CONFIG, "Setting [" + setting + "] does not exist in the keystore.");
            }
            keyStore.remove(setting);
        }
        keyStore.save(env.configDir(), getKeyStorePassword().getChars());
    }
}
