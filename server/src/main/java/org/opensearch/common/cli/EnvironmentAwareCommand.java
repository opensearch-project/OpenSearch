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

package org.opensearch.common.cli;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import joptsimple.util.KeyValuePair;
import org.opensearch.cli.Command;
import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.InternalSettingsPreparer;

import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;

/**
 * A cli command which requires an {@link org.opensearch.env.Environment} to use current paths and settings.
 *
 * @opensearch.internal
 */
public abstract class EnvironmentAwareCommand extends Command {

    public static final int MAX_PASSPHRASE_LENGTH = 128;

    private final OptionSpec<KeyValuePair> settingOption;

    /**
     * Construct the command with the specified command description. This command will have logging configured without reading OpenSearch
     * configuration files.
     *
     * @param description the command description
     */
    public EnvironmentAwareCommand(final String description) {
        this(description, CommandLoggingConfigurator::configureLoggingWithoutConfig);
    }

    /**
     * Construct the command with the specified command description and runnable to execute before main is invoked. Commands constructed
     * with this constructor must take ownership of configuring logging.
     *
     * @param description the command description
     * @param beforeMain the before-main runnable
     */
    public EnvironmentAwareCommand(final String description, final Runnable beforeMain) {
        super(description, beforeMain);
        this.settingOption = parser.accepts("E", "Configure a setting").withRequiredArg().ofType(KeyValuePair.class);
    }

    @Override
    protected void execute(Terminal terminal, OptionSet options) throws Exception {
        final Map<String, String> settings = new HashMap<>();
        for (final KeyValuePair kvp : settingOption.values(options)) {
            if (kvp.value.isEmpty()) {
                throw new UserException(ExitCodes.USAGE, "setting [" + kvp.key + "] must not be empty");
            }
            if (settings.containsKey(kvp.key)) {
                final String message = String.format(
                    Locale.ROOT,
                    "setting [%s] already set, saw [%s] and [%s]",
                    kvp.key,
                    settings.get(kvp.key),
                    kvp.value
                );
                throw new UserException(ExitCodes.USAGE, message);
            }
            settings.put(kvp.key, kvp.value);
        }

        putSystemPropertyIfSettingIsMissing(settings, "path.data", "opensearch.path.data");
        putSystemPropertyIfSettingIsMissing(settings, "path.home", "opensearch.path.home");
        putSystemPropertyIfSettingIsMissing(settings, "path.logs", "opensearch.path.logs");

        execute(terminal, options, createEnv(settings));
    }

    /** Create an {@link Environment} for the command to use. Overrideable for tests. */
    protected Environment createEnv(final Map<String, String> settings) throws UserException {
        return createEnv(Settings.EMPTY, settings);
    }

    /** Create an {@link Environment} for the command to use. Overrideable for tests. */
    protected final Environment createEnv(final Settings baseSettings, final Map<String, String> settings) throws UserException {
        final String esPathConf = System.getProperty("opensearch.path.conf");
        if (esPathConf == null) {
            throw new UserException(ExitCodes.CONFIG, "the system property [opensearch.path.conf] must be set");
        }
        return InternalSettingsPreparer.prepareEnvironment(
            baseSettings,
            settings,
            getConfigPath(esPathConf),
            // HOSTNAME is set by opensearch-env and opensearch-env.bat so it is always available
            () -> System.getenv("HOSTNAME")
        );
    }

    @SuppressForbidden(reason = "need path to construct environment")
    private static Path getConfigPath(final String pathConf) {
        return Paths.get(pathConf);
    }

    /** Ensure the given setting exists, reading it from system properties if not already set. */
    private static void putSystemPropertyIfSettingIsMissing(final Map<String, String> settings, final String setting, final String key) {
        final String value = System.getProperty(key);
        if (value != null) {
            if (settings.containsKey(setting)) {
                final String message = String.format(
                    Locale.ROOT,
                    "duplicate setting [%s] found via command-line [%s] and system property [%s]",
                    setting,
                    settings.get(setting),
                    value
                );
                throw new IllegalArgumentException(message);
            } else {
                settings.put(setting, value);
            }
        }
    }

    /** Execute the command with the initialized {@link Environment}. */
    protected abstract void execute(Terminal terminal, OptionSet options, Environment env) throws Exception;

}
