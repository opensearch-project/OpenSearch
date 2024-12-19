/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.bootstrap;

import org.opensearch.cli.UserException;
import org.opensearch.common.logging.LogConfigurator;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.node.InternalSettingsPreparer;
import org.opensearch.node.Node;
import org.opensearch.node.NodeValidationException;

import java.io.IOException;
import java.nio.file.Paths;
import java.security.NoSuchAlgorithmException;
import java.util.Collections;

/**
 * Quick bootstrap
 */
public class QuickBoostrap {
    /**
     * This method is invoked by {@link OpenSearch#main(String[])} to startup opensearch.
     */
    public static void bootstrap(final String configPath, final String homePath) throws BootstrapException, NodeValidationException,
        UserException {

        // force the class initializer for BootstrapInfo to run before
        // the security manager is installed
        BootstrapInfo.init();

        Settings settings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), homePath).build();
        final Environment environment = InternalSettingsPreparer.prepareEnvironment(
            settings,
            Collections.emptyMap(),
            Paths.get(configPath),
            // HOSTNAME is set by opensearch-env and opensearch-env.bat so it is always available
            () -> System.getenv("HOSTNAME")
        );

        LogConfigurator.setNodeName(Node.NODE_NAME_SETTING.get(environment.settings()));
        try {
            LogConfigurator.configure(environment);
        } catch (IOException e) {
            throw new BootstrapException(e);
        }

        try {
            Security.configure(environment, BootstrapSettings.SECURITY_FILTER_BAD_DEFAULTS_SETTING.get(settings));
        } catch (IOException | NoSuchAlgorithmException e) {
            throw new BootstrapException(e);
        }
    }
}
