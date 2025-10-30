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
 * the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
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

package org.opensearch.cluster.coordination;

import org.opensearch.cli.ExitCodes;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import picocli.CommandLine.Parameters;

/**
 * Removes custom settings
 *
 * @opensearch.internal
 */
public class RemoveSettingsCommand extends OpenSearchNodeCommand {

    static final String SETTINGS_REMOVED_MSG = "Settings were successfully removed from the cluster state";

    static final String CONFIRMATION_MSG = DELIMITER + """

        You should only run this tool if you have incompatible settings in the
        cluster state that prevent the cluster from forming.
        This tool can cause data loss and its use should be your last resort.

        Do you want to proceed?
        """;

    /** Positional arguments: one or more setting names (supports simple globbing) */
    @Parameters(arity = "1..", paramLabel = "SETTING", description = "Persistent cluster setting keys to remove (supports simple globbing).")
    private List<String> settingsToRemove = new ArrayList<>();

    public RemoveSettingsCommand() {
        super("Removes persistent settings from the cluster state");
    }

    @Override
    protected void processNodePaths(final Terminal terminal, final Path[] dataPaths, final int nodeLockId, final Environment env)
        throws IOException, UserException {

        if (settingsToRemove == null || settingsToRemove.isEmpty()) {
            throw new UserException(ExitCodes.USAGE, "Must supply at least one setting to remove");
        }

        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);

        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        final Tuple<Long, ClusterState> termAndClusterState = loadTermAndClusterState(persistedClusterStateService, env);
        final ClusterState oldClusterState = termAndClusterState.v2();
        final Settings oldPersistentSettings = oldClusterState.metadata().persistentSettings();

        terminal.println(Terminal.Verbosity.VERBOSE, "persistent settings: " + oldPersistentSettings);

        final Settings.Builder newPersistentSettingsBuilder = Settings.builder().put(oldPersistentSettings);

        for (final String settingToRemove : settingsToRemove) {
            boolean matched = false;
            for (final String settingKey : oldPersistentSettings.keySet()) {
                if (Regex.simpleMatch(settingToRemove, settingKey)) {
                    newPersistentSettingsBuilder.remove(settingKey);
                    if (matched == false) {
                        terminal.println("The following settings will be removed:");
                    }
                    matched = true;
                    terminal.println(settingKey + ": " + oldPersistentSettings.get(settingKey));
                }
            }
            if (matched == false) {
                throw new UserException(
                    ExitCodes.USAGE,
                    "No persistent cluster settings matching [" + settingToRemove + "] were found on this node"
                );
            }
        }

        final ClusterState newClusterState = ClusterState.builder(oldClusterState)
            .metadata(Metadata.builder(oldClusterState.metadata()).persistentSettings(newPersistentSettingsBuilder.build()).build())
            .build();

        terminal.println(
            Terminal.Verbosity.VERBOSE,
            "[old cluster state = " + oldClusterState + ", new cluster state = " + newClusterState + "]"
        );

        confirm(terminal, CONFIRMATION_MSG);

        try (PersistedClusterStateService.Writer writer = persistedClusterStateService.createWriter()) {
            writer.writeFullStateAndCommit(termAndClusterState.v1(), newClusterState);
        }

        terminal.println(SETTINGS_REMOVED_MSG);
    }
}
