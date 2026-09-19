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

package org.opensearch.env;

import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.coordination.OpenSearchNodeCommand;
import org.opensearch.env.NodeEnvironment.NodePath;
import org.opensearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Locale;

import picocli.CommandLine.Command;

/**
 * Command to override a node version
 *
 * @opensearch.internal
 */
@Command(name = "override-node-version", description = "Overwrite the version stored in this node's data path with the current version to bypass compatibility checks", mixinStandardHelpOptions = true, usageHelpAutoWidth = true)
public class OverrideNodeVersionCommand extends OpenSearchNodeCommand {

    private static final String TOO_NEW_MESSAGE = String.format(Locale.ROOT, """
        %s
        This data path was last written by OpenSearch version [V_NEW] and may no
        longer be compatible with OpenSearch version [V_CUR]. This tool will bypass
        this compatibility check, allowing a version [V_CUR] node to start on this data
        path, but a version [V_CUR] node may not be able to read this data or may read
        it incorrectly leading to data loss.

        You should not use this tool. Instead, continue to use a version [V_NEW] node
        on this data path. If necessary, you can use reindex-from-remote to copy the
        data from here into an older cluster.

        Do you want to proceed?
        """, DELIMITER);

    private static final String TOO_OLD_MESSAGE = String.format(Locale.ROOT, """
        %s
        This data path was last written by OpenSearch version [V_OLD] which may be
        too old to be readable by OpenSearch version [V_CUR]. This tool will bypass
        this compatibility check, allowing a version [V_CUR] node to start on this data
        path, but this version [V_CUR] node may not be able to read this data or may
        read it incorrectly leading to data loss.

        You should not use this tool. Instead, upgrade this data path from [V_OLD] to
        [V_CUR] using one or more intermediate versions of OpenSearch.

        Do you want to proceed?
        """, DELIMITER);

    static final String NO_METADATA_MESSAGE = "no node metadata found, so there is no version to override";
    static final String SUCCESS_MESSAGE = "Successfully overwrote this node's metadata to bypass its version compatibility checks.";

    public OverrideNodeVersionCommand() {
        super(
            "Overwrite the version stored in this node's data path with ["
                + Version.CURRENT
                + "] to bypass the version compatibility checks"
        );
    }

    @Override
    protected void processNodePaths(
        final Terminal terminal,
        final Path[] dataPaths,
        final int nodeLockId,
        final org.opensearch.env.Environment env
    ) throws IOException {

        // Resolve actual node path directories (strip NodePath wrapper)
        final Path[] nodePaths = Arrays.stream(toNodePaths(dataPaths)).map((NodePath p) -> p.path).toArray(Path[]::new);

        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePaths);
        if (nodeMetadata == null) {
            throw new OpenSearchException(NO_METADATA_MESSAGE);
        }

        // If the metadata is already compatible with current version, nothing to do.
        try {
            nodeMetadata.upgradeToCurrentVersion(); // throws IllegalStateException if not supported (i.e., version differs)
            throw new OpenSearchException(
                "found ["
                    + nodeMetadata
                    + "] which is compatible with current version ["
                    + Version.CURRENT
                    + "], so there is no need to override the version checks"
            );
        } catch (IllegalStateException ok) {
            // Expected when version change is needed; proceed with confirmation
        }

        final String msgTemplate = nodeMetadata.nodeVersion().before(Version.CURRENT) ? TOO_OLD_MESSAGE : TOO_NEW_MESSAGE;
        final String confirmMsg = msgTemplate.replace("V_OLD", nodeMetadata.nodeVersion().toString())
            .replace("V_NEW", nodeMetadata.nodeVersion().toString())
            .replace("V_CUR", Version.CURRENT.toString());

        confirm(terminal, confirmMsg);

        PersistedClusterStateService.overrideVersion(Version.CURRENT, dataPaths);
        terminal.println(SUCCESS_MESSAGE);
    }
}
