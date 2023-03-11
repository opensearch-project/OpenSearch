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

import com.carrotsearch.hppc.cursors.ObjectObjectCursor;
import joptsimple.OptionParser;
import joptsimple.OptionSet;
import org.opensearch.OpenSearchException;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.OpenSearchNodeCommand;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.set.Sets;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.gateway.PersistedClusterStateService;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static org.opensearch.env.NodeEnvironment.CACHE_FOLDER;
import static org.opensearch.env.NodeEnvironment.INDICES_FOLDER;

/**
 * Command to repurpose a node
 *
 * @opensearch.internal
 */
public class NodeRepurposeCommand extends OpenSearchNodeCommand {

    static final String ABORTED_BY_USER_MSG = OpenSearchNodeCommand.ABORTED_BY_USER_MSG;
    static final String FAILED_TO_OBTAIN_NODE_LOCK_MSG = OpenSearchNodeCommand.FAILED_TO_OBTAIN_NODE_LOCK_MSG;
    static final String NO_CLEANUP = "Node has node.data=true and node.search=true -> no clean up necessary";
    static final String NO_DATA_TO_CLEAN_UP_FOUND = "No data to clean-up found";
    static final String NO_SHARD_DATA_TO_CLEAN_UP_FOUND = "No shard data to clean-up found";
    static final String NO_FILE_CACHE_DATA_TO_CLEAN_UP_FOUND = "No file cache to clean-up found";
    private static final int FILE_CACHE_NODE_PATH_LOCATION = 0;

    public NodeRepurposeCommand() {
        super("Repurpose this node to another cluster-manager/data/search role, cleaning up any excess persisted data");
    }

    void testExecute(Terminal terminal, OptionSet options, Environment env) throws Exception {
        execute(terminal, options, env);
    }

    @Override
    protected boolean validateBeforeLock(Terminal terminal, Environment env) {
        Settings settings = env.settings();
        if (DiscoveryNode.isDataNode(settings) && DiscoveryNode.isSearchNode(settings)) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_CLEANUP);
            return false;
        }

        return true;
    }

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, int nodeLockId, OptionSet options, Environment env)
        throws IOException {
        assert DiscoveryNode.isDataNode(env.settings()) == false || DiscoveryNode.isSearchNode(env.settings()) == false;

        boolean repurposeData = DiscoveryNode.isDataNode(env.settings()) == false;
        boolean repurposeSearch = DiscoveryNode.isSearchNode(env.settings()) == false;

        if (DiscoveryNode.isClusterManagerNode(env.settings()) == false) {
            processNoClusterManagerRepurposeNode(terminal, dataPaths, env, repurposeData, repurposeSearch);
        } else {
            processClusterManagerRepurposeNode(terminal, dataPaths, env, repurposeData, repurposeSearch);
        }
    }

    private void processNoClusterManagerRepurposeNode(
        Terminal terminal,
        Path[] dataPaths,
        Environment env,
        boolean repurposeData,
        boolean repurposeSearch
    ) throws IOException {
        NodeEnvironment.NodePath[] nodePaths = toNodePaths(dataPaths);
        NodeEnvironment.NodePath fileCacheNodePath = toNodePaths(dataPaths)[FILE_CACHE_NODE_PATH_LOCATION];
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);
        final Metadata metadata = loadClusterState(terminal, env, persistedClusterStateService).metadata();

        Set<Path> indexPaths = Set.of();
        List<Path> shardDataPaths = List.of();
        Set<Path> fileCachePaths = Set.of();
        List<Path> fileCacheDataPaths = List.of();

        terminal.println(Terminal.Verbosity.VERBOSE, "Collecting index metadata paths");
        List<Path> indexMetadataPaths = NodeEnvironment.collectIndexMetadataPaths(nodePaths);

        if (repurposeData) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
            shardDataPaths = NodeEnvironment.collectShardDataPaths(nodePaths);
            indexPaths = uniqueParentPaths(shardDataPaths, indexMetadataPaths);
        }

        if (repurposeSearch) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Collecting file cache data paths");
            fileCacheDataPaths = NodeEnvironment.collectFileCacheDataPath(fileCacheNodePath);
            fileCachePaths = uniqueParentPaths(fileCacheDataPaths, indexMetadataPaths);
        }

        if (repurposeData && repurposeSearch && fileCacheDataPaths.isEmpty() && indexPaths.isEmpty() && metadata.indices().isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_DATA_TO_CLEAN_UP_FOUND);
            return;
        } else if (repurposeData && !repurposeSearch && indexPaths.isEmpty() && metadata.indices().isEmpty()) {
            terminal.println(Terminal.Verbosity.NORMAL, NO_DATA_TO_CLEAN_UP_FOUND);
            return;
        } else if (!repurposeData && repurposeSearch && fileCacheDataPaths.isEmpty() && metadata.indices().isEmpty()) {
            terminal.println(NO_FILE_CACHE_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        final Set<String> indexUUIDs = Sets.union(
            indexUUIDsFor(fileCachePaths),
            Sets.union(
                indexUUIDsFor(indexPaths),
                StreamSupport.stream(metadata.indices().values().spliterator(), false)
                    .map(imd -> imd.value.getIndexUUID())
                    .collect(Collectors.toSet())
            )
        );

        List<Path> cleanUpPaths = new ArrayList<>(shardDataPaths);
        cleanUpPaths.addAll(fileCacheDataPaths);
        outputVerboseInformation(terminal, cleanUpPaths, indexUUIDs, metadata);
        terminal.println(noClusterManagerMessage(indexUUIDs.size(), cleanUpPaths.size(), indexMetadataPaths.size()));
        outputHowToSeeVerboseInformation(terminal);

        if (repurposeData && repurposeSearch) {
            terminal.println(
                "Node is being re-purposed as no-cluster-manager, no-data and no-search. Clean-up of index data and file cache will be performed."
            );
        } else if (repurposeData) {
            terminal.println("Node is being re-purposed as no-cluster-manager and no-data. Clean-up of index data will be performed.");
        } else if (repurposeSearch) {
            terminal.println(
                "Node is being re-purposed as no-cluster-manager and no-search. Clean-up of file cache and corresponding index metadata will be performed."
            );
        }
        confirm(terminal, "Do you want to proceed?");

        // clean-up all metadata dirs
        MetadataStateFormat.deleteMetaState(dataPaths);
        if (repurposeData) {
            removePaths(terminal, indexPaths); // clean-up shard dirs
            IOUtils.rm(Stream.of(dataPaths).map(path -> path.resolve(INDICES_FOLDER)).toArray(Path[]::new));
        }

        if (repurposeSearch) {
            removePaths(terminal, fileCachePaths); // clean-up file cache dirs
            IOUtils.rm(dataPaths[FILE_CACHE_NODE_PATH_LOCATION].resolve(CACHE_FOLDER));
        }

        if (repurposeData && repurposeSearch) {
            terminal.println("Node successfully repurposed to no-cluster-manager, no-data and no-search.");
        } else if (repurposeData) {
            terminal.println("Node successfully repurposed to no-cluster-manager and no-data.");
        } else if (repurposeSearch) {
            terminal.println("Node successfully repurposed to no-cluster-manager and no-search.");
        }
    }

    private void processClusterManagerRepurposeNode(
        Terminal terminal,
        Path[] dataPaths,
        Environment env,
        boolean repurposeData,
        boolean repurposeSearch
    ) throws IOException {
        NodeEnvironment.NodePath[] nodePaths = toNodePaths(dataPaths);
        NodeEnvironment.NodePath fileCacheNodePath = toNodePaths(dataPaths)[FILE_CACHE_NODE_PATH_LOCATION];
        final PersistedClusterStateService persistedClusterStateService = createPersistedClusterStateService(env.settings(), dataPaths);
        final Metadata metadata = loadClusterState(terminal, env, persistedClusterStateService).metadata();

        Set<Path> indexPaths = Set.of();
        List<Path> shardDataPaths = List.of();
        Set<Path> fileCachePaths = Set.of();
        List<Path> fileCacheDataPaths = List.of();

        if (repurposeData) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Collecting shard data paths");
            shardDataPaths = NodeEnvironment.collectShardDataPaths(nodePaths);
            indexPaths = uniqueParentPaths(shardDataPaths);
        }

        if (repurposeSearch) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Collecting file cache data paths");
            fileCacheDataPaths = NodeEnvironment.collectFileCacheDataPath(fileCacheNodePath);
            fileCachePaths = uniqueParentPaths(fileCacheDataPaths);
        }

        if (repurposeData && repurposeSearch && shardDataPaths.isEmpty() && fileCacheDataPaths.isEmpty()) {
            terminal.println(NO_SHARD_DATA_TO_CLEAN_UP_FOUND);
            return;
        } else if (repurposeData && !repurposeSearch && shardDataPaths.isEmpty()) {
            terminal.println(NO_SHARD_DATA_TO_CLEAN_UP_FOUND);
            return;
        } else if (!repurposeData && repurposeSearch && fileCacheDataPaths.isEmpty()) {
            terminal.println(NO_FILE_CACHE_DATA_TO_CLEAN_UP_FOUND);
            return;
        }

        final Set<String> indexUUIDs = Sets.union(indexUUIDsFor(indexPaths), indexUUIDsFor(fileCachePaths));

        List<Path> cleanUpPaths = new ArrayList<>(shardDataPaths);
        cleanUpPaths.addAll(fileCacheDataPaths);
        outputVerboseInformation(terminal, cleanUpPaths, indexUUIDs, metadata);
        terminal.println(shardMessage(cleanUpPaths.size(), indexUUIDs.size()));
        outputHowToSeeVerboseInformation(terminal);

        if (repurposeData && repurposeSearch) {
            terminal.println(
                "Node is being re-purposed as cluster-manager, no-data and no-search. Clean-up of shard data and file cache data will be performed."
            );
        } else if (repurposeData) {
            terminal.println("Node is being re-purposed as cluster-manager and no-data. Clean-up of shard data will be performed.");
        } else if (repurposeSearch) {
            terminal.println("Node is being re-purposed as cluster-manager and no-search. Clean-up of file cache data will be performed.");
        }

        confirm(terminal, "Do you want to proceed?");

        if (repurposeData) {
            removePaths(terminal, shardDataPaths); // clean-up shard dirs
        }

        if (repurposeSearch) {
            removePaths(terminal, fileCacheDataPaths); // clean-up file cache dirs
        }

        if (repurposeData && repurposeSearch) {
            terminal.println("Node successfully repurposed to cluster-manager, no-data and no-search.");
        } else if (repurposeData) {
            terminal.println("Node successfully repurposed to cluster-manager and no-data.");
        } else if (repurposeSearch) {
            terminal.println("Node successfully repurposed to cluster-manager and no-search.");
        }
    }

    private ClusterState loadClusterState(Terminal terminal, Environment env, PersistedClusterStateService psf) throws IOException {
        terminal.println(Terminal.Verbosity.VERBOSE, "Loading cluster state");
        return clusterState(env, psf.loadBestOnDiskState());
    }

    private void outputVerboseInformation(Terminal terminal, Collection<Path> pathsToCleanup, Set<String> indexUUIDs, Metadata metadata) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE)) {
            terminal.println(Terminal.Verbosity.VERBOSE, "Paths to clean up:");
            pathsToCleanup.forEach(p -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + p.toString()));
            terminal.println(Terminal.Verbosity.VERBOSE, "Indices affected:");
            indexUUIDs.forEach(uuid -> terminal.println(Terminal.Verbosity.VERBOSE, "  " + toIndexName(uuid, metadata)));
        }
    }

    private void outputHowToSeeVerboseInformation(Terminal terminal) {
        if (terminal.isPrintable(Terminal.Verbosity.VERBOSE) == false) {
            terminal.println("Use -v to see list of paths and indices affected");
        }
    }

    private String toIndexName(String uuid, Metadata metadata) {
        if (metadata != null) {
            for (ObjectObjectCursor<String, IndexMetadata> indexMetadata : metadata.indices()) {
                if (indexMetadata.value.getIndexUUID().equals(uuid)) {
                    return indexMetadata.value.getIndex().getName();
                }
            }
        }
        return "no name for uuid: " + uuid;
    }

    private Set<String> indexUUIDsFor(Set<Path> indexPaths) {
        return indexPaths.stream().map(Path::getFileName).map(Path::toString).collect(Collectors.toSet());
    }

    static String noClusterManagerMessage(int indexes, int shards, int indexMetadata) {
        return "Found "
            + indexes
            + " indices ("
            + shards
            + " shards/file cache folders and "
            + indexMetadata
            + " index meta data) to clean up";
    }

    static String shardMessage(int shards, int indices) {
        return "Found " + shards + " shards/file cache folders in " + indices + " indices to clean up";
    }

    private void removePaths(Terminal terminal, Collection<Path> paths) {
        terminal.println(Terminal.Verbosity.VERBOSE, "Removing data");
        paths.forEach(this::removePath);
    }

    private void removePath(Path path) {
        try {
            IOUtils.rm(path);
        } catch (IOException e) {
            throw new OpenSearchException("Unable to clean up path: " + path + ": " + e.getMessage());
        }
    }

    @SafeVarargs
    @SuppressWarnings("varargs")
    private final Set<Path> uniqueParentPaths(Collection<Path>... paths) {
        // equals on Path is good enough here due to the way these are collected.
        return Arrays.stream(paths).flatMap(Collection::stream).map(Path::getParent).collect(Collectors.toSet());
    }

    // package-private for testing
    OptionParser getParser() {
        return parser;
    }
}
