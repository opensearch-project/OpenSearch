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

package org.opensearch.index.shard;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.NoMergePolicy;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NativeFSLockFactory;
import org.opensearch.OpenSearchException;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.coordination.OpenSearchNodeCommand;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.AllocationId;
import org.opensearch.cluster.routing.allocation.command.AllocateEmptyPrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocateStalePrimaryAllocationCommand;
import org.opensearch.cluster.routing.allocation.command.AllocationCommands;
import org.opensearch.common.CheckedConsumer;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.Strings;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.env.Environment;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.NodeMetadata;
import org.opensearch.gateway.PersistedClusterStateService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.Engine;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.store.Store;
import org.opensearch.index.translog.TruncateTranslogAction;

import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static java.util.stream.StreamSupport.stream;

/**
 * Command for removing a corrupted shard
 *
 * @opensearch.internal
 */
public class RemoveCorruptedShardDataCommand extends OpenSearchNodeCommand {

    private static final Logger logger = LogManager.getLogger(RemoveCorruptedShardDataCommand.class);

    /** CLI: --dir / -d : path to the shard’s dir (…/nodes/N/indices/UUID/SHARD-ID/{index,translog}) */
    private String dir;

    /** CLI: --index : index name (alternative to --dir) */
    private String indexName;

    /** CLI: --shard-id : shard id (required with --index) */
    private Integer shardId;

    /** CLI: --truncate-clean-translog : force truncation even if not corrupt */
    private boolean truncateCleanTranslog;

    static final String TRUNCATE_CLEAN_TRANSLOG_FLAG = "truncate-clean-translog";

    private final RemoveCorruptedLuceneSegmentsAction removeCorruptedLuceneSegmentsAction;
    private final TruncateTranslogAction truncateTranslogAction;

    public RemoveCorruptedShardDataCommand() {
        super("Removes corrupted shard files");
        this.removeCorruptedLuceneSegmentsAction = new RemoveCorruptedLuceneSegmentsAction();
        this.truncateTranslogAction = new TruncateTranslogAction(namedXContentRegistry);
    }

    // ------------------------------ Options wiring (simple setters) ------------------------------

    /** Set via launcher/dispatcher before execute() */
    public RemoveCorruptedShardDataCommand withDir(String dir) {
        this.dir = dir;
        return this;
    }

    /** Set via launcher/dispatcher before execute() */
    public RemoveCorruptedShardDataCommand withIndex(String indexName) {
        this.indexName = indexName;
        return this;
    }

    /** Set via launcher/dispatcher before execute() */
    public RemoveCorruptedShardDataCommand withShardId(Integer shardId) {
        this.shardId = shardId;
        return this;
    }

    /** Set via launcher/dispatcher before execute() */
    public RemoveCorruptedShardDataCommand withTruncateCleanTranslog(boolean truncate) {
        this.truncateCleanTranslog = truncate;
        return this;
    }

    @Override
    protected void printAdditionalHelp(Terminal terminal) {
        terminal.println("This tool attempts to detect and remove unrecoverable corrupted data in a shard.");
    }

    // ------------------------------ Path + lookup helpers ------------------------------

    @SuppressForbidden(reason = "Necessary to use the path passed in")
    protected Path getPath(String dirValue) {
        return PathUtils.get(dirValue, "", "");
    }

    /**
     * Resolve a {@link ShardPath} either from {@code --dir} (preferred) or
     * from {@code --index} + {@code --shard-id}, then pass it to {@code consumer}.
     */
    protected void findAndProcessShardPath(
        Environment environment,
        Path[] dataPaths,
        int nodeLockId,
        ClusterState clusterState,
        CheckedConsumer<ShardPath, IOException> consumer
    ) throws IOException {
        final Settings settings = environment.settings();

        final IndexMetadata indexMetadata;
        final int resolvedShardId;

        if (dir != null && dir.isBlank() == false) {
            final Path path = getPath(dir).getParent(); // dir is …/…/SHARD-ID/{index|translog}; we want …/…/SHARD-ID
            final Path shardParent = path.getParent();
            final Path shardParentParent = shardParent.getParent();
            final Path indexPath = path.resolve(ShardPath.INDEX_FOLDER_NAME);
            if (Files.exists(indexPath) == false || Files.isDirectory(indexPath) == false) {
                throw new OpenSearchException("index directory [" + indexPath + "], must exist and be a directory");
            }

            final String shardIdFileName = path.getFileName().toString();
            final String nodeIdFileName = shardParentParent.getParent().getFileName().toString();
            final String indexUUIDFolderName = shardParent.getFileName().toString();

            if (Files.isDirectory(path)
                && shardIdFileName.chars().allMatch(Character::isDigit) // SHARD-ID path element check
                && NodeEnvironment.INDICES_FOLDER.equals(shardParentParent.getFileName().toString()) // `indices` check
                && nodeIdFileName.chars().allMatch(Character::isDigit) // NODE-ID check
                && NodeEnvironment.NODES_FOLDER.equals(shardParentParent.getParent().getParent().getFileName().toString()) // `nodes` check
            ) {
                resolvedShardId = Integer.parseInt(shardIdFileName);
                indexMetadata = stream(clusterState.metadata().indices().values().spliterator(), false).filter(
                    imd -> imd.getIndexUUID().equals(indexUUIDFolderName)
                ).findFirst().orElse(null);
            } else {
                throw new OpenSearchException(
                    "Unable to resolve shard id. Wrong folder structure at [ "
                        + path
                        + " ], expected .../nodes/[NODE-ID]/indices/[INDEX-UUID]/[SHARD-ID]"
                );
            }
        } else {
            // Resolve from index + shard-id
            final String name = Objects.requireNonNull(indexName, "Index name is required");
            final Integer sid = Objects.requireNonNull(shardId, "Shard ID is required");
            resolvedShardId = sid;
            indexMetadata = clusterState.metadata().index(name);
        }

        if (indexMetadata == null) {
            throw new OpenSearchException("Unable to find index in cluster state");
        }

        final IndexSettings indexSettings = new IndexSettings(indexMetadata, settings);
        if (indexSettings.isRemoteTranslogStoreEnabled()) {
            // TODO: Revisit corrupt shard recovery for remote translog enabled indices
            throw new OpenSearchException("tool doesn't work for remote translog enabled indices");
        }

        final Index index = indexMetadata.getIndex();
        final ShardId shId = new ShardId(index, resolvedShardId);

        for (Path dataPath : dataPaths) {
            final Path shardPathLocation = dataPath.resolve(NodeEnvironment.INDICES_FOLDER)
                .resolve(index.getUUID())
                .resolve(Integer.toString(shId.id()));
            if (Files.exists(shardPathLocation)) {
                final ShardPath shardPath = ShardPath.loadShardPath(
                    logger,
                    shId,
                    indexSettings.customDataPath(),
                    new Path[] { shardPathLocation },
                    nodeLockId,
                    dataPath
                );
                if (shardPath != null) {
                    consumer.accept(shardPath);
                    return;
                }
            }
        }

        throw new OpenSearchException(
            "Unable to resolve shard path for index [" + indexMetadata.getIndex().getName() + "] and shard id [" + resolvedShardId + "]"
        );
    }

    public static boolean isCorruptMarkerFileIsPresent(final Directory directory) throws IOException {
        for (String file : directory.listAll()) {
            if (file.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                return true;
            }
        }
        return false;
    }

    protected void dropCorruptMarkerFiles(Terminal terminal, Path path, Directory directory, boolean clean) throws IOException {
        if (clean) {
            confirm("""
                This shard has been marked as corrupted but no corruption can now be detected.
                This may indicate an intermittent hardware problem. The corruption marker can be
                removed, but there is a risk that data has been undetectably lost.

                Are you taking a risk of losing documents and proceed with removing a corrupted marker ?""", terminal);
        }
        for (String file : directory.listAll()) {
            if (file.startsWith(Store.CORRUPTED_MARKER_NAME_PREFIX)) {
                directory.deleteFile(file);
                terminal.println("Deleted corrupt marker " + file + " from " + path);
            }
        }
    }

    private static void loseDataDetailsBanner(Terminal terminal, Tuple<CleanStatus, String> cleanStatus) {
        if (cleanStatus.v2() != null) {
            terminal.println("");
            terminal.println("  " + cleanStatus.v2());
            terminal.println("");
        }
    }

    private static void confirm(String msg, Terminal terminal) {
        terminal.println(msg);
        String text = terminal.readText("Confirm [y/N] ");
        if (text.equalsIgnoreCase("y") == false) {
            throw new OpenSearchException("aborted by user");
        }
    }

    private void warnAboutIndexBackup(Terminal terminal) {
        terminal.println("-----------------------------------------------------------------------");
        terminal.println("");
        terminal.println("  Please make a complete backup of your index before using this tool.");
        terminal.println("");
        terminal.println("-----------------------------------------------------------------------");
    }

    // ------------------------------ Main flow ------------------------------

    @Override
    protected void processNodePaths(Terminal terminal, Path[] dataPaths, int nodeLockId, Environment environment) throws IOException {
        warnAboutIndexBackup(terminal);

        final ClusterState clusterState = loadTermAndClusterState(
            createPersistedClusterStateService(environment.settings(), dataPaths),
            environment
        ).v2();

        findAndProcessShardPath(environment, dataPaths, nodeLockId, clusterState, shardPath -> {
            final Path indexPath = shardPath.resolveIndex();
            final Path translogPath = shardPath.resolveTranslog();
            if (Files.exists(translogPath) == false || Files.isDirectory(translogPath) == false) {
                throw new OpenSearchException("translog directory [" + translogPath + "], must exist and be a directory");
            }

            final PrintWriter writer = terminal.getWriter();
            final PrintStream printStream = new PrintStream(new OutputStream() {
                @Override
                public void write(int b) {
                    writer.write(b);
                }
            }, false, "UTF-8");
            final boolean verbose = terminal.isPrintable(Terminal.Verbosity.VERBOSE);

            final Directory indexDirectory = getDirectory(indexPath);

            final Tuple<CleanStatus, String> indexCleanStatus;
            final Tuple<CleanStatus, String> translogCleanStatus;
            try (Directory indexDir = indexDirectory) {
                // keep the index lock to block any runs of older versions of this tool
                try (Lock writeIndexLock = indexDir.obtainLock(IndexWriter.WRITE_LOCK_NAME)) {

                    // ---- Index analysis
                    terminal.println("");
                    terminal.println("Opening Lucene index at " + indexPath);
                    terminal.println("");
                    try {
                        indexCleanStatus = removeCorruptedLuceneSegmentsAction.getCleanStatus(
                            indexDir,
                            writeIndexLock,
                            printStream,
                            verbose
                        );
                    } catch (Exception e) {
                        terminal.println(e.getMessage());
                        throw e;
                    }
                    terminal.println("");
                    terminal.println(" >> Lucene index is " + indexCleanStatus.v1().getMessage() + " at " + indexPath);
                    terminal.println("");

                    // ---- Translog analysis
                    if (truncateCleanTranslog) {
                        translogCleanStatus = Tuple.tuple(
                            CleanStatus.OVERRIDDEN,
                            "Translog was not analysed and will be truncated due to the --" + TRUNCATE_CLEAN_TRANSLOG_FLAG + " flag"
                        );
                    } else if (indexCleanStatus.v1() != CleanStatus.UNRECOVERABLE) {
                        terminal.println("");
                        terminal.println("Opening translog at " + translogPath);
                        terminal.println("");
                        try {
                            translogCleanStatus = truncateTranslogAction.getCleanStatus(shardPath, clusterState, indexDir);
                        } catch (Exception e) {
                            terminal.println(e.getMessage());
                            throw e;
                        }
                        terminal.println("");
                        terminal.println(" >> Translog is " + translogCleanStatus.v1().getMessage() + " at " + translogPath);
                        terminal.println("");
                    } else {
                        translogCleanStatus = Tuple.tuple(CleanStatus.UNRECOVERABLE, null);
                    }

                    // ---- Decide + warn
                    final CleanStatus indexStatus = indexCleanStatus.v1();
                    final CleanStatus translogStatus = translogCleanStatus.v1();

                    if (indexStatus == CleanStatus.CLEAN && translogStatus == CleanStatus.CLEAN) {
                        throw new OpenSearchException(
                            "Shard does not seem to be corrupted at "
                                + shardPath.getDataPath()
                                + " (pass --"
                                + TRUNCATE_CLEAN_TRANSLOG_FLAG
                                + " to truncate the translog anyway)"
                        );
                    }

                    if (indexStatus == CleanStatus.UNRECOVERABLE) {
                        if (indexCleanStatus.v2() != null) {
                            terminal.println("Details: " + indexCleanStatus.v2());
                        }
                        terminal.println("You can allocate a new, empty, primary shard with the following command:");
                        printRerouteCommand(shardPath, terminal, false);
                        throw new OpenSearchException("Index is unrecoverable");
                    }

                    terminal.println("-----------------------------------------------------------------------");
                    if (indexStatus != CleanStatus.CLEAN) {
                        loseDataDetailsBanner(terminal, indexCleanStatus);
                    }
                    if (translogStatus != CleanStatus.CLEAN) {
                        loseDataDetailsBanner(terminal, translogCleanStatus);
                    }
                    terminal.println("            WARNING:              YOU MAY LOSE DATA.");
                    terminal.println("-----------------------------------------------------------------------");

                    confirm("Continue and remove corrupted data from the shard ?", terminal);

                    if (indexStatus != CleanStatus.CLEAN) {
                        removeCorruptedLuceneSegmentsAction.execute(terminal, indexDir, writeIndexLock, printStream, verbose);
                    }
                    if (translogStatus != CleanStatus.CLEAN) {
                        truncateTranslogAction.execute(terminal, shardPath, indexDir);
                    }
                } catch (LockObtainFailedException lofe) {
                    final String msg = "Failed to lock shard's directory at [" + indexPath + "], is OpenSearch still running?";
                    terminal.println(msg);
                    throw new OpenSearchException(msg);
                }

                final CleanStatus indexStatus = indexCleanStatus.v1();
                final CleanStatus translogStatus = translogCleanStatus.v1();

                // Write a new history commit (IndexWriter obtains its own lock)
                addNewHistoryCommit(indexDir, terminal, translogStatus != CleanStatus.CLEAN);

                // Bump allocation id so only a stale allocation is allowed
                newAllocationId(shardPath, terminal);

                // If index was clean-but-marked, drop markers
                if (indexStatus != CleanStatus.CLEAN) {
                    dropCorruptMarkerFiles(terminal, indexPath, indexDir, indexStatus == CleanStatus.CLEAN_WITH_CORRUPTED_MARKER);
                }
            }
        });
    }

    private Directory getDirectory(Path indexPath) {
        try {
            return FSDirectory.open(indexPath, NativeFSLockFactory.INSTANCE);
        } catch (Throwable t) {
            throw new OpenSearchException("ERROR: could not open directory \"" + indexPath + "\"; exiting");
        }
    }

    protected void addNewHistoryCommit(Directory indexDirectory, Terminal terminal, boolean updateLocalCheckpoint) throws IOException {
        final String historyUUID = UUIDs.randomBase64UUID();

        terminal.println("Marking index with the new history uuid : " + historyUUID);
        // commit the new history id
        final IndexWriterConfig iwc = new IndexWriterConfig(null)
            // we don't want merges to happen here - we call maybe merge on the engine
            // later once we stared it up otherwise we would need to wait for it here
            // we also don't specify a codec here and merges should use the engines for this index
            .setCommitOnClose(false)
            .setSoftDeletesField(Lucene.SOFT_DELETES_FIELD)
            .setMergePolicy(NoMergePolicy.INSTANCE)
            .setOpenMode(IndexWriterConfig.OpenMode.APPEND);
        // IndexWriter acquires directory lock by its own

        try (IndexWriter indexWriter = new IndexWriter(indexDirectory, iwc)) {
            final Map<String, String> userData = new HashMap<>();
            indexWriter.getLiveCommitData().forEach(e -> userData.put(e.getKey(), e.getValue()));

            if (updateLocalCheckpoint) {
                // In order to have a safe commit invariant, we have to assign the global checkpoint to the max_seqno of the last commit.
                // We can only safely do it because we will generate a new history uuid this shard.
                final SequenceNumbers.CommitInfo commitInfo = SequenceNumbers.loadSeqNoInfoFromLuceneCommit(userData.entrySet());
                // Also advances the local checkpoint of the last commit to its max_seqno.
                userData.put(SequenceNumbers.LOCAL_CHECKPOINT_KEY, Long.toString(commitInfo.maxSeqNo));
            }

            // commit the new history id
            userData.put(Engine.HISTORY_UUID_KEY, historyUUID);

            indexWriter.setLiveCommitData(userData.entrySet());
            indexWriter.commit();
        }
    }

    private void newAllocationId(ShardPath shardPath, Terminal terminal) throws IOException {
        final Path shardStatePath = shardPath.getShardStatePath();
        final ShardStateMetadata shardStateMetadata = ShardStateMetadata.FORMAT.loadLatestState(
            logger,
            namedXContentRegistry,
            shardStatePath
        );

        if (shardStateMetadata == null) {
            throw new OpenSearchException("No shard state meta data at " + shardStatePath);
        }

        final AllocationId newAllocationId = AllocationId.newInitializing();
        terminal.println("Changing allocation id " + shardStateMetadata.allocationId.getId() + " to " + newAllocationId.getId());

        final ShardStateMetadata newShardStateMetadata = new ShardStateMetadata(
            shardStateMetadata.primary,
            shardStateMetadata.indexUUID,
            newAllocationId,
            ShardStateMetadata.IndexDataLocation.LOCAL
        );

        ShardStateMetadata.FORMAT.writeAndCleanup(newShardStateMetadata, shardStatePath);

        terminal.println("");
        terminal.println("You should run the following command to allocate this shard:");
        printRerouteCommand(shardPath, terminal, true);
    }

    private void printRerouteCommand(ShardPath shardPath, Terminal terminal, boolean allocateStale) throws IOException {
        final Path nodePath = getNodePath(shardPath);
        final NodeMetadata nodeMetadata = PersistedClusterStateService.nodeMetadata(nodePath);
        if (nodeMetadata == null) {
            throw new OpenSearchException("No node meta data at " + nodePath);
        }

        final String nodeId = nodeMetadata.nodeId();
        final String index = shardPath.getShardId().getIndexName();
        final int id = shardPath.getShardId().id();

        final AllocationCommands commands = new AllocationCommands(
            allocateStale
                ? new AllocateStalePrimaryAllocationCommand(index, id, nodeId, false)
                : new AllocateEmptyPrimaryAllocationCommand(index, id, nodeId, false)
        );

        terminal.println("");
        terminal.println("POST /_cluster/reroute\n" + Strings.toString(MediaTypeRegistry.JSON, commands, true, true));
        terminal.println("");
        terminal.println("You must accept the possibility of data loss by changing the `accept_data_loss` parameter to `true`.");
        terminal.println("");
    }

    private Path getNodePath(ShardPath shardPath) {
        final Path nodePath = shardPath.getDataPath().getParent().getParent().getParent();
        if (Files.exists(nodePath) == false
            || Files.exists(nodePath.resolve(PersistedClusterStateService.METADATA_DIRECTORY_NAME)) == false) {
            throw new OpenSearchException("Unable to resolve node path for " + shardPath);
        }
        return nodePath;
    }

    /**
     * Status of the shard cleaning operation
     *
     * @opensearch.internal
     */
    public enum CleanStatus {
        CLEAN("clean"),
        CLEAN_WITH_CORRUPTED_MARKER("marked corrupted, but no corruption detected"),
        CORRUPTED("corrupted"),
        UNRECOVERABLE("corrupted and unrecoverable"),
        OVERRIDDEN("to be truncated regardless of whether it is corrupt");

        private final String msg;

        CleanStatus(String msg) {
            this.msg = msg;
        }

        public String getMessage() {
            return msg;
        }
    }
}
