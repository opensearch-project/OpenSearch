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

package org.opensearch.index.translog;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexCommit;
import org.apache.lucene.store.Directory;
import org.opensearch.OpenSearchException;
import org.opensearch.cli.Terminal;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.UUIDs;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.BigArrays;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.index.IndexNotFoundException;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.seqno.SequenceNumbers;
import org.opensearch.index.shard.RemoveCorruptedShardDataCommand;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.nio.channels.FileChannel;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.StandardOpenOption;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

/**
 * Action event when translog is truncated
 *
 * @opensearch.internal
 */
public class TruncateTranslogAction {

    protected static final Logger logger = LogManager.getLogger(TruncateTranslogAction.class);
    private final NamedXContentRegistry namedXContentRegistry;

    public TruncateTranslogAction(NamedXContentRegistry namedXContentRegistry) {
        this.namedXContentRegistry = namedXContentRegistry;
    }

    public Tuple<RemoveCorruptedShardDataCommand.CleanStatus, String> getCleanStatus(
        ShardPath shardPath,
        ClusterState clusterState,
        Directory indexDirectory
    ) throws IOException {
        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();
        final List<IndexCommit> commits;
        try {
            commits = DirectoryReader.listCommits(indexDirectory);
        } catch (IndexNotFoundException infe) {
            throw new OpenSearchException("unable to find a valid shard at [" + indexPath + "]", infe);
        } catch (IOException e) {
            throw new OpenSearchException("unable to list commits at [" + indexPath + "]", e);
        }

        // Retrieve the generation and UUID from the existing data
        final Map<String, String> commitData = new HashMap<>(commits.get(commits.size() - 1).getUserData());
        final String translogUUID = commitData.get(Translog.TRANSLOG_UUID_KEY);

        if (translogUUID == null) {
            throw new OpenSearchException("shard must have a valid translog UUID but got: [null]");
        }

        final boolean clean = isTranslogClean(shardPath, clusterState, translogUUID);

        if (clean) {
            return Tuple.tuple(RemoveCorruptedShardDataCommand.CleanStatus.CLEAN, null);
        }

        // Hold the lock open for the duration of the tool running
        Set<Path> translogFiles;
        try {
            translogFiles = filesInDirectory(translogPath);
        } catch (IOException e) {
            throw new OpenSearchException("failed to find existing translog files", e);
        }
        final String details = deletingFilesDetails(translogPath, translogFiles);

        return Tuple.tuple(RemoveCorruptedShardDataCommand.CleanStatus.CORRUPTED, details);
    }

    public void execute(Terminal terminal, ShardPath shardPath, Directory indexDirectory) throws IOException {
        final Path indexPath = shardPath.resolveIndex();
        final Path translogPath = shardPath.resolveTranslog();

        final String historyUUID = UUIDs.randomBase64UUID();
        final Map<String, String> commitData;
        // Hold the lock open for the duration of the tool running
        Set<Path> translogFiles;
        try {
            terminal.println("Checking existing translog files");
            translogFiles = filesInDirectory(translogPath);
        } catch (IOException e) {
            terminal.println("encountered IOException while listing directory, aborting...");
            throw new OpenSearchException("failed to find existing translog files", e);
        }

        List<IndexCommit> commits;
        try {
            terminal.println("Reading translog UUID information from Lucene commit from shard at [" + indexPath + "]");
            commits = DirectoryReader.listCommits(indexDirectory);
        } catch (IndexNotFoundException infe) {
            throw new OpenSearchException("unable to find a valid shard at [" + indexPath + "]", infe);
        }

        // Retrieve the generation and UUID from the existing data
        commitData = commits.get(commits.size() - 1).getUserData();
        final String translogUUID = commitData.get(Translog.TRANSLOG_UUID_KEY);
        if (translogUUID == null) {
            throw new OpenSearchException("shard must have a valid translog UUID");
        }

        final long globalCheckpoint = commitData.containsKey(SequenceNumbers.MAX_SEQ_NO)
            ? Long.parseLong(commitData.get(SequenceNumbers.MAX_SEQ_NO))
            : SequenceNumbers.UNASSIGNED_SEQ_NO;

        terminal.println("Translog UUID      : " + translogUUID);
        terminal.println("History UUID       : " + historyUUID);

        Path tempEmptyCheckpoint = translogPath.resolve("temp-" + Translog.CHECKPOINT_FILE_NAME);
        Path realEmptyCheckpoint = translogPath.resolve(Translog.CHECKPOINT_FILE_NAME);
        final long gen = 1;
        Path tempEmptyTranslog = translogPath.resolve("temp-" + Translog.TRANSLOG_FILE_PREFIX + gen + Translog.TRANSLOG_FILE_SUFFIX);
        Path realEmptyTranslog = translogPath.resolve(Translog.TRANSLOG_FILE_PREFIX + gen + Translog.TRANSLOG_FILE_SUFFIX);

        // Write empty checkpoint and translog to empty files
        int translogLen = writeEmptyTranslog(tempEmptyTranslog, translogUUID);
        writeEmptyCheckpoint(tempEmptyCheckpoint, translogLen, gen, globalCheckpoint);

        terminal.println("Removing existing translog files");
        IOUtils.rm(translogFiles.toArray(new Path[] {}));

        terminal.println("Creating new empty checkpoint at [" + realEmptyCheckpoint + "]");
        Files.move(tempEmptyCheckpoint, realEmptyCheckpoint, StandardCopyOption.ATOMIC_MOVE);
        terminal.println("Creating new empty translog at [" + realEmptyTranslog + "]");
        Files.move(tempEmptyTranslog, realEmptyTranslog, StandardCopyOption.ATOMIC_MOVE);

        // Fsync the translog directory after rename
        IOUtils.fsync(translogPath, true);
    }

    private boolean isTranslogClean(ShardPath shardPath, ClusterState clusterState, String translogUUID) throws IOException {
        // perform clean check of translog instead of corrupted marker file
        try {
            final Path translogPath = shardPath.resolveTranslog();
            final long translogGlobalCheckpoint = Translog.readGlobalCheckpoint(translogPath, translogUUID);
            final IndexMetadata indexMetadata = clusterState.metadata().getIndexSafe(shardPath.getShardId().getIndex());
            final IndexSettings indexSettings = new IndexSettings(indexMetadata, Settings.EMPTY);
            final TranslogConfig translogConfig = new TranslogConfig(
                shardPath.getShardId(),
                translogPath,
                indexSettings,
                BigArrays.NON_RECYCLING_INSTANCE,
                "",
                false
            );
            long primaryTerm = indexSettings.getIndexMetadata().primaryTerm(shardPath.getShardId().id());
            // We open translog to check for corruption, do not clean anything.
            final TranslogDeletionPolicy retainAllTranslogPolicy = new DefaultTranslogDeletionPolicy(
                Long.MAX_VALUE,
                Long.MAX_VALUE,
                Integer.MAX_VALUE
            ) {
                @Override
                public long minTranslogGenRequired(List<TranslogReader> readers, TranslogWriter writer) {
                    long minGen = writer.generation;
                    for (TranslogReader reader : readers) {
                        minGen = Math.min(reader.generation, minGen);
                    }
                    return minGen;
                }
            };
            try (
                Translog translog = new LocalTranslog(
                    translogConfig,
                    translogUUID,
                    retainAllTranslogPolicy,
                    () -> translogGlobalCheckpoint,
                    () -> primaryTerm,
                    seqNo -> {}
                );
                Translog.Snapshot snapshot = translog.newSnapshot(0, Long.MAX_VALUE)
            ) {
                // noinspection StatementWithEmptyBody we are just checking that we can iterate through the whole snapshot
                while (snapshot.next() != null) {
                }
            }
            return true;
        } catch (TranslogCorruptedException e) {
            return false;
        }
    }

    /** Write a checkpoint file to the given location with the given generation */
    private static void writeEmptyCheckpoint(Path filename, int translogLength, long translogGeneration, long globalCheckpoint)
        throws IOException {
        Checkpoint emptyCheckpoint = Checkpoint.emptyTranslogCheckpoint(
            translogLength,
            translogGeneration,
            globalCheckpoint,
            translogGeneration
        );
        Checkpoint.write(
            FileChannel::open,
            filename,
            emptyCheckpoint,
            StandardOpenOption.WRITE,
            StandardOpenOption.READ,
            StandardOpenOption.CREATE_NEW
        );
    }

    /**
     * Write a translog containing the given translog UUID to the given location. Returns the number of bytes written.
     */
    private static int writeEmptyTranslog(Path filename, String translogUUID) throws IOException {
        try (FileChannel fc = FileChannel.open(filename, StandardOpenOption.WRITE, StandardOpenOption.CREATE_NEW)) {
            TranslogHeader header = new TranslogHeader(translogUUID, SequenceNumbers.UNASSIGNED_PRIMARY_TERM);
            header.write(fc, true);
            return header.sizeInBytes();
        }
    }

    /** Show a warning about deleting files, asking for a confirmation if {@code batchMode} is false */
    private String deletingFilesDetails(Path translogPath, Set<Path> files) {
        StringBuilder builder = new StringBuilder();

        builder.append("Documents inside of translog files will be lost.\n")
            .append("  The following files will be DELETED at ")
            .append(translogPath)
            .append("\n\n");
        for (Iterator<Path> it = files.iterator(); it.hasNext();) {
            builder.append("  --> ").append(it.next().getFileName());
            if (it.hasNext()) {
                builder.append("\n");
            }
        }
        return builder.toString();
    }

    /** Return a Set of all files in a given directory */
    private static Set<Path> filesInDirectory(Path directory) throws IOException {
        Set<Path> files = new TreeSet<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(directory)) {
            for (Path file : stream) {
                files.add(file);
            }
        }
        return files;
    }

}
