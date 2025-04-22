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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.message.ParameterizedMessage;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.SegmentInfos;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.Lock;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.opensearch.OpenSearchException;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.CheckedFunction;
import org.opensearch.common.Randomness;
import org.opensearch.common.SuppressForbidden;
import org.opensearch.common.UUIDs;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.lease.Releasable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.util.FileSystemUtils;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.gateway.MetadataStateFormat;
import org.opensearch.gateway.PersistedClusterStateService;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.IndexStoreListener;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.fs.FsProbe;
import org.opensearch.monitor.jvm.JvmInfo;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileStore;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.Collections.unmodifiableSet;

/**
 * A component that holds all data paths for a single node.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public final class NodeEnvironment implements Closeable {
    /**
     * A node path.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class NodePath {
        /* ${data.paths}/nodes/{node.id} */
        public final Path path;
        /* ${data.paths}/nodes/{node.id}/indices */
        public final Path indicesPath;
        /** Cached FileStore from path */
        public final FileStore fileStore;
        public final Path fileCachePath;
        /*
          Cache reserved size can default to a different value depending on configuration
        */
        public ByteSizeValue fileCacheReservedSize;
        public final int majorDeviceNumber;
        public final int minorDeviceNumber;

        public NodePath(Path path) throws IOException {
            this.path = path;
            this.indicesPath = path.resolve(INDICES_FOLDER);
            this.fileCachePath = path.resolve(CACHE_FOLDER);
            this.fileStore = Environment.getFileStore(path);
            this.fileCacheReservedSize = ByteSizeValue.ZERO;
            if (fileStore.supportsFileAttributeView("lucene")) {
                this.majorDeviceNumber = (int) fileStore.getAttribute("lucene:major_device_number");
                this.minorDeviceNumber = (int) fileStore.getAttribute("lucene:minor_device_number");
            } else {
                this.majorDeviceNumber = -1;
                this.minorDeviceNumber = -1;
            }
        }

        /**
         * Resolves the given shards directory against this NodePath
         * ${data.paths}/nodes/{node.id}/indices/{index.uuid}/{shard.id}
         */
        public Path resolve(ShardId shardId) {
            return resolve(shardId.getIndex()).resolve(Integer.toString(shardId.id()));
        }

        /**
         * Resolves index directory against this NodePath
         * ${data.paths}/nodes/{node.id}/indices/{index.uuid}
         */
        public Path resolve(Index index) {
            return resolve(index.getUUID());
        }

        Path resolve(String uuid) {
            return indicesPath.resolve(uuid);
        }

        @Override
        public String toString() {
            return "NodePath{"
                + "path="
                + path
                + ", indicesPath="
                + indicesPath
                + ", fileStore="
                + fileStore
                + ", majorDeviceNumber="
                + majorDeviceNumber
                + ", minorDeviceNumber="
                + minorDeviceNumber
                + '}';
        }

    }

    private final Logger logger = LogManager.getLogger(NodeEnvironment.class);
    private final NodePath[] nodePaths;
    private final NodePath fileCacheNodePath;
    private final Path sharedDataPath;
    private final Lock[] locks;

    private final int nodeLockId;
    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final Map<ShardId, InternalShardLock> shardLocks = new HashMap<>();

    private final NodeMetadata nodeMetadata;

    private final org.opensearch.index.store.IndexStoreListener indexStoreListener;

    /**
     * Maximum number of data nodes that should run in an environment.
     */
    public static final Setting<Integer> MAX_LOCAL_STORAGE_NODES_SETTING = Setting.intSetting(
        "node.max_local_storage_nodes",
        1,
        1,
        Property.NodeScope,
        Property.Deprecated
    );

    /**
     * Seed for determining a persisted unique uuid of this node. If the node has already a persisted uuid on disk,
     * this seed will be ignored and the uuid from disk will be reused.
     */
    public static final Setting<Long> NODE_ID_SEED_SETTING = Setting.longSetting("node.id.seed", 0L, Long.MIN_VALUE, Property.NodeScope);

    /**
     * If true the [verbose] SegmentInfos.infoStream logging is sent to System.out.
     */
    public static final Setting<Boolean> ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING = Setting.boolSetting(
        "node.enable_lucene_segment_infos_trace",
        false,
        Property.NodeScope
    );

    public static final String NODES_FOLDER = "nodes";
    public static final String INDICES_FOLDER = "indices";
    public static final String CACHE_FOLDER = "cache";
    public static final String NODE_LOCK_FILENAME = "node.lock";

    /**
     * The node lock.
     *
     * @opensearch.internal
     */
    public static class NodeLock implements Releasable {

        private final int nodeId;
        private final Lock[] locks;
        private final NodePath[] nodePaths;

        /**
         * Tries to acquire a node lock for a node id, throws {@code IOException} if it is unable to acquire it
         * @param pathFunction function to check node path before attempt of acquiring a node lock
         */
        public NodeLock(
            final int nodeId,
            final Logger logger,
            final Environment environment,
            final CheckedFunction<Path, Boolean, IOException> pathFunction
        ) throws IOException {
            this.nodeId = nodeId;
            nodePaths = new NodePath[environment.dataFiles().length];
            locks = new Lock[nodePaths.length];
            try {
                final Path[] dataPaths = environment.dataFiles();
                for (int dirIndex = 0; dirIndex < dataPaths.length; dirIndex++) {
                    Path dataDir = dataPaths[dirIndex];
                    Path dir = resolveNodePath(dataDir, nodeId);
                    if (pathFunction.apply(dir) == false) {
                        continue;
                    }
                    try (Directory luceneDir = FSDirectory.open(dir, NativeFSLockFactory.INSTANCE)) {
                        logger.trace("obtaining node lock on {} ...", dir.toAbsolutePath());
                        locks[dirIndex] = luceneDir.obtainLock(NODE_LOCK_FILENAME);
                        nodePaths[dirIndex] = new NodePath(dir);
                    } catch (IOException e) {
                        logger.trace(() -> new ParameterizedMessage("failed to obtain node lock on {}", dir.toAbsolutePath()), e);
                        // release all the ones that were obtained up until now
                        throw (e instanceof LockObtainFailedException
                            ? e
                            : new IOException("failed to obtain lock on " + dir.toAbsolutePath(), e));
                    }
                }
            } catch (IOException e) {
                close();
                throw e;
            }
        }

        public NodePath[] getNodePaths() {
            return nodePaths;
        }

        @Override
        public void close() {
            for (int i = 0; i < locks.length; i++) {
                if (locks[i] != null) {
                    IOUtils.closeWhileHandlingException(locks[i]);
                }
                locks[i] = null;
            }
        }
    }

    public NodeEnvironment(Settings settings, Environment environment) throws IOException {
        this(settings, environment, org.opensearch.index.store.IndexStoreListener.EMPTY);
    }

    /**
     * Use {@link NodeEnvironment#NodeEnvironment(Settings, Environment, org.opensearch.index.store.IndexStoreListener)} instead.
     * This constructor is kept around on 2.x to avoid breaking changes.
     */
    @Deprecated(forRemoval = true, since = "2.19.0")
    public NodeEnvironment(Settings settings, Environment environment, IndexStoreListener indexStoreListener) throws IOException {
        this(settings, environment, (org.opensearch.index.store.IndexStoreListener) indexStoreListener);
    }

    /**
     * Setup the environment.
     * @param settings settings from opensearch.yml
     */
    public NodeEnvironment(Settings settings, Environment environment, org.opensearch.index.store.IndexStoreListener indexStoreListener)
        throws IOException {
        if (DiscoveryNode.nodeRequiresLocalStorage(settings) == false) {
            nodePaths = null;
            fileCacheNodePath = null;
            sharedDataPath = null;
            locks = null;
            nodeLockId = -1;
            nodeMetadata = new NodeMetadata(generateNodeId(settings), Version.CURRENT);
            this.indexStoreListener = org.opensearch.index.store.IndexStoreListener.EMPTY;
            return;
        }
        boolean success = false;
        NodeLock nodeLock = null;

        try {
            sharedDataPath = environment.sharedDataFile();
            IOException lastException = null;
            int maxLocalStorageNodes = MAX_LOCAL_STORAGE_NODES_SETTING.get(settings);

            final AtomicReference<IOException> onCreateDirectoriesException = new AtomicReference<>();
            for (int possibleLockId = 0; possibleLockId < maxLocalStorageNodes; possibleLockId++) {
                try {
                    nodeLock = new NodeLock(possibleLockId, logger, environment, dir -> {
                        try {
                            Files.createDirectories(dir);
                        } catch (IOException e) {
                            onCreateDirectoriesException.set(e);
                            throw e;
                        }
                        return true;
                    });
                    break;
                } catch (LockObtainFailedException e) {
                    // ignore any LockObtainFailedException
                } catch (IOException e) {
                    if (onCreateDirectoriesException.get() != null) {
                        throw onCreateDirectoriesException.get();
                    }
                    lastException = e;
                }
            }

            if (nodeLock == null) {
                final String message = String.format(
                    Locale.ROOT,
                    "failed to obtain node locks, tried [%s] with lock id%s;"
                        + " maybe these locations are not writable or multiple nodes were started without increasing [%s] (was [%d])?",
                    Arrays.toString(environment.dataFiles()),
                    maxLocalStorageNodes == 1 ? " [0]" : "s [0--" + (maxLocalStorageNodes - 1) + "]",
                    MAX_LOCAL_STORAGE_NODES_SETTING.getKey(),
                    maxLocalStorageNodes
                );
                throw new IllegalStateException(message, lastException);
            }
            this.locks = nodeLock.locks;
            this.nodePaths = nodeLock.nodePaths;
            this.fileCacheNodePath = nodePaths[0];

            this.nodeLockId = nodeLock.nodeId;

            if (logger.isDebugEnabled()) {
                logger.debug("using node location [{}], local_lock_id [{}]", nodePaths, nodeLockId);
            }

            maybeLogPathDetails();
            maybeLogHeapDetails();

            applySegmentInfosTrace(settings);
            assertCanWrite();

            if (DiscoveryNode.isClusterManagerNode(settings) || DiscoveryNode.isDataNode(settings)) {
                ensureAtomicMoveSupported(nodePaths);
            }

            if (DiscoveryNode.isDataNode(settings) == false) {
                if (DiscoveryNode.isClusterManagerNode(settings) == false) {
                    ensureNoIndexMetadata(nodePaths);
                }

                ensureNoShardData(nodePaths);
            }

            if (DiscoveryNode.isSearchNode(settings) == false) {
                ensureNoFileCacheData(fileCacheNodePath);
            }

            this.nodeMetadata = loadNodeMetadata(settings, logger, nodePaths);
            this.indexStoreListener = indexStoreListener;
            success = true;
        } finally {
            if (success == false) {
                close();
            }
        }
    }

    /**
     * Resolve a specific nodes/{node.id} path for the specified path and node lock id.
     *
     * @param path       the path
     * @param nodeLockId the node lock id
     * @return the resolved path
     */
    public static Path resolveNodePath(final Path path, final int nodeLockId) {
        return path.resolve(NODES_FOLDER).resolve(Integer.toString(nodeLockId));
    }

    private void maybeLogPathDetails() throws IOException {

        // We do some I/O in here, so skip this if DEBUG/INFO are not enabled:
        if (logger.isDebugEnabled()) {
            // Log one line per path.data:
            StringBuilder sb = new StringBuilder();
            for (NodePath nodePath : nodePaths) {
                sb.append('\n').append(" -> ").append(nodePath.path.toAbsolutePath());

                FsInfo.Path fsPath = FsProbe.getFSInfo(nodePath);
                sb.append(", free_space [")
                    .append(fsPath.getFree())
                    .append("], usable_space [")
                    .append(fsPath.getAvailable())
                    .append("], total_space [")
                    .append(fsPath.getTotal())
                    .append("], mount [")
                    .append(fsPath.getMount())
                    .append("], type [")
                    .append(fsPath.getType())
                    .append(']');
            }
            logger.debug("node data locations details:{}", sb);
        } else if (logger.isInfoEnabled()) {
            FsInfo.Path totFSPath = new FsInfo.Path();
            Set<String> allTypes = new HashSet<>();
            Set<String> allMounts = new HashSet<>();
            for (NodePath nodePath : nodePaths) {
                FsInfo.Path fsPath = FsProbe.getFSInfo(nodePath);
                String mount = fsPath.getMount();
                if (allMounts.contains(mount) == false) {
                    allMounts.add(mount);
                    String type = fsPath.getType();
                    if (type != null) {
                        allTypes.add(type);
                    }
                    totFSPath.add(fsPath);
                }
            }

            // Just log a 1-line summary:
            logger.info(
                "using [{}] data paths, mounts [{}], net usable_space [{}], net total_space [{}], types [{}]",
                nodePaths.length,
                allMounts,
                totFSPath.getAvailable(),
                totFSPath.getTotal(),
                toString(allTypes)
            );
        }
    }

    private void maybeLogHeapDetails() {
        JvmInfo jvmInfo = JvmInfo.jvmInfo();
        ByteSizeValue maxHeapSize = jvmInfo.getMem().getHeapMax();
        String useCompressedOops = jvmInfo.useCompressedOops();
        logger.info("heap size [{}], compressed ordinary object pointers [{}]", maxHeapSize, useCompressedOops);
    }

    /**
     * scans the node paths and loads existing metadata file. If not found a new meta data will be generated
     */
    private static NodeMetadata loadNodeMetadata(Settings settings, Logger logger, NodePath... nodePaths) throws IOException {
        final Path[] paths = Arrays.stream(nodePaths).map(np -> np.path).toArray(Path[]::new);
        NodeMetadata metadata = PersistedClusterStateService.nodeMetadata(paths);
        if (metadata == null) {
            // load legacy metadata
            final Set<String> nodeIds = new HashSet<>();
            for (final Path path : paths) {
                final NodeMetadata oldStyleMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, path);
                if (oldStyleMetadata != null) {
                    nodeIds.add(oldStyleMetadata.nodeId());
                }
            }
            if (nodeIds.size() > 1) {
                throw new IllegalStateException("data paths " + Arrays.toString(paths) + " belong to multiple nodes with IDs " + nodeIds);
            }
            // load legacy metadata
            final NodeMetadata legacyMetadata = NodeMetadata.FORMAT.loadLatestState(logger, NamedXContentRegistry.EMPTY, paths);
            if (legacyMetadata == null) {
                assert nodeIds.isEmpty() : nodeIds;
                metadata = new NodeMetadata(generateNodeId(settings), Version.CURRENT);
            } else {
                assert nodeIds.equals(Collections.singleton(legacyMetadata.nodeId())) : nodeIds + " doesn't match " + legacyMetadata;
                metadata = legacyMetadata;
            }
        }
        metadata = metadata.upgradeToCurrentVersion();
        assert metadata.nodeVersion().equals(Version.CURRENT) : metadata.nodeVersion() + " != " + Version.CURRENT;

        return metadata;
    }

    public static String generateNodeId(Settings settings) {
        Random random = Randomness.get(settings, NODE_ID_SEED_SETTING);
        return UUIDs.randomBase64UUID(random);
    }

    @SuppressForbidden(reason = "System.out.*")
    static void applySegmentInfosTrace(Settings settings) {
        if (ENABLE_LUCENE_SEGMENT_INFOS_TRACE_SETTING.get(settings)) {
            SegmentInfos.setInfoStream(System.out);
        }
    }

    private static String toString(Collection<String> items) {
        StringBuilder b = new StringBuilder();
        for (String item : items) {
            if (b.length() > 0) {
                b.append(", ");
            }
            b.append(item);
        }
        return b.toString();
    }

    /**
     * Deletes a shard data directory iff the shards locks were successfully acquired.
     *
     * @param shardId the id of the shard to delete to delete
     * @throws IOException if an IOException occurs
     */
    public void deleteShardDirectorySafe(ShardId shardId, IndexSettings indexSettings) throws IOException, ShardLockObtainFailedException {
        final Path[] paths = availableShardPaths(shardId);
        logger.trace("deleting shard {} directory, paths: [{}]", shardId, paths);
        try (ShardLock lock = shardLock(shardId, "shard deletion under lock")) {
            deleteShardDirectoryUnderLock(lock, indexSettings);
        }
    }

    /**
     * Acquires, then releases, all {@code write.lock} files in the given
     * shard paths. The "write.lock" file is assumed to be under the shard
     * path's "index" directory as used by OpenSearch.
     *
     * @throws LockObtainFailedException if any of the locks could not be acquired
     */
    public static void acquireFSLockForPaths(IndexSettings indexSettings, Path... shardPaths) throws IOException {
        Lock[] locks = new Lock[shardPaths.length];
        Directory[] dirs = new Directory[shardPaths.length];
        try {
            for (int i = 0; i < shardPaths.length; i++) {
                // resolve the directory the shard actually lives in
                Path p = shardPaths[i].resolve("index");
                // open a directory (will be immediately closed) on the shard's location
                dirs[i] = new NIOFSDirectory(p, indexSettings.getValue(FsDirectoryFactory.INDEX_LOCK_FACTOR_SETTING));
                // create a lock for the "write.lock" file
                try {
                    locks[i] = dirs[i].obtainLock(IndexWriter.WRITE_LOCK_NAME);
                } catch (IOException ex) {
                    throw new LockObtainFailedException("unable to acquire " + IndexWriter.WRITE_LOCK_NAME + " for " + p, ex);
                }
            }
        } finally {
            IOUtils.closeWhileHandlingException(locks);
            IOUtils.closeWhileHandlingException(dirs);
        }
    }

    /**
     * Deletes a shard data directory. Note: this method assumes that the shard
     * lock is acquired. This method will also attempt to acquire the write
     * locks for the shard's paths before deleting the data, but this is best
     * effort, as the lock is released before the deletion happens in order to
     * allow the folder to be deleted
     *
     * @param lock the shards lock
     * @throws IOException if an IOException occurs
     * @throws OpenSearchException if the write.lock is not acquirable
     */
    public void deleteShardDirectoryUnderLock(ShardLock lock, IndexSettings indexSettings) throws IOException {
        final ShardId shardId = lock.getShardId();
        assert isShardLocked(shardId) : "shard " + shardId + " is not locked";

        indexStoreListener.beforeShardPathDeleted(shardId, indexSettings, this);

        final Path[] paths = availableShardPaths(shardId);
        logger.trace("acquiring locks for {}, paths: [{}]", shardId, paths);
        acquireFSLockForPaths(indexSettings, paths);
        IOUtils.rm(paths);
        if (indexSettings.hasCustomDataPath()) {
            Path customLocation = resolveCustomLocation(indexSettings.customDataPath(), shardId);
            logger.trace("acquiring lock for {}, custom path: [{}]", shardId, customLocation);
            acquireFSLockForPaths(indexSettings, customLocation);
            logger.trace("deleting custom shard {} directory [{}]", shardId, customLocation);
            IOUtils.rm(customLocation);
        }
        logger.trace("deleted shard {} directory, paths: [{}]", shardId, paths);
        assert assertPathsDoNotExist(paths);
    }

    private static boolean assertPathsDoNotExist(final Path[] paths) {
        Set<Path> existingPaths = Stream.of(paths).filter(FileSystemUtils::exists).filter(leftOver -> {
            // Relaxed assertion for the special case where only the empty state directory exists after deleting
            // the shard directory because it was created again as a result of a metadata read action concurrently.
            try (DirectoryStream<Path> children = Files.newDirectoryStream(leftOver)) {
                Iterator<Path> iter = children.iterator();
                if (iter.hasNext() == false) {
                    return true;
                }
                Path maybeState = iter.next();
                if (iter.hasNext() || maybeState.equals(leftOver.resolve(MetadataStateFormat.STATE_DIR_NAME)) == false) {
                    return true;
                }
                try (DirectoryStream<Path> stateChildren = Files.newDirectoryStream(maybeState)) {
                    return stateChildren.iterator().hasNext();
                }
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }).collect(Collectors.toSet());
        assert existingPaths.size() == 0 : "Paths exist that should have been deleted: " + existingPaths;
        return existingPaths.size() == 0;
    }

    private boolean isShardLocked(ShardId id) {
        try {
            shardLock(id, "checking if shard is locked").close();
            return false;
        } catch (ShardLockObtainFailedException ex) {
            return true;
        }
    }

    /**
     * Deletes an indexes data directory recursively iff all of the indexes
     * shards locks were successfully acquired. If any of the indexes shard directories can't be locked
     * non of the shards will be deleted
     *
     * @param index the index to delete
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @param indexSettings settings for the index being deleted
     * @throws IOException if any of the shards data directories can't be locked or deleted
     */
    public void deleteIndexDirectorySafe(Index index, long lockTimeoutMS, IndexSettings indexSettings) throws IOException,
        ShardLockObtainFailedException {
        final List<ShardLock> locks = lockAllForIndex(index, indexSettings, "deleting index directory", lockTimeoutMS);
        try {
            deleteIndexDirectoryUnderLock(index, indexSettings);
        } finally {
            IOUtils.closeWhileHandlingException(locks);
        }
    }

    /**
     * Deletes an indexes data directory recursively.
     * Note: this method assumes that the shard lock is acquired
     *
     * @param index the index to delete
     * @param indexSettings settings for the index being deleted
     */
    public void deleteIndexDirectoryUnderLock(Index index, IndexSettings indexSettings) throws IOException {
        indexStoreListener.beforeIndexPathDeleted(index, indexSettings, this);

        final Path[] indexPaths = indexPaths(index);
        logger.trace("deleting index {} directory, paths({}): [{}]", index, indexPaths.length, indexPaths);
        IOUtils.rm(indexPaths);
        if (indexSettings.hasCustomDataPath()) {
            Path customLocation = resolveIndexCustomLocation(indexSettings.customDataPath(), index.getUUID());
            logger.trace("deleting custom index {} directory [{}]", index, customLocation);
            IOUtils.rm(customLocation);
        }
    }

    private void deleteIndexFileCacheDirectory(Index index) {
        final Path indexCachePath = fileCacheNodePath().fileCachePath.resolve(index.getUUID());
        logger.trace("deleting index {} file cache directory, path: [{}]", index, indexCachePath);
        if (Files.exists(indexCachePath)) {
            try {
                IOUtils.rm(indexCachePath);
            } catch (IOException e) {
                logger.error(() -> new ParameterizedMessage("Failed to delete cache path for index {}", index), e);
            }
        }
    }

    /**
     * Tries to lock all local shards for the given index. If any of the shard locks can't be acquired
     * a {@link ShardLockObtainFailedException} is thrown and all previously acquired locks are released.
     *
     * @param index the index to lock shards for
     * @param lockTimeoutMS how long to wait for acquiring the indices shard locks
     * @return the {@link ShardLock} instances for this index.
     */
    public List<ShardLock> lockAllForIndex(
        final Index index,
        final IndexSettings settings,
        final String lockDetails,
        final long lockTimeoutMS
    ) throws ShardLockObtainFailedException {
        final int numShards = settings.getNumberOfShards();
        if (numShards <= 0) {
            throw new IllegalArgumentException("settings must contain a non-null > 0 number of shards");
        }
        logger.trace("locking all shards for index {} - [{}]", index, numShards);
        List<ShardLock> allLocks = new ArrayList<>(numShards);
        boolean success = false;
        long startTimeNS = System.nanoTime();
        try {
            for (int i = 0; i < numShards; i++) {
                long timeoutLeftMS = Math.max(0, lockTimeoutMS - TimeValue.nsecToMSec((System.nanoTime() - startTimeNS)));
                allLocks.add(shardLock(new ShardId(index, i), lockDetails, timeoutLeftMS));
            }
            success = true;
        } finally {
            if (success == false) {
                logger.trace("unable to lock all shards for index {}", index);
                IOUtils.closeWhileHandlingException(allLocks);
            }
        }
        return allLocks;
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * a {@link ShardLockObtainFailedException} is thrown.
     * <p>
     * Note: this method will return immediately if the lock can't be acquired.
     *
     * @param id the shard ID to lock
     * @param details information about why the shard is being locked
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     */
    public ShardLock shardLock(ShardId id, final String details) throws ShardLockObtainFailedException {
        return shardLock(id, details, 0);
    }

    /**
     * Tries to lock the given shards ID. A shard lock is required to perform any kind of
     * write operation on a shards data directory like deleting files, creating a new index writer
     * or recover from a different shard instance into it. If the shard lock can not be acquired
     * a {@link ShardLockObtainFailedException} is thrown
     * @param shardId the shard ID to lock
     * @param details information about why the shard is being locked
     * @param lockTimeoutMS the lock timeout in milliseconds
     * @return the shard lock. Call {@link ShardLock#close()} to release the lock
     */
    public ShardLock shardLock(final ShardId shardId, final String details, final long lockTimeoutMS)
        throws ShardLockObtainFailedException {
        logger.trace("acquiring node shardlock on [{}], timeout [{}], details [{}]", shardId, lockTimeoutMS, details);
        final InternalShardLock shardLock;
        final boolean acquired;
        synchronized (shardLocks) {
            if (shardLocks.containsKey(shardId)) {
                shardLock = shardLocks.get(shardId);
                shardLock.incWaitCount();
                acquired = false;
            } else {
                shardLock = new InternalShardLock(shardId, details);
                shardLocks.put(shardId, shardLock);
                acquired = true;
            }
        }
        if (acquired == false) {
            boolean success = false;
            try {
                shardLock.acquire(lockTimeoutMS, details);
                success = true;
            } finally {
                if (success == false) {
                    shardLock.decWaitCount();
                }
            }
        }
        logger.trace("successfully acquired shardlock for [{}]", shardId);
        return new ShardLock(shardId) { // new instance prevents double closing
            @Override
            protected void closeInternal() {
                shardLock.release();
                logger.trace("released shard lock for [{}]", shardId);
            }

            @Override
            public void setDetails(String details) {
                shardLock.setDetails(details);
            }
        };
    }

    /**
     * A functional interface that people can use to reference {@link #shardLock(ShardId, String, long)}
     *
     * @opensearch.api
     */
    @FunctionalInterface
    @PublicApi(since = "1.0.0")
    public interface ShardLocker {
        ShardLock lock(ShardId shardId, String lockDetails, long lockTimeoutMS) throws ShardLockObtainFailedException;
    }

    /**
     * Returns all currently lock shards.
     * <p>
     * Note: the shard ids return do not contain a valid Index UUID
     */
    public Set<ShardId> lockedShards() {
        synchronized (shardLocks) {
            return unmodifiableSet(new HashSet<>(shardLocks.keySet()));
        }
    }

    private final class InternalShardLock {
        /*
         * This class holds a mutex for exclusive access and timeout / wait semantics
         * and a reference count to cleanup the shard lock instance form the internal data
         * structure if nobody is waiting for it. the wait count is guarded by the same lock
         * that is used to mutate the map holding the shard locks to ensure exclusive access
         */
        private final Semaphore mutex = new Semaphore(1);
        private int waitCount = 1; // guarded by shardLocks
        private final ShardId shardId;
        private volatile Tuple<Long, String> lockDetails;

        InternalShardLock(final ShardId shardId, final String details) {
            this.shardId = shardId;
            mutex.acquireUninterruptibly();
            lockDetails = Tuple.tuple(System.nanoTime(), details);
        }

        protected void release() {
            mutex.release();
            decWaitCount();
        }

        void incWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                waitCount++;
            }
        }

        private void decWaitCount() {
            synchronized (shardLocks) {
                assert waitCount > 0 : "waitCount is " + waitCount + " but should be > 0";
                --waitCount;
                logger.trace("shard lock wait count for {} is now [{}]", shardId, waitCount);
                if (waitCount == 0) {
                    logger.trace("last shard lock wait decremented, removing lock for {}", shardId);
                    InternalShardLock remove = shardLocks.remove(shardId);
                    assert remove != null : "Removed lock was null";
                }
            }
        }

        void acquire(long timeoutInMillis, final String details) throws ShardLockObtainFailedException {
            try {
                if (mutex.tryAcquire(timeoutInMillis, TimeUnit.MILLISECONDS)) {
                    setDetails(details);
                } else {
                    final Tuple<Long, String> lockDetails = this.lockDetails; // single volatile read
                    throw new ShardLockObtainFailedException(
                        shardId,
                        "obtaining shard lock for ["
                            + details
                            + "] timed out after ["
                            + timeoutInMillis
                            + "ms], lock already held for ["
                            + lockDetails.v2()
                            + "] with age ["
                            + TimeUnit.NANOSECONDS.toMillis(System.nanoTime() - lockDetails.v1())
                            + "ms]"
                    );
                }
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new ShardLockObtainFailedException(shardId, "thread interrupted while trying to obtain shard lock", e);
            }
        }

        public void setDetails(String details) {
            lockDetails = Tuple.tuple(System.nanoTime(), details);
        }
    }

    public boolean hasNodeFile() {
        return nodePaths != null && locks != null;
    }

    /**
     * Returns an array of all of the nodes data locations.
     * @throws IllegalStateException if the node is not configured to store local locations
     */
    public Path[] nodeDataPaths() {
        assertEnvIsLocked();
        Path[] paths = new Path[nodePaths.length];
        for (int i = 0; i < paths.length; i++) {
            paths[i] = nodePaths[i].path;
        }
        return paths;
    }

    /**
     * Returns shared data path for this node environment
     */
    public Path sharedDataPath() {
        return sharedDataPath;
    }

    /**
     * returns the unique uuid describing this node. The uuid is persistent in the data folder of this node
     * and remains across restarts.
     **/
    public String nodeId() {
        // we currently only return the ID and hide the underlying nodeMetadata implementation in order to avoid
        // confusion with other "metadata" like node settings found in opensearch.yml. In future
        // we can encapsulate both (and more) in one NodeMetadata (or NodeSettings) object ala IndexSettings
        return nodeMetadata.nodeId();
    }

    /**
     * Returns an array of all of the {@link NodePath}s.
     */
    public NodePath[] nodePaths() {
        assertEnvIsLocked();
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        return nodePaths;
    }

    /**
     * Returns the {@link NodePath} used for file caching.
     */
    public NodePath fileCacheNodePath() {
        assertEnvIsLocked();
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        return fileCacheNodePath;
    }

    public int getNodeLockId() {
        assertEnvIsLocked();
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        return nodeLockId;
    }

    /**
     * Returns all index paths.
     */
    public Path[] indexPaths(Index index) {
        assertEnvIsLocked();
        Path[] indexPaths = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            indexPaths[i] = nodePaths[i].resolve(index);
        }
        return indexPaths;
    }

    /**
     * Returns all shard paths excluding custom shard path. Note: Shards are only allocated on one of the
     * returned paths. The returned array may contain paths to non-existing directories.
     *
     * @see IndexSettings#hasCustomDataPath()
     * @see #resolveCustomLocation(String, ShardId)
     *
     */
    public Path[] availableShardPaths(ShardId shardId) {
        assertEnvIsLocked();
        final NodePath[] nodePaths = nodePaths();
        final Path[] shardLocations = new Path[nodePaths.length];
        for (int i = 0; i < nodePaths.length; i++) {
            shardLocations[i] = nodePaths[i].resolve(shardId);
        }
        return shardLocations;
    }

    /**
     * Returns all folder names in ${data.paths}/nodes/{node.id}/indices folder
     */
    public Set<String> availableIndexFolders() throws IOException {
        return availableIndexFolders(p -> false);
    }

    /**
     * Returns folder names in ${data.paths}/nodes/{node.id}/indices folder that don't match the given predicate.
     * @param excludeIndexPathIdsPredicate folder names to exclude
     */
    public Set<String> availableIndexFolders(Predicate<String> excludeIndexPathIdsPredicate) throws IOException {
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        Set<String> indexFolders = new HashSet<>();
        for (NodePath nodePath : nodePaths) {
            indexFolders.addAll(availableIndexFoldersForPath(nodePath, excludeIndexPathIdsPredicate));
        }
        return indexFolders;

    }

    /**
     * Return all directory names in the nodes/{node.id}/indices directory for the given node path.
     *
     * @param nodePath the path
     * @return all directories that could be indices for the given node path.
     * @throws IOException if an I/O exception occurs traversing the filesystem
     */
    public Set<String> availableIndexFoldersForPath(final NodePath nodePath) throws IOException {
        return availableIndexFoldersForPath(nodePath, p -> false);
    }

    /**
     * Return directory names in the nodes/{node.id}/indices directory for the given node path that don't match the given predicate.
     *
     * @param nodePath the path
     * @param excludeIndexPathIdsPredicate folder names to exclude
     * @return all directories that could be indices for the given node path.
     * @throws IOException if an I/O exception occurs traversing the filesystem
     */
    public Set<String> availableIndexFoldersForPath(final NodePath nodePath, Predicate<String> excludeIndexPathIdsPredicate)
        throws IOException {
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Set<String> indexFolders = new HashSet<>();
        Path indicesLocation = nodePath.indicesPath;
        if (Files.isDirectory(indicesLocation)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indicesLocation)) {
                for (Path index : stream) {
                    final String fileName = index.getFileName().toString();
                    if (excludeIndexPathIdsPredicate.test(fileName) == false && Files.isDirectory(index)) {
                        indexFolders.add(fileName);
                    }
                }
            }
        }
        return indexFolders;
    }

    /**
     * Resolves all existing paths to <code>indexFolderName</code> in ${data.paths}/nodes/{node.id}/indices
     */
    public Path[] resolveIndexFolder(String indexFolderName) {
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        List<Path> paths = new ArrayList<>(nodePaths.length);
        for (NodePath nodePath : nodePaths) {
            Path indexFolder = nodePath.indicesPath.resolve(indexFolderName);
            if (Files.exists(indexFolder)) {
                paths.add(indexFolder);
            }
        }
        return paths.toArray(new Path[0]);
    }

    /**
     * Tries to find all allocated shards for the given index
     * on the current node. NOTE: This methods is prone to race-conditions on the filesystem layer since it might not
     * see directories created concurrently or while it's traversing.
     * @param index the index to filter shards
     * @return a set of shard IDs
     * @throws IOException if an IOException occurs
     */
    public Set<ShardId> findAllShardIds(final Index index) throws IOException {
        assert index != null;
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Set<ShardId> shardIds = new HashSet<>();
        final String indexUniquePathId = index.getUUID();
        for (final NodePath nodePath : nodePaths) {
            shardIds.addAll(findAllShardsForIndex(nodePath.indicesPath.resolve(indexUniquePathId), index));
        }
        return shardIds;
    }

    /**
     * Find all the shards for this index, returning a map of the {@code NodePath} to the number of shards on that path
     * @param index the index by which to filter shards
     * @return a map of NodePath to count of the shards for the index on that path
     * @throws IOException if an IOException occurs
     */
    public Map<NodePath, Long> shardCountPerPath(final Index index) throws IOException {
        assert index != null;
        if (nodePaths == null || locks == null) {
            throw new IllegalStateException("node is not configured to store local location");
        }
        assertEnvIsLocked();
        final Map<NodePath, Long> shardCountPerPath = new HashMap<>();
        final String indexUniquePathId = index.getUUID();
        for (final NodePath nodePath : nodePaths) {
            Path indexLocation = nodePath.indicesPath.resolve(indexUniquePathId);
            if (Files.isDirectory(indexLocation)) {
                shardCountPerPath.put(nodePath, (long) findAllShardsForIndex(indexLocation, index).size());
            }
        }
        return shardCountPerPath;
    }

    private static Set<ShardId> findAllShardsForIndex(Path indexPath, Index index) throws IOException {
        assert indexPath.getFileName().toString().equals(index.getUUID());
        Set<ShardId> shardIds = new HashSet<>();
        if (Files.isDirectory(indexPath)) {
            try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                for (Path shardPath : stream) {
                    String fileName = shardPath.getFileName().toString();
                    if (Files.isDirectory(shardPath) && fileName.chars().allMatch(Character::isDigit)) {
                        int shardId = Integer.parseInt(fileName);
                        ShardId id = new ShardId(index, shardId);
                        shardIds.add(id);
                    }
                }
            }
        }
        return shardIds;
    }

    @Override
    public void close() {
        if (closed.compareAndSet(false, true) && locks != null) {
            for (Lock lock : locks) {
                try {
                    logger.trace("releasing lock [{}]", lock);
                    lock.close();
                } catch (IOException e) {
                    logger.trace(() -> new ParameterizedMessage("failed to release lock [{}]", lock), e);
                }
            }
        }
    }

    private void assertEnvIsLocked() {
        if (!closed.get() && locks != null) {
            for (Lock lock : locks) {
                try {
                    lock.ensureValid();
                } catch (IOException e) {
                    logger.warn("lock assertion failed", e);
                    throw new IllegalStateException("environment is not locked", e);
                }
            }
        }
    }

    /**
     * This method tries to write an empty file and moves it using an atomic move operation.
     * This method throws an {@link IllegalStateException} if this operation is
     * not supported by the filesystem. This test is executed on each of the data directories.
     * This method cleans up all files even in the case of an error.
     */
    private static void ensureAtomicMoveSupported(final NodePath[] nodePaths) throws IOException {
        for (NodePath nodePath : nodePaths) {
            assert Files.isDirectory(nodePath.path) : nodePath.path + " is not a directory";
            final Path src = nodePath.path.resolve(TEMP_FILE_NAME + ".tmp");
            final Path target = nodePath.path.resolve(TEMP_FILE_NAME + ".final");
            try {
                Files.deleteIfExists(src);
                Files.createFile(src);
                Files.move(src, target, StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException ex) {
                throw new IllegalStateException(
                    "atomic_move is not supported by the filesystem on path ["
                        + nodePath.path
                        + "] atomic_move is required for opensearch to work correctly.",
                    ex
                );
            } finally {
                try {
                    Files.deleteIfExists(src);
                } finally {
                    Files.deleteIfExists(target);
                }
            }
        }
    }

    private void ensureNoShardData(final NodePath[] nodePaths) throws IOException {
        List<Path> shardDataPaths = collectShardDataPaths(nodePaths);
        if (shardDataPaths.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "node does not have the %s role but has shard data: %s. Use 'opensearch-node repurpose' tool to clean up",
                DiscoveryNodeRole.DATA_ROLE.roleName(),
                shardDataPaths
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Throws an exception if cache exists on a non-search node.
     */
    private void ensureNoFileCacheData(final NodePath fileCacheNodePath) throws IOException {
        List<Path> cacheDataPaths = collectFileCacheDataPath(fileCacheNodePath);
        if (cacheDataPaths.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "node does not have the %s role but has data within node search cache: %s. Use 'opensearch-node repurpose' tool to clean up",
                DiscoveryNodeRole.SEARCH_ROLE.roleName(),
                cacheDataPaths
            );
            throw new IllegalStateException(message);
        }
    }

    private void ensureNoIndexMetadata(final NodePath[] nodePaths) throws IOException {
        List<Path> indexMetadataPaths = collectIndexMetadataPaths(nodePaths);
        if (indexMetadataPaths.isEmpty() == false) {
            final String message = String.format(
                Locale.ROOT,
                "node does not have the %s and %s roles but has index metadata: %s. Use 'opensearch-node repurpose' tool to clean up",
                DiscoveryNodeRole.DATA_ROLE.roleName(),
                DiscoveryNodeRole.CLUSTER_MANAGER_ROLE.roleName(),
                indexMetadataPaths
            );
            throw new IllegalStateException(message);
        }
    }

    /**
     * Collect the paths containing shard data in the indicated node paths. The returned paths will point to the shard data folder.
     */
    static List<Path> collectShardDataPaths(NodePath[] nodePaths) throws IOException {
        return collectIndexSubPaths(nodePaths, NodeEnvironment::isShardPath);
    }

    /**
     * Collect the paths containing index meta data in the indicated node paths. The returned paths will point to the
     * {@link MetadataStateFormat#STATE_DIR_NAME} folder
     */
    static List<Path> collectIndexMetadataPaths(NodePath[] nodePaths) throws IOException {
        return collectIndexSubPaths(nodePaths, NodeEnvironment::isIndexMetadataPath);
    }

    private static List<Path> collectIndexSubPaths(NodePath[] nodePaths, Predicate<Path> subPathPredicate) throws IOException {
        List<Path> indexSubPaths = new ArrayList<>();
        for (NodePath nodePath : nodePaths) {
            Path indicesPath = nodePath.indicesPath;
            if (Files.isDirectory(indicesPath)) {
                try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(indicesPath)) {
                    for (Path indexPath : indexStream) {
                        if (Files.isDirectory(indexPath)) {
                            try (Stream<Path> shardStream = Files.list(indexPath)) {
                                shardStream.filter(subPathPredicate).map(Path::toAbsolutePath).forEach(indexSubPaths::add);
                            }
                        }
                    }
                }
            }
        }

        return indexSubPaths;
    }

    private static boolean isShardPath(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().chars().allMatch(Character::isDigit);
    }

    private static boolean isIndexMetadataPath(Path path) {
        return Files.isDirectory(path) && path.getFileName().toString().equals(MetadataStateFormat.STATE_DIR_NAME);
    }

    /**
     * Collect the path containing cache data in the indicated cache node path.
     * The returned paths will point to the shard data folder.
     */
    public static List<Path> collectFileCacheDataPath(NodePath fileCacheNodePath) throws IOException {
        // Structure is: <file cache path>/<index uuid>/<shard id>/...
        List<Path> indexSubPaths = new ArrayList<>();
        Path fileCachePath = fileCacheNodePath.fileCachePath;
        if (Files.isDirectory(fileCachePath)) {
            try (DirectoryStream<Path> indexStream = Files.newDirectoryStream(fileCachePath)) {
                for (Path indexPath : indexStream) {
                    if (Files.isDirectory(indexPath)) {
                        try (Stream<Path> shardStream = Files.list(indexPath)) {
                            shardStream.filter(NodeEnvironment::isShardPath).map(Path::toAbsolutePath).forEach(indexSubPaths::add);
                        }
                    }
                }
            }
        }

        return indexSubPaths;
    }

    /**
     * Resolve the custom path for a index's shard.
     */
    public static Path resolveBaseCustomLocation(String customDataPath, Path sharedDataPath, int nodeLockId) {
        if (!Strings.isNullOrEmpty(customDataPath)) {
            // This assert is because this should be caught by MetadataCreateIndexService
            assert sharedDataPath != null;
            return sharedDataPath.resolve(customDataPath).resolve(Integer.toString(nodeLockId));
        } else {
            throw new IllegalArgumentException("no custom " + IndexMetadata.SETTING_DATA_PATH + " setting available");
        }
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetadata.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param customDataPath the custom data path
     */
    private Path resolveIndexCustomLocation(String customDataPath, String indexUUID) {
        return resolveIndexCustomLocation(customDataPath, indexUUID, sharedDataPath, nodeLockId);
    }

    private static Path resolveIndexCustomLocation(String customDataPath, String indexUUID, Path sharedDataPath, int nodeLockId) {
        return resolveBaseCustomLocation(customDataPath, sharedDataPath, nodeLockId).resolve(indexUUID);
    }

    /**
     * Resolve the file cache path for remote shards.
     *
     * @param fileCachePath the file cache path
     * @param shardId shard to resolve the path to
     */
    public Path resolveFileCacheLocation(final Path fileCachePath, final ShardId shardId) {
        return fileCachePath.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
    }

    /**
     * Resolve the custom path for a index's shard.
     * Uses the {@code IndexMetadata.SETTING_DATA_PATH} setting to determine
     * the root path for the index.
     *
     * @param customDataPath the custom data path
     * @param shardId shard to resolve the path to
     */
    public Path resolveCustomLocation(String customDataPath, final ShardId shardId) {
        return resolveCustomLocation(customDataPath, shardId, sharedDataPath, nodeLockId);
    }

    public static Path resolveCustomLocation(String customDataPath, final ShardId shardId, Path sharedDataPath, int nodeLockId) {
        return resolveIndexCustomLocation(customDataPath, shardId.getIndex().getUUID(), sharedDataPath, nodeLockId).resolve(
            Integer.toString(shardId.id())
        );
    }

    /**
     * Returns the {@code NodePath.path} for this shard.
     */
    public static Path shardStatePathToDataPath(Path shardPath) {
        int count = shardPath.getNameCount();

        // Sanity check:
        assert Integer.parseInt(shardPath.getName(count - 1).toString()) >= 0;
        assert "indices".equals(shardPath.getName(count - 3).toString());

        return shardPath.getParent().getParent().getParent();
    }

    /**
     * This is a best effort to ensure that we actually have write permissions to write in all our data directories.
     * This prevents disasters if nodes are started under the wrong username etc.
     */
    private void assertCanWrite() throws IOException {
        for (Path path : nodeDataPaths()) { // check node-paths are writable
            tryWriteTempFile(path);
        }
        for (String indexFolderName : this.availableIndexFolders()) {
            for (Path indexPath : this.resolveIndexFolder(indexFolderName)) { // check index paths are writable
                Path indexStatePath = indexPath.resolve(MetadataStateFormat.STATE_DIR_NAME);
                tryWriteTempFile(indexStatePath);
                tryWriteTempFile(indexPath);
                try (DirectoryStream<Path> stream = Files.newDirectoryStream(indexPath)) {
                    for (Path shardPath : stream) {
                        String fileName = shardPath.getFileName().toString();
                        if (Files.isDirectory(shardPath) && fileName.chars().allMatch(Character::isDigit)) {
                            Path indexDir = shardPath.resolve(ShardPath.INDEX_FOLDER_NAME);
                            Path statePath = shardPath.resolve(MetadataStateFormat.STATE_DIR_NAME);
                            Path translogDir = shardPath.resolve(ShardPath.TRANSLOG_FOLDER_NAME);
                            tryWriteTempFile(indexDir);
                            tryWriteTempFile(translogDir);
                            tryWriteTempFile(statePath);
                            tryWriteTempFile(shardPath);
                        }
                    }
                }
            }
        }
    }

    // package private for testing
    static final String TEMP_FILE_NAME = ".opensearch_temp_file";

    private static void tryWriteTempFile(Path path) throws IOException {
        if (Files.exists(path)) {
            Path resolve = path.resolve(TEMP_FILE_NAME);
            try {
                // delete any lingering file from a previous failure
                Files.deleteIfExists(resolve);
                Files.createFile(resolve);
                Files.delete(resolve);
            } catch (IOException ex) {
                throw new IOException("failed to test writes in data directory [" + path + "] write permission is required", ex);
            }
        }
    }

    /**
     * Use {@link org.opensearch.index.store.IndexStoreListener} instead. This interface is kept around on 2.x to avoid breaking changes.
     *
     * @opensearch.internal
     */
    @Deprecated(forRemoval = true, since = "2.19.0")
    public interface IndexStoreListener extends org.opensearch.index.store.IndexStoreListener {
        IndexStoreListener EMPTY = new IndexStoreListener() {
        };
    }
}
