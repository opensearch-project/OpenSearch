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

package org.opensearch.index.store;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.apache.lucene.store.FileSwitchDirectory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockFactory;
import org.apache.lucene.store.MMapDirectory;
import org.apache.lucene.store.NIOFSDirectory;
import org.apache.lucene.store.NativeFSLockFactory;
import org.apache.lucene.store.SimpleFSLockFactory;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.util.io.IOUtils;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.distributed.DistributedSegmentDirectory;
import org.opensearch.index.store.distributed.PrimaryTermAwareDirectoryWrapper;
import org.opensearch.plugins.IndexStorePlugin;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashSet;
import java.util.Set;
import java.util.function.BiPredicate;

/**
 * Factory for a filesystem directory
 *
 * @opensearch.internal
 */
public class FsDirectoryFactory implements IndexStorePlugin.DirectoryFactory {

    public static final Setting<LockFactory> INDEX_LOCK_FACTOR_SETTING = new Setting<>("index.store.fs.fs_lock", "native", (s) -> {
        switch (s) {
            case "native":
                return NativeFSLockFactory.INSTANCE;
            case "simple":
                return SimpleFSLockFactory.INSTANCE;
            default:
                throw new IllegalArgumentException("unrecognized [index.store.fs.fs_lock] \"" + s + "\": must be native or simple");
        } // can we set on both - node and index level, some nodes might be running on NFS so they might need simple rather than native
    }, Property.IndexScope, Property.NodeScope);

    @Override
    public Directory newDirectory(IndexSettings indexSettings, ShardPath path) throws IOException {
        final Path location = path.resolveIndex();
        final LockFactory lockFactory = indexSettings.getValue(INDEX_LOCK_FACTOR_SETTING);
        Files.createDirectories(location);
        return newFSDirectory(location, lockFactory, indexSettings);
    }

    protected Directory newFSDirectory(Path location, LockFactory lockFactory, IndexSettings indexSettings) throws IOException {
        final String storeType = indexSettings.getSettings()
            .get(IndexModule.INDEX_STORE_TYPE_SETTING.getKey(), IndexModule.Type.FS.getSettingsKey());
        IndexModule.Type type;
        if (IndexModule.Type.FS.match(storeType)) {
            type = IndexModule.defaultStoreType(IndexModule.NODE_STORE_ALLOW_MMAP.get(indexSettings.getNodeSettings()));
        } else {
            type = IndexModule.Type.fromSettingsKey(storeType);
        }
        Set<String> preLoadExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_PRE_LOAD_SETTING));
        switch (type) {
            case HYBRIDFS:
                // Create primary directory
                final FSDirectory primaryDirectory = new NIOFSDirectory(location, lockFactory);
                
                return new PrimaryTermAwareDirectoryWrapper(primaryDirectory, location);
                // // Check if primary term routing should be enabled
                // boolean enablePrimaryTermRouting = indexSettings.getSettings()
                //     .getAsBoolean("index.store.distributed_segment.enable_primary_term_routing", true);
                
                // if (enablePrimaryTermRouting) {
                //     // Use wrapper that can be configured for primary term routing later
                //     return new PrimaryTermAwareDirectoryWrapper(primaryDirectory, location);
                // } else {
                //     // Use legacy hash-based routing
                //     return new DistributedSegmentDirectory(primaryDirectory, location);
                // }

                // final Set<String> nioExtensions = new HashSet<>(indexSettings.getValue(IndexModule.INDEX_STORE_HYBRID_NIO_EXTENSIONS));
                // if (primaryDirectory instanceof MMapDirectory) {
                //     MMapDirectory mMapDirectory = (MMapDirectory) primaryDirectory;
                //     return new HybridDirectory(lockFactory, setPreload(mMapDirectory, preLoadExtensions), nioExtensions);
                // } else {
                //     return primaryDirectory;
                // }
            case MMAPFS:
                return setPreload(new MMapDirectory(location, lockFactory), preLoadExtensions);
            // simplefs was removed in Lucene 9; support for enum is maintained for bwc
            case SIMPLEFS:
            case NIOFS:
                return new NIOFSDirectory(location, lockFactory);
            default:
                throw new AssertionError("unexpected built-in store type [" + type + "]");
        }
    }

    public static MMapDirectory setPreload(MMapDirectory mMapDirectory, Set<String> preLoadExtensions) throws IOException {
        if (preLoadExtensions.isEmpty() == false) {
            mMapDirectory.setPreload(createPreloadPredicate(preLoadExtensions));
        }
        return mMapDirectory;
    }

    /**
     * Returns true iff the directory is a hybrid fs directory
     */
    public static boolean isHybridFs(Directory directory) {
        Directory unwrap = FilterDirectory.unwrap(directory);
        return unwrap instanceof HybridDirectory;
    }

    static BiPredicate<String, IOContext> createPreloadPredicate(Set<String> preLoadExtensions) {
        if (preLoadExtensions.contains("*")) {
            return MMapDirectory.ALL_FILES;
        } else {
            return (s, f) -> {
                int dotIndex = s.lastIndexOf('.');
                if (dotIndex > 0) {
                    return preLoadExtensions.contains(s.substring(dotIndex + 1));
                }
                return false;
            };
        }
    }

    /**
     * A hybrid directory implementation
     *
     * @opensearch.internal
     */
    static final class HybridDirectory extends NIOFSDirectory {
        private final MMapDirectory delegate;
        private final Set<String> nioExtensions;

        HybridDirectory(LockFactory lockFactory, MMapDirectory delegate, Set<String> nioExtensions) throws IOException {
            super(delegate.getDirectory(), lockFactory);
            this.delegate = delegate;
            this.nioExtensions = nioExtensions;
        }

        @Override
        public IndexInput openInput(String name, IOContext context) throws IOException {
            if (useDelegate(name)) {
                // we need to do these checks on the outer directory since the inner doesn't know about pending deletes
                ensureOpen();
                ensureCanRead(name);
                // we only use the mmap to open inputs. Everything else is managed by the NIOFSDirectory otherwise
                // we might run into trouble with files that are pendingDelete in one directory but still
                // listed in listAll() from the other. We on the other hand don't want to list files from both dirs
                // and intersect for perf reasons.
                return delegate.openInput(name, context);
            } else {
                return super.openInput(name, context);
            }
        }

        boolean useDelegate(String name) {
            final String extension = FileSwitchDirectory.getExtension(name);
            return nioExtensions.contains(extension) == false;
        }

        @Override
        public void close() throws IOException {
            IOUtils.close(super::close, delegate);
        }

        MMapDirectory getDelegate() {
            return delegate;
        }
    }
}
