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

package org.opensearch.test.store;

import com.carrotsearch.randomizedtesting.generators.RandomPicks;

import org.apache.logging.log4j.Logger;
import org.apache.lucene.index.CheckIndex;
import org.apache.lucene.index.IndexFileNames;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.LockObtainFailedException;
import org.apache.lucene.tests.store.BaseDirectoryWrapper;
import org.apache.lucene.tests.store.MockDirectoryWrapper;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.apache.lucene.tests.util.TestRuleMarkFailure;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.lucene.Lucene;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.settings.Setting.Property;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexModule;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.index.store.Store;
import org.opensearch.plugins.IndexStorePlugin;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Assert;

import java.io.Closeable;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

public class MockFSDirectoryFactory implements IndexStorePlugin.DirectoryFactory {
    public static final List<IndexModule.Type> FILE_SYSTEM_BASED_STORE_TYPES = Arrays.stream(IndexModule.Type.values())
        .filter(t -> (t == IndexModule.Type.REMOTE_SNAPSHOT) == false)
        .collect(Collectors.toUnmodifiableList());

    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING = Setting.doubleSetting(
        "index.store.mock.random.io_exception_rate_on_open",
        0.0d,
        0.0d,
        Property.IndexScope,
        Property.NodeScope
    );
    public static final Setting<Double> RANDOM_IO_EXCEPTION_RATE_SETTING = Setting.doubleSetting(
        "index.store.mock.random.io_exception_rate",
        0.0d,
        0.0d,
        Property.IndexScope,
        Property.NodeScope
    );
    public static final Setting<Boolean> CRASH_INDEX_SETTING = Setting.boolSetting(
        "index.store.mock.random.crash_index",
        true,
        Property.IndexScope,
        Property.NodeScope
    );

    @Override
    public Directory newDirectory(IndexSettings idxSettings, ShardPath path) throws IOException {
        Settings indexSettings = idxSettings.getSettings();
        Random random = new Random(idxSettings.getValue(OpenSearchIntegTestCase.INDEX_TEST_SEED_SETTING));
        return wrap(randomDirectoryService(random, idxSettings, path), random, indexSettings, path.getShardId());
    }

    public static void checkIndex(Logger logger, Store store, ShardId shardId) {
        if (store.tryIncRef()) {
            logger.info("start check index");
            try {
                Directory dir = store.directory();
                if (!Lucene.indexExists(dir)) {
                    return;
                }
                try {
                    BytesStreamOutput os = new BytesStreamOutput();
                    PrintStream out = new PrintStream(os, false, StandardCharsets.UTF_8.name());
                    CheckIndex.Status status = store.checkIndex(out);
                    out.flush();
                    if (!status.clean) {
                        IOException failure = new IOException(
                            "failed to check index for shard "
                                + shardId
                                + ";index files ["
                                + Arrays.toString(dir.listAll())
                                + "] os ["
                                + os.bytes().utf8ToString()
                                + "]"
                        );
                        OpenSearchTestCase.checkIndexFailures.add(failure);
                        throw failure;
                    } else {
                        if (logger.isDebugEnabled()) {
                            logger.debug("check index [success]\n{}", os.bytes().utf8ToString());
                        }
                    }
                } catch (LockObtainFailedException e) {
                    IllegalStateException failure = new IllegalStateException("IndexWriter is still open on shard " + shardId, e);
                    OpenSearchTestCase.checkIndexFailures.add(failure);
                    throw failure;
                }
            } catch (Exception e) {
                logger.warn("failed to check index", e);
            } finally {
                logger.info("end check index");
                store.decRef();
            }
        }
    }

    private Directory wrap(Directory dir, Random random, Settings indexSettings, ShardId shardId) {

        double randomIOExceptionRate = RANDOM_IO_EXCEPTION_RATE_SETTING.get(indexSettings);
        double randomIOExceptionRateOnOpen = RANDOM_IO_EXCEPTION_RATE_ON_OPEN_SETTING.get(indexSettings);
        random.nextInt(shardId.getId() + 1); // some randomness per shard
        MockDirectoryWrapper.Throttling throttle = MockDirectoryWrapper.Throttling.NEVER;
        boolean crashIndex = CRASH_INDEX_SETTING.get(indexSettings);
        final OpenSearchMockDirectoryWrapper w = new OpenSearchMockDirectoryWrapper(random, dir, crashIndex);
        w.setRandomIOExceptionRate(randomIOExceptionRate);
        w.setRandomIOExceptionRateOnOpen(randomIOExceptionRateOnOpen);
        w.setThrottling(throttle);
        w.setCheckIndexOnClose(false); // we do this on the index level
        // TODO: make this test robust to virus scanner
        w.setAssertNoDeleteOpenFile(false);
        w.setUseSlowOpenClosers(false);
        LuceneTestCase.closeAfterSuite(new CloseableDirectory(w));
        return w;
    }

    private Directory randomDirectoryService(Random random, IndexSettings indexSettings, ShardPath path) throws IOException {
        final IndexMetadata build = IndexMetadata.builder(indexSettings.getIndexMetadata())
            .settings(
                Settings.builder()
                    // don't use the settings from indexSettings#getSettings() they are merged with node settings and might contain
                    // secure settings that should not be copied in here since the new IndexSettings ctor below will barf if we do
                    .put(indexSettings.getIndexMetadata().getSettings())
                    .put(
                        IndexModule.INDEX_STORE_TYPE_SETTING.getKey(),
                        RandomPicks.randomFrom(random, FILE_SYSTEM_BASED_STORE_TYPES).getSettingsKey()
                    )
            )
            .build();
        final IndexSettings newIndexSettings = new IndexSettings(build, indexSettings.getNodeSettings());
        return new FsDirectoryFactory().newDirectory(newIndexSettings, path);
    }

    public static final class OpenSearchMockDirectoryWrapper extends MockDirectoryWrapper {

        private final boolean crash;

        public OpenSearchMockDirectoryWrapper(Random random, Directory delegate, boolean crash) {
            super(random, delegate);
            this.crash = crash;
        }

        @Override
        public synchronized void crash() throws IOException {
            if (crash) {
                super.crash();
            }
        }

        // temporary override until LUCENE-8735 is integrated
        @Override
        public Set<String> getPendingDeletions() throws IOException {
            return in.getPendingDeletions();
        }

        // In remote store feature, the upload flow is async and IndexInput can be opened and closed
        // by different threads, so we always use IOContext.DEFAULT.
        // But MockDirectoryWrapper throws an exception if segments_N fil is opened with any IOContext other than READONCE.
        // Following change is temporary override to avoid the test failures. We should fix the multiple thread access
        // in remote store upload flow.
        @Override
        public synchronized IndexInput openInput(String name, IOContext context) throws IOException {
            if (name.startsWith(IndexFileNames.SEGMENTS)) {
                context = IOContext.READONCE;
            }
            return super.openInput(name, context);
        }
    }

    static final class CloseableDirectory implements Closeable {
        private final BaseDirectoryWrapper dir;
        private final TestRuleMarkFailure failureMarker;

        CloseableDirectory(BaseDirectoryWrapper dir) {
            this.dir = dir;
            this.failureMarker = OpenSearchTestCase.getSuiteFailureMarker();
        }

        @Override
        public void close() {
            // We only attempt to check open/closed state if there were no other test
            // failures.
            try {
                if (failureMarker.wasSuccessful() && dir.isOpen()) {
                    Assert.fail("Directory not closed: " + dir);
                }
            } finally {
                // TODO: perform real close of the delegate: LUCENE-4058
                // dir.close();
            }
        }
    }
}
