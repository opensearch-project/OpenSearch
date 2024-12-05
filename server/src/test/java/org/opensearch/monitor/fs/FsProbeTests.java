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

package org.opensearch.monitor.fs;

import org.apache.lucene.util.Constants;
import org.opensearch.common.collect.Tuple;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.breaker.CircuitBreaker;
import org.opensearch.core.common.breaker.NoopCircuitBreaker;
import org.opensearch.core.common.unit.ByteSizeUnit;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.env.NodeEnvironment.NodePath;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.index.store.remote.filecache.FileCacheFactory;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttributeView;
import java.nio.file.attribute.FileStoreAttributeView;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.function.Supplier;

import static org.opensearch.monitor.fs.FsProbe.adjustForHugeFilesystems;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.lessThan;
import static org.hamcrest.Matchers.not;

public class FsProbeTests extends OpenSearchTestCase {

    public void testFsInfo() throws IOException {

        try (NodeEnvironment env = newNodeEnvironment()) {
            FsProbe probe = new FsProbe(env, null);

            FsInfo stats = probe.stats(null);
            assertNotNull(stats);
            assertThat(stats.getTimestamp(), greaterThan(0L));

            if (Constants.LINUX) {
                assertNotNull(stats.getIoStats());
                assertNotNull(stats.getIoStats().devicesStats);
                for (int i = 0; i < stats.getIoStats().devicesStats.length; i++) {
                    final FsInfo.DeviceStats deviceStats = stats.getIoStats().devicesStats[i];
                    assertNotNull(deviceStats);
                    assertThat(deviceStats.currentReadsCompleted, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousReadsCompleted, equalTo(-1L));
                    assertThat(deviceStats.currentSectorsRead, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousSectorsRead, equalTo(-1L));
                    assertThat(deviceStats.currentWritesCompleted, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousWritesCompleted, equalTo(-1L));
                    assertThat(deviceStats.currentSectorsWritten, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousSectorsWritten, equalTo(-1L));
                    assertThat(deviceStats.currentReadTime, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousReadTime, greaterThanOrEqualTo(-1L));
                    assertThat(deviceStats.currentWriteTime, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousWriteTime, greaterThanOrEqualTo(-1L));
                    assertThat(deviceStats.currentQueueSize, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousQueueSize, greaterThanOrEqualTo(-1L));
                    assertThat(deviceStats.currentIOTime, greaterThanOrEqualTo(0L));
                    assertThat(deviceStats.previousIOTime, greaterThanOrEqualTo(-1L));
                }
            } else {
                assertNull(stats.getIoStats());
            }

            FsInfo.Path total = stats.getTotal();
            assertNotNull(total);
            assertThat(total.total, greaterThan(0L));
            assertThat(total.free, greaterThan(0L));
            assertThat(total.available, greaterThan(0L));

            for (FsInfo.Path path : stats) {
                assertNotNull(path);
                assertThat(path.getPath(), is(not(emptyOrNullString())));
                assertThat(path.getMount(), is(not(emptyOrNullString())));
                assertThat(path.getType(), is(not(emptyOrNullString())));
                assertThat(path.total, greaterThan(0L));
                assertThat(path.free, greaterThan(0L));
                assertThat(path.available, greaterThan(0L));
                assertTrue(path.fileCacheReserved == 0);
                assertTrue(path.fileCacheUtilized == 0);
            }
        }
    }

    public void testFsCacheInfo() throws IOException {
        Settings settings = Settings.builder().put("node.roles", "search").build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            ByteSizeValue gbByteSizeValue = new ByteSizeValue(1, ByteSizeUnit.GB);
            env.fileCacheNodePath().fileCacheReservedSize = gbByteSizeValue;
            FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(
                gbByteSizeValue.getBytes(),
                16,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST)
            );
            FsProbe probe = new FsProbe(env, fileCache);
            FsInfo stats = probe.stats(null);
            assertNotNull(stats);
            assertTrue(stats.getTimestamp() > 0L);
            FsInfo.Path total = stats.getTotal();
            assertNotNull(total);
            assertTrue(total.total > 0L);
            assertTrue(total.free > 0L);
            assertTrue(total.available > 0L);
            assertTrue(total.fileCacheReserved > 0L);
            assertTrue((total.free - total.available) >= total.fileCacheReserved);

            for (FsInfo.Path path : stats) {
                assertNotNull(path);
                assertFalse(path.getPath().isEmpty());
                assertFalse(path.getMount().isEmpty());
                assertFalse(path.getType().isEmpty());
                assertTrue(path.total > 0L);
                assertTrue(path.free > 0L);
                assertTrue(path.available > 0L);

                if (path.fileCacheReserved > -1L) {
                    assertTrue(path.free - path.available >= path.fileCacheReserved);
                }
            }
        }
    }

    public void testFsInfoWhenFileCacheOccupied() throws IOException {
        Settings settings = Settings.builder().putList("node.roles", "search", "data").build();
        try (NodeEnvironment env = newNodeEnvironment(settings)) {
            // Use the total space as reserved space to simulate the situation where the cache space is occupied
            final long totalSpace = adjustForHugeFilesystems(env.fileCacheNodePath().fileStore.getTotalSpace());
            ByteSizeValue gbByteSizeValue = new ByteSizeValue(totalSpace, ByteSizeUnit.BYTES);
            env.fileCacheNodePath().fileCacheReservedSize = gbByteSizeValue;
            FileCache fileCache = FileCacheFactory.createConcurrentLRUFileCache(
                gbByteSizeValue.getBytes(),
                16,
                new NoopCircuitBreaker(CircuitBreaker.REQUEST)
            );

            FsProbe probe = new FsProbe(env, fileCache);
            FsInfo stats = probe.stats(null);
            assertNotNull(stats);
            assertTrue(stats.getTimestamp() > 0L);
            FsInfo.Path total = stats.getTotal();
            assertNotNull(total);
            assertTrue(total.total > 0L);
            assertTrue(total.free > 0L);
            assertTrue(total.fileCacheReserved > 0L);

            for (FsInfo.Path path : stats) {
                assertNotNull(path);
                assertFalse(path.getPath().isEmpty());
                assertFalse(path.getMount().isEmpty());
                assertFalse(path.getType().isEmpty());
                assertTrue(path.total > 0L);
                assertTrue(path.free > 0L);

                if (path.fileCacheReserved > 0L) {
                    assertEquals(0L, path.available);
                } else {
                    assertTrue(path.available > 0L);
                }
            }
        }
    }

    public void testFsInfoOverflow() throws Exception {
        final FsInfo.Path pathStats = new FsInfo.Path(
            "/foo/bar",
            null,
            randomNonNegativeLong(),
            randomNonNegativeLong(),
            randomNonNegativeLong()
        );

        addUntilOverflow(pathStats, p -> p.total, "total", () -> new FsInfo.Path("/foo/baz", null, randomNonNegativeLong(), 0, 0));

        addUntilOverflow(pathStats, p -> p.free, "free", () -> new FsInfo.Path("/foo/baz", null, 0, randomNonNegativeLong(), 0));

        addUntilOverflow(pathStats, p -> p.available, "available", () -> new FsInfo.Path("/foo/baz", null, 0, 0, randomNonNegativeLong()));

        // even after overflowing these should not be negative
        assertThat(pathStats.total, greaterThan(0L));
        assertThat(pathStats.free, greaterThan(0L));
        assertThat(pathStats.available, greaterThan(0L));
    }

    private void addUntilOverflow(
        final FsInfo.Path pathStats,
        final Function<FsInfo.Path, Long> getter,
        final String field,
        final Supplier<FsInfo.Path> supplier
    ) {
        FsInfo.Path pathToAdd = supplier.get();
        while ((getter.apply(pathStats) + getter.apply(pathToAdd)) > 0) {
            // add a path to increase the total bytes until it overflows
            logger.info(
                "--> adding {} bytes to {}, {} will be: {}",
                getter.apply(pathToAdd),
                getter.apply(pathStats),
                field,
                getter.apply(pathStats) + getter.apply(pathToAdd)
            );
            pathStats.add(pathToAdd);
            pathToAdd = supplier.get();
        }
        // this overflows
        logger.info(
            "--> adding {} bytes to {}, {} will be: {}",
            getter.apply(pathToAdd),
            getter.apply(pathStats),
            field,
            getter.apply(pathStats) + getter.apply(pathToAdd)
        );
        assertThat(getter.apply(pathStats) + getter.apply(pathToAdd), lessThan(0L));
        pathStats.add(pathToAdd);
    }

    public void testIoStats() {
        final AtomicReference<List<String>> diskStats = new AtomicReference<>();
        diskStats.set(
            Arrays.asList(
                " 259       0 nvme0n1 336609 0 7923613 82813 10264051 0 182983933 52451441 0 2970886 52536260",
                " 259       1 nvme0n1p1 602 0 9919 131 1 0 1 0 0 19 131",
                " 259       2 nvme0n1p2 186 0 8626 18 24 0 60 20 0 34 38",
                " 259       3 nvme0n1p3 335733 0 7901620 82658 9592875 0 182983872 50843431 0 1737726 50926087",
                " 253       0 dm-0 287716 0 7184666 33457 8398869 0 118857776 18730966 0 1918440 18767169",
                " 253       1 dm-1 112 0 4624 13 0 0 0 0 0 5 13",
                " 253       2 dm-2 47802 0 710658 49312 1371977 0 64126096 33730596 0 1058193 33781827"
            )
        );

        final FsProbe probe = new FsProbe(null, null) {
            @Override
            List<String> readProcDiskStats() throws IOException {
                return diskStats.get();
            }
        };

        final Set<Tuple<Integer, Integer>> devicesNumbers = new HashSet<>();
        devicesNumbers.add(Tuple.tuple(253, 0));
        devicesNumbers.add(Tuple.tuple(253, 2));
        final FsInfo.IoStats first = probe.ioStats(devicesNumbers, null);
        assertNotNull(first);
        assertThat(first.devicesStats[0].majorDeviceNumber, equalTo(253));
        assertThat(first.devicesStats[0].minorDeviceNumber, equalTo(0));
        assertThat(first.devicesStats[0].deviceName, equalTo("dm-0"));
        assertThat(first.devicesStats[0].currentReadsCompleted, equalTo(287716L));
        assertThat(first.devicesStats[0].previousReadsCompleted, equalTo(-1L));
        assertThat(first.devicesStats[0].currentSectorsRead, equalTo(7184666L));
        assertThat(first.devicesStats[0].previousSectorsRead, equalTo(-1L));
        assertThat(first.devicesStats[0].currentWritesCompleted, equalTo(8398869L));
        assertThat(first.devicesStats[0].previousWritesCompleted, equalTo(-1L));
        assertThat(first.devicesStats[0].currentSectorsWritten, equalTo(118857776L));
        assertThat(first.devicesStats[0].previousSectorsWritten, equalTo(-1L));

        assertEquals(33457, first.devicesStats[0].currentReadTime);
        assertEquals(-1, first.devicesStats[0].previousReadTime);
        assertEquals(18730966, first.devicesStats[0].currentWriteTime);
        assertEquals(-1, first.devicesStats[0].previousWriteTime);
        assertEquals(18767169, first.devicesStats[0].currentQueueSize);
        assertEquals(-1, first.devicesStats[0].previousQueueSize);
        assertEquals(1918440, first.devicesStats[0].currentIOTime);
        assertEquals(-1, first.devicesStats[0].previousIOTime);

        assertThat(first.devicesStats[1].majorDeviceNumber, equalTo(253));
        assertThat(first.devicesStats[1].minorDeviceNumber, equalTo(2));
        assertThat(first.devicesStats[1].deviceName, equalTo("dm-2"));
        assertThat(first.devicesStats[1].currentReadsCompleted, equalTo(47802L));
        assertThat(first.devicesStats[1].previousReadsCompleted, equalTo(-1L));
        assertThat(first.devicesStats[1].currentSectorsRead, equalTo(710658L));
        assertThat(first.devicesStats[1].previousSectorsRead, equalTo(-1L));
        assertThat(first.devicesStats[1].currentWritesCompleted, equalTo(1371977L));
        assertThat(first.devicesStats[1].previousWritesCompleted, equalTo(-1L));
        assertThat(first.devicesStats[1].currentSectorsWritten, equalTo(64126096L));
        assertThat(first.devicesStats[1].previousSectorsWritten, equalTo(-1L));

        assertEquals(49312, first.devicesStats[1].currentReadTime);
        assertEquals(-1, first.devicesStats[1].previousReadTime);
        assertEquals(33730596, first.devicesStats[1].currentWriteTime);
        assertEquals(-1, first.devicesStats[1].previousWriteTime);
        assertEquals(33781827, first.devicesStats[1].currentQueueSize);
        assertEquals(-1, first.devicesStats[1].previousQueueSize);
        assertEquals(1058193, first.devicesStats[1].currentIOTime);
        assertEquals(-1, first.devicesStats[1].previousIOTime);

        diskStats.set(
            Arrays.asList(
                " 259       0 nvme0n1 336870 0 7928397 82876 10264393 0 182986405 52451610 0 2971042 52536492",
                " 259       1 nvme0n1p1 602 0 9919 131 1 0 1 0 0 19 131",
                " 259       2 nvme0n1p2 186 0 8626 18 24 0 60 20 0 34 38",
                " 259       3 nvme0n1p3 335994 0 7906404 82721 9593184 0 182986344 50843529 0 1737840 50926248",
                " 253       0 dm-0 287734 0 7185242 33464 8398869 0 118857776 18730966 0 1918444 18767176",
                " 253       1 dm-1 112 0 4624 13 0 0 0 0 0 5 13",
                " 253       2 dm-2 48045 0 714866 49369 1372291 0 64128568 33730766 0 1058347 33782056"
            )
        );

        final FsInfo previous = new FsInfo(System.currentTimeMillis(), first, new FsInfo.Path[0]);
        final FsInfo.IoStats second = probe.ioStats(devicesNumbers, previous);
        assertNotNull(second);
        assertThat(second.devicesStats[0].majorDeviceNumber, equalTo(253));
        assertThat(second.devicesStats[0].minorDeviceNumber, equalTo(0));
        assertThat(second.devicesStats[0].deviceName, equalTo("dm-0"));
        assertThat(second.devicesStats[0].currentReadsCompleted, equalTo(287734L));
        assertThat(second.devicesStats[0].previousReadsCompleted, equalTo(287716L));
        assertThat(second.devicesStats[0].currentSectorsRead, equalTo(7185242L));
        assertThat(second.devicesStats[0].previousSectorsRead, equalTo(7184666L));
        assertThat(second.devicesStats[0].currentWritesCompleted, equalTo(8398869L));
        assertThat(second.devicesStats[0].previousWritesCompleted, equalTo(8398869L));
        assertThat(second.devicesStats[0].currentSectorsWritten, equalTo(118857776L));
        assertThat(second.devicesStats[0].previousSectorsWritten, equalTo(118857776L));

        assertEquals(33464, second.devicesStats[0].currentReadTime);
        assertEquals(33457, second.devicesStats[0].previousReadTime);
        assertEquals(18730966, second.devicesStats[0].currentWriteTime);
        assertEquals(18730966, second.devicesStats[0].previousWriteTime);
        assertEquals(18767176, second.devicesStats[0].currentQueueSize);
        assertEquals(18767169, second.devicesStats[0].previousQueueSize);
        assertEquals(1918444, second.devicesStats[0].currentIOTime);
        assertEquals(1918440, second.devicesStats[0].previousIOTime);

        assertThat(second.devicesStats[1].majorDeviceNumber, equalTo(253));
        assertThat(second.devicesStats[1].minorDeviceNumber, equalTo(2));
        assertThat(second.devicesStats[1].deviceName, equalTo("dm-2"));
        assertThat(second.devicesStats[1].currentReadsCompleted, equalTo(48045L));
        assertThat(second.devicesStats[1].previousReadsCompleted, equalTo(47802L));
        assertThat(second.devicesStats[1].currentSectorsRead, equalTo(714866L));
        assertThat(second.devicesStats[1].previousSectorsRead, equalTo(710658L));
        assertThat(second.devicesStats[1].currentWritesCompleted, equalTo(1372291L));
        assertThat(second.devicesStats[1].previousWritesCompleted, equalTo(1371977L));
        assertThat(second.devicesStats[1].currentSectorsWritten, equalTo(64128568L));
        assertThat(second.devicesStats[1].previousSectorsWritten, equalTo(64126096L));

        assertEquals(49369, second.devicesStats[1].currentReadTime);
        assertEquals(49312, second.devicesStats[1].previousReadTime);
        assertEquals(33730766, second.devicesStats[1].currentWriteTime);
        assertEquals(33730596, second.devicesStats[1].previousWriteTime);
        assertEquals(33781827, first.devicesStats[1].currentQueueSize);
        assertEquals(-1L, first.devicesStats[1].previousQueueSize);
        assertEquals(1058193, first.devicesStats[1].currentIOTime);
        assertEquals(-1L, first.devicesStats[1].previousIOTime);

        assertThat(second.totalOperations, equalTo(575L));
        assertThat(second.totalReadOperations, equalTo(261L));
        assertThat(second.totalWriteOperations, equalTo(314L));
        assertThat(second.totalReadKilobytes, equalTo(2392L));
        assertThat(second.totalWriteKilobytes, equalTo(1236L));

        assertEquals(64, second.totalReadTime);
        assertEquals(170, second.totalWriteTime);
        assertEquals(236, second.totalQueueSize);
        assertEquals(158, second.totalIOTimeInMillis);
    }

    public void testAdjustForHugeFilesystems() throws Exception {
        NodePath np = new FakeNodePath(createTempDir());
        assertThat(FsProbe.getFSInfo(np).total, greaterThanOrEqualTo(0L));
        assertThat(FsProbe.getFSInfo(np).free, greaterThanOrEqualTo(0L));
        assertThat(FsProbe.getFSInfo(np).available, greaterThanOrEqualTo(0L));
    }

    static class FakeNodePath extends NodeEnvironment.NodePath {
        public final FileStore fileStore;

        FakeNodePath(Path path) throws IOException {
            super(path);
            this.fileStore = new HugeFileStore();
        }
    }

    /**
     * Randomly returns negative values for disk space to simulate https://bugs.openjdk.java.net/browse/JDK-8162520
     */
    static class HugeFileStore extends FileStore {

        @Override
        public String name() {
            return "myHugeFS";
        }

        @Override
        public String type() {
            return "bigFS";
        }

        @Override
        public boolean isReadOnly() {
            return false;
        }

        @Override
        public long getTotalSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public long getUsableSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public long getUnallocatedSpace() throws IOException {
            return randomIntBetween(-1000, 1000);
        }

        @Override
        public boolean supportsFileAttributeView(Class<? extends FileAttributeView> type) {
            return false;
        }

        @Override
        public boolean supportsFileAttributeView(String name) {
            return false;
        }

        @Override
        public <V extends FileStoreAttributeView> V getFileStoreAttributeView(Class<V> type) {
            throw new UnsupportedOperationException("don't call me");
        }

        @Override
        public Object getAttribute(String attribute) throws IOException {
            throw new UnsupportedOperationException("don't call me");
        }

    }
}
