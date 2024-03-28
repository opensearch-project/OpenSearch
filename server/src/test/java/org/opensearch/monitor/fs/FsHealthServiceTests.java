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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.lucene.tests.mockfile.FilterFileChannel;
import org.apache.lucene.tests.mockfile.FilterFileSystemProvider;
import org.opensearch.cluster.coordination.DeterministicTaskQueue;
import org.opensearch.common.io.PathUtils;
import org.opensearch.common.io.PathUtilsForTesting;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.monitor.StatusInfo;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.junit.annotations.TestLogging;
import org.opensearch.threadpool.TestThreadPool;
import org.opensearch.threadpool.ThreadPool;
import org.junit.Before;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.channels.FileChannel;
import java.nio.file.FileSystem;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static org.opensearch.monitor.StatusInfo.Status.HEALTHY;
import static org.opensearch.monitor.StatusInfo.Status.UNHEALTHY;
import static org.opensearch.node.Node.NODE_NAME_SETTING;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

public class FsHealthServiceTests extends OpenSearchTestCase {

    private DeterministicTaskQueue deterministicTaskQueue;

    @Before
    public void createObjects() {
        Settings settings = Settings.builder().put(NODE_NAME_SETTING.getKey(), "node").build();
        deterministicTaskQueue = new DeterministicTaskQueue(settings, random());
    }

    public void testSchedulesHealthCheckAtRefreshIntervals() throws Exception {
        long refreshInterval = randomLongBetween(1000, 12000);
        final Settings settings = Settings.builder().put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms").build();
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, deterministicTaskQueue.getThreadPool(), env);
            final long startTimeMillis = deterministicTaskQueue.getCurrentTimeMillis();
            fsHealthService.doStart();
            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertTrue(deterministicTaskQueue.hasDeferredTasks());
            int rescheduledCount = 0;
            for (int i = 1; i <= randomIntBetween(5, 10); i++) {
                if (deterministicTaskQueue.hasRunnableTasks()) {
                    deterministicTaskQueue.runRandomTask();
                } else {
                    assertThat(deterministicTaskQueue.getLatestDeferredExecutionTime(), is(refreshInterval * (rescheduledCount + 1)));
                    deterministicTaskQueue.advanceTime();
                    rescheduledCount++;
                }
                assertThat(deterministicTaskQueue.getCurrentTimeMillis() - startTimeMillis, is(refreshInterval * rescheduledCount));
            }

            fsHealthService.doStop();
            deterministicTaskQueue.runAllTasksInTimeOrder();

            assertFalse(deterministicTaskQueue.hasRunnableTasks());
            assertFalse(deterministicTaskQueue.hasDeferredTasks());
        }
    }

    public void testFailsHealthOnIOException() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptFileSystemProvider = new FileSystemIOExceptionProvider(fileSystem);
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            // disrupt file system
            disruptFileSystemProvider.restrictPathPrefix(""); // disrupt all paths
            disruptFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            for (Path path : env.nodeDataPaths()) {
                assertTrue(fsHealthService.getHealth().getInfo().contains(path.toString()));
            }
            assertEquals(env.nodeDataPaths().length, disruptFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    @TestLogging(value = "org.opensearch.monitor.fs:WARN", reason = "to ensure that we log on hung IO at WARN level")
    public void testLoggingOnHungIO() throws Exception {
        long slowLogThreshold = randomLongBetween(100, 200);
        final Settings settings = Settings.builder()
            .put(FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING.getKey(), slowLogThreshold + "ms")
            .build();
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        FileSystemFsyncHungProvider disruptFileSystemProvider = new FileSystemFsyncHungProvider(
            fileSystem,
            randomLongBetween(slowLogThreshold + 1, 400),
            testThreadPool
        );
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        try (
            MockLogAppender mockAppender = MockLogAppender.createForLoggers(LogManager.getLogger(FsHealthService.class));
            NodeEnvironment env = newNodeEnvironment()
        ) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            int counter = 0;
            for (Path path : env.nodeDataPaths()) {
                mockAppender.addExpectation(
                    new MockLogAppender.SeenEventExpectation(
                        "test" + ++counter,
                        FsHealthService.class.getCanonicalName(),
                        Level.WARN,
                        "health check of [" + path + "] took [*ms] which is above the warn threshold*"
                    )
                );
            }

            // disrupt file system
            disruptFileSystemProvider.injectIODelay.set(true);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(env.nodeDataPaths().length, disruptFileSystemProvider.getInjectedPathCount());
            assertBusy(mockAppender::assertAllExpectationsMatched);
        } finally {
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    @AwaitsFix(bugUrl = "https://github.com/opensearch-project/OpenSearch/issues/7557")
    public void testFailsHealthOnHungIOBeyondHealthyTimeout() throws Exception {
        long healthyTimeoutThreshold = randomLongBetween(1500, 2000);
        long refreshInterval = randomLongBetween(500, 1000);
        long slowLogThreshold = randomLongBetween(100, 200);
        long delayBetweenChecks = 100;
        final Settings settings = Settings.builder()
            .put(FsHealthService.HEALTHY_TIMEOUT_SETTING.getKey(), healthyTimeoutThreshold + "ms")
            .put(FsHealthService.REFRESH_INTERVAL_SETTING.getKey(), refreshInterval + "ms")
            .put(FsHealthService.SLOW_PATH_LOGGING_THRESHOLD_SETTING.getKey(), slowLogThreshold + "ms")
            .put(ThreadPool.ESTIMATED_TIME_INTERVAL_SETTING.getKey(), 0)// we need to verify exact time
            .build();
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        FileSystemFsyncHungProvider disruptFileSystemProvider = new FileSystemFsyncHungProvider(fileSystem, testThreadPool);
        fileSystem = disruptFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            logger.info("--> Initial health status prior to the first monitor run");
            StatusInfo fsHealth = fsHealthService.getHealth();
            assertEquals(HEALTHY, fsHealth.getStatus());
            assertEquals("health check passed", fsHealth.getInfo());
            logger.info("--> First monitor run");
            fsHealthService.new FsHealthMonitor().run();
            fsHealth = fsHealthService.getHealth();
            assertEquals(HEALTHY, fsHealth.getStatus());
            assertEquals("health check passed", fsHealth.getInfo());
            logger.info("--> Disrupt file system");
            disruptFileSystemProvider.injectIODelay.set(true);
            final FsHealthService fsHealthSrvc = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthSrvc.doStart();
            waitUntil(
                () -> fsHealthSrvc.getHealth().getStatus() == UNHEALTHY,
                healthyTimeoutThreshold + (2 * refreshInterval),
                TimeUnit.MILLISECONDS
            );
            fsHealth = fsHealthSrvc.getHealth();
            assertEquals(UNHEALTHY, fsHealth.getStatus());
            assertEquals("healthy threshold breached", fsHealth.getInfo());
            int disruptedPathCount = disruptFileSystemProvider.getInjectedPathCount();
            assertThat(disruptedPathCount, equalTo(1));
            logger.info("--> Fix file system disruption");
            disruptFileSystemProvider.injectIODelay.set(false);
            waitUntil(
                () -> fsHealthSrvc.getHealth().getStatus() == HEALTHY,
                delayBetweenChecks + (4 * refreshInterval),
                TimeUnit.MILLISECONDS
            );
            fsHealth = fsHealthSrvc.getHealth();
            assertEquals(HEALTHY, fsHealth.getStatus());
            assertEquals("health check passed", fsHealth.getInfo());
            assertEquals(disruptedPathCount, disruptFileSystemProvider.getInjectedPathCount());
            fsHealthSrvc.doStop();
        } finally {
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnSinglePathFsyncFailure() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemFsyncIOExceptionProvider disruptFsyncFileSystemProvider = new FileSystemFsyncIOExceptionProvider(fileSystem);
        fileSystem = disruptFsyncFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            Path[] paths = env.nodeDataPaths();
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            // disrupt file system fsync on single path
            disruptFsyncFileSystemProvider.injectIOException.set(true);
            String disruptedPath = randomFrom(paths).toString();
            disruptFsyncFileSystemProvider.restrictPathPrefix(disruptedPath);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed on [" + disruptedPath + "]"));
            assertEquals(1, disruptFsyncFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptFsyncFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnSinglePathWriteFailure() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        FileSystemIOExceptionProvider disruptWritesFileSystemProvider = new FileSystemIOExceptionProvider(fileSystem);
        fileSystem = disruptWritesFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final Settings settings = Settings.EMPTY;
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        try (NodeEnvironment env = newNodeEnvironment()) {
            Path[] paths = env.nodeDataPaths();
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            // disrupt file system writes on single path
            String disruptedPath = randomFrom(paths).toString();
            disruptWritesFileSystemProvider.restrictPathPrefix(disruptedPath);
            disruptWritesFileSystemProvider.injectIOException.set(true);
            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed on [" + disruptedPath + "]"));
            assertEquals(1, disruptWritesFileSystemProvider.getInjectedPathCount());
        } finally {
            disruptWritesFileSystemProvider.injectIOException.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    public void testFailsHealthOnUnexpectedLockFileSize() throws IOException {
        FileSystem fileSystem = PathUtils.getDefaultFileSystem();
        final Settings settings = Settings.EMPTY;
        TestThreadPool testThreadPool = new TestThreadPool(getClass().getName(), settings);
        FileSystemUnexpectedLockFileSizeProvider unexpectedLockFileSizeFileSystemProvider = new FileSystemUnexpectedLockFileSizeProvider(
            fileSystem,
            1,
            testThreadPool
        );
        fileSystem = unexpectedLockFileSizeFileSystemProvider.getFileSystem(null);
        PathUtilsForTesting.installMock(fileSystem);
        final ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        try (NodeEnvironment env = newNodeEnvironment()) {
            FsHealthService fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(HEALTHY, fsHealthService.getHealth().getStatus());
            assertEquals("health check passed", fsHealthService.getHealth().getInfo());

            // enabling unexpected file size injection
            unexpectedLockFileSizeFileSystemProvider.injectUnexpectedFileSize.set(true);

            fsHealthService = new FsHealthService(settings, clusterSettings, testThreadPool, env);
            fsHealthService.new FsHealthMonitor().run();
            assertEquals(UNHEALTHY, fsHealthService.getHealth().getStatus());
            assertThat(fsHealthService.getHealth().getInfo(), is("health check failed due to broken node lock"));
            assertEquals(1, unexpectedLockFileSizeFileSystemProvider.getInjectedPathCount());
        } finally {
            unexpectedLockFileSizeFileSystemProvider.injectUnexpectedFileSize.set(false);
            PathUtilsForTesting.teardown();
            ThreadPool.terminate(testThreadPool, 500, TimeUnit.MILLISECONDS);
        }
    }

    private static class FileSystemIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private String pathPrefix;

        FileSystemIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        public void restrictPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public int getInjectedPathCount() {
            return injectedPaths.get();
        }

        @Override
        public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
            if (injectIOException.get()) {
                assert pathPrefix != null : "must set pathPrefix before starting disruptions";
                if (path.toString().startsWith(pathPrefix) && path.toString().endsWith(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                    injectedPaths.incrementAndGet();
                    throw new IOException("fake IOException");
                }
            }
            return super.newOutputStream(path, options);
        }
    }

    private static class FileSystemFsyncIOExceptionProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIOException = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private String pathPrefix = null;

        FileSystemFsyncIOExceptionProvider(FileSystem inner) {
            super("disrupt_fs_health://", inner);
        }

        public void restrictPathPrefix(String pathPrefix) {
            this.pathPrefix = pathPrefix;
        }

        public int getInjectedPathCount() {
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (injectIOException.get()) {
                        assert pathPrefix != null : "must set pathPrefix before starting disruptions";
                        if (path.toString().startsWith(pathPrefix)
                            && path.toString().endsWith(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                            injectedPaths.incrementAndGet();
                            throw new IOException("fake IOException");
                        }
                    }
                    super.force(metaData);
                }
            };
        }
    }

    private static class FileSystemFsyncHungProvider extends FilterFileSystemProvider {

        AtomicBoolean injectIODelay = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private final long delay;
        private final ThreadPool threadPool;
        private static final long AWAIT_BUSY_THRESHOLD = 100L;

        FileSystemFsyncHungProvider(FileSystem inner, long delay, ThreadPool threadPool) {
            super("disrupt_fs_health://", inner);
            this.delay = delay;
            this.threadPool = threadPool;
        }

        FileSystemFsyncHungProvider(FileSystem inner, ThreadPool threadPool) {
            super("disrupt_fs_health://", inner);
            this.threadPool = threadPool;
            this.delay = Long.MAX_VALUE;
        }

        public int getInjectedPathCount() {
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public void force(boolean metaData) throws IOException {
                    if (injectIODelay.get()) {
                        if (path.getFileName().toString().equals(FsHealthService.FsHealthMonitor.TEMP_FILE_NAME)) {
                            injectedPaths.incrementAndGet();
                            final long startTimeMillis = threadPool.relativeTimeInMillis();
                            long timeInMillis = 1;
                            long maxWaitTimeMillis = startTimeMillis + delay >= 0 ? startTimeMillis + delay : Long.MAX_VALUE;// long
                                                                                                                             // overflow
                            do {
                                try {
                                    Thread.sleep(timeInMillis);
                                } catch (InterruptedException e) {
                                    throw new AssertionError(e);
                                }
                                timeInMillis = Math.min(AWAIT_BUSY_THRESHOLD, timeInMillis * 2);
                            } while (threadPool.relativeTimeInMillis() <= maxWaitTimeMillis && injectIODelay.get());
                        }
                    }
                    super.force(metaData);
                }
            };
        }
    }

    private static class FileSystemUnexpectedLockFileSizeProvider extends FilterFileSystemProvider {

        AtomicBoolean injectUnexpectedFileSize = new AtomicBoolean();
        AtomicInteger injectedPaths = new AtomicInteger();

        private final long size;
        private final ThreadPool threadPool;

        FileSystemUnexpectedLockFileSizeProvider(FileSystem inner, long size, ThreadPool threadPool) {
            super("disrupt_fs_health://", inner);
            this.size = size;
            this.threadPool = threadPool;
        }

        public int getInjectedPathCount() {
            return injectedPaths.get();
        }

        @Override
        public FileChannel newFileChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs) throws IOException {
            return new FilterFileChannel(super.newFileChannel(path, options, attrs)) {
                @Override
                public long size() throws IOException {
                    if (injectUnexpectedFileSize.get()) {
                        if (path.getFileName().toString().equals(NodeEnvironment.NODE_LOCK_FILENAME)) {
                            injectedPaths.incrementAndGet();
                            return size;
                        }
                    }
                    return super.size();
                }
            };
        }
    }
}
