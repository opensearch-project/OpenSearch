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

package org.opensearch.indices.recovery;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex.FileMetadata;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.recovery.RecoveryState.Stage;
import org.opensearch.indices.recovery.RecoveryState.Translog;
import org.opensearch.indices.recovery.RecoveryState.VerifyIndex;
import org.opensearch.indices.replication.common.ReplicationTimer;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.opensearch.test.VersionUtils.randomVersion;
import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.closeTo;
import static org.hamcrest.Matchers.either;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.startsWith;

public class RecoveryTargetTests extends OpenSearchTestCase {
    abstract class Streamer<T extends Writeable> extends Thread {
        private T lastRead;
        private final AtomicBoolean shouldStop;
        private final T source;
        final AtomicReference<Exception> error = new AtomicReference<>();
        final Version streamVersion;

        Streamer(AtomicBoolean shouldStop, T source) {
            this(shouldStop, source, randomVersion(random()));
        }

        Streamer(AtomicBoolean shouldStop, T source, Version streamVersion) {
            this.shouldStop = shouldStop;
            this.source = source;
            this.streamVersion = streamVersion;
        }

        public T lastRead() throws Throwable {
            Exception t = error.get();
            if (t != null) {
                throw t;
            }
            return lastRead;
        }

        public T serializeDeserialize() throws IOException {
            BytesStreamOutput out = new BytesStreamOutput();
            source.writeTo(out);
            out.close();
            StreamInput in = out.bytes().streamInput();
            T obj = deserialize(in);
            lastRead = obj;
            return obj;
        }

        protected T deserialize(StreamInput in) throws IOException {
            return createObj(in);
        }

        abstract T createObj(StreamInput in) throws IOException;

        @Override
        public void run() {
            try {
                while (shouldStop.get() == false) {
                    serializeDeserialize();
                }
                serializeDeserialize();
            } catch (Exception e) {
                error.set(e);
            }
        }
    }

    public void testTimer() throws Throwable {
        AtomicBoolean stop = new AtomicBoolean();
        final ReplicationTimer timer = new ReplicationTimer();
        Streamer<ReplicationTimer> streamer = new Streamer<>(stop, timer) {
            @Override
            ReplicationTimer createObj(StreamInput in) throws IOException {
                return new ReplicationTimer(in);
            }
        };
        doTimerTest(timer, streamer);
    }

    public void testIndexTimer() throws Throwable {
        AtomicBoolean stop = new AtomicBoolean();
        ReplicationLuceneIndex index = new ReplicationLuceneIndex();
        Streamer<ReplicationLuceneIndex> streamer = new Streamer<>(stop, index) {
            @Override
            ReplicationLuceneIndex createObj(StreamInput in) throws IOException {
                return new ReplicationLuceneIndex(in);
            }
        };
        doTimerTest(index, streamer);
    }

    public void testVerifyIndexTimer() throws Throwable {
        AtomicBoolean stop = new AtomicBoolean();
        VerifyIndex verifyIndex = new VerifyIndex();
        Streamer<VerifyIndex> streamer = new Streamer<>(stop, verifyIndex) {
            @Override
            VerifyIndex createObj(StreamInput in) throws IOException {
                return new VerifyIndex(in);
            }
        };
        doTimerTest(verifyIndex, streamer);
    }

    public void testTranslogTimer() throws Throwable {
        AtomicBoolean stop = new AtomicBoolean();
        Translog translog = new Translog();
        Streamer<Translog> streamer = new Streamer<>(stop, translog) {
            @Override
            Translog createObj(StreamInput in) throws IOException {
                return new Translog(in);
            }
        };
        doTimerTest(translog, streamer);
    }

    private void doTimerTest(ReplicationTimer timer, Streamer<? extends ReplicationTimer> streamer) throws Exception {
        timer.start();
        assertTrue(timer.startTime() > 0);
        assertEquals(0, timer.stopTime());
        ReplicationTimer lastRead = streamer.serializeDeserialize();
        final long time = lastRead.time();
        assertBusy(() -> assertTrue("timer timer should progress compared to captured one ", time < timer.time()));
        assertEquals("captured time shouldn't change", time, lastRead.time());

        timer.stop();
        assertTrue(timer.stopTime() >= timer.startTime());
        assertTrue(timer.time() > 0);
        // validate captured time
        lastRead = streamer.serializeDeserialize();
        assertEquals(timer.startTime(), lastRead.startTime());
        assertEquals(timer.time(), lastRead.time());
        assertEquals(timer.stopTime(), lastRead.stopTime());

        timer.reset();
        assertEquals(0, timer.startTime());
        assertEquals(0, timer.time());
        assertEquals(0, timer.stopTime());
        // validate captured time
        lastRead = streamer.serializeDeserialize();
        assertEquals(0, lastRead.startTime());
        assertEquals(0, lastRead.time());
        assertEquals(0, lastRead.stopTime());
    }

    public void testIndex() throws Throwable {
        FileMetadata[] files = new FileMetadata[randomIntBetween(1, 20)];
        ArrayList<FileMetadata> filesToRecover = new ArrayList<>();
        long totalFileBytes = 0;
        long totalReusedBytes = 0;
        int totalReused = 0;
        for (int i = 0; i < files.length; i++) {
            final int fileLength = randomIntBetween(1, 1000);
            final boolean reused = randomBoolean();
            totalFileBytes += fileLength;
            files[i] = new FileMetadata("f_" + i, fileLength, reused);
            if (reused) {
                totalReused++;
                totalReusedBytes += fileLength;
            } else {
                filesToRecover.add(files[i]);
            }
        }

        Collections.shuffle(Arrays.asList(files), random());
        final ReplicationLuceneIndex index = new ReplicationLuceneIndex();
        assertThat(index.bytesStillToRecover(), equalTo(-1L));

        if (randomBoolean()) {
            // initialize with some data and then reset
            index.start();
            for (int i = randomIntBetween(0, 10); i > 0; i--) {
                index.addFileDetail("t_" + i, randomIntBetween(1, 100), randomBoolean());
                if (randomBoolean()) {
                    index.addSourceThrottling(randomIntBetween(0, 20));
                }
                if (randomBoolean()) {
                    index.addTargetThrottling(randomIntBetween(0, 20));
                }
            }
            if (randomBoolean()) {
                index.setFileDetailsComplete();
            }
            if (randomBoolean()) {
                index.stop();
            }
            index.reset();
        }

        // before we start we must report 0
        assertThat(index.recoveredFilesPercent(), equalTo((float) 0.0));
        assertThat(index.recoveredBytesPercent(), equalTo((float) 0.0));
        assertThat(index.sourceThrottling().nanos(), equalTo(ReplicationLuceneIndex.UNKNOWN));
        assertThat(index.targetThrottling().nanos(), equalTo(ReplicationLuceneIndex.UNKNOWN));

        index.start();
        for (FileMetadata file : files) {
            index.addFileDetail(file.name(), file.length(), file.reused());
        }

        logger.info("testing initial information");
        assertThat(index.totalBytes(), equalTo(totalFileBytes));
        assertThat(index.reusedBytes(), equalTo(totalReusedBytes));
        assertThat(index.totalRecoverBytes(), equalTo(totalFileBytes - totalReusedBytes));
        assertThat(index.totalFileCount(), equalTo(files.length));
        assertThat(index.reusedFileCount(), equalTo(totalReused));
        assertThat(index.totalRecoverFiles(), equalTo(filesToRecover.size()));
        assertThat(index.recoveredFileCount(), equalTo(0));
        assertThat(index.recoveredBytes(), equalTo(0L));
        assertThat(index.recoveredFilesPercent(), equalTo(filesToRecover.size() == 0 ? 100.0f : 0.0f));
        assertThat(index.recoveredBytesPercent(), equalTo(filesToRecover.size() == 0 ? 100.0f : 0.0f));
        assertThat(index.bytesStillToRecover(), equalTo(-1L));

        index.setFileDetailsComplete();
        assertThat(index.bytesStillToRecover(), equalTo(totalFileBytes - totalReusedBytes));

        long bytesToRecover = totalFileBytes - totalReusedBytes;
        boolean completeRecovery = bytesToRecover == 0 || randomBoolean();
        if (completeRecovery == false) {
            bytesToRecover = randomIntBetween(1, (int) bytesToRecover);
            logger.info("performing partial recovery ([{}] bytes of [{}])", bytesToRecover, totalFileBytes - totalReusedBytes);
        }
        AtomicBoolean streamShouldStop = new AtomicBoolean();

        Streamer<ReplicationLuceneIndex> backgroundReader = new Streamer<ReplicationLuceneIndex>(streamShouldStop, index) {
            @Override
            ReplicationLuceneIndex createObj(StreamInput in) throws IOException {
                return new ReplicationLuceneIndex(in);
            }
        };

        backgroundReader.start();

        long recoveredBytes = 0;
        long sourceThrottling = ReplicationLuceneIndex.UNKNOWN;
        long targetThrottling = ReplicationLuceneIndex.UNKNOWN;
        while (bytesToRecover > 0) {
            FileMetadata file = randomFrom(filesToRecover);
            final long toRecover = Math.min(bytesToRecover, randomIntBetween(1, (int) (file.length() - file.recovered())));
            final long throttledOnSource = rarely() ? randomIntBetween(10, 200) : 0;
            index.addSourceThrottling(throttledOnSource);
            if (sourceThrottling == ReplicationLuceneIndex.UNKNOWN) {
                sourceThrottling = throttledOnSource;
            } else {
                sourceThrottling += throttledOnSource;
            }
            index.addRecoveredBytesToFile(file.name(), toRecover);
            file.addRecoveredBytes(toRecover);
            final long throttledOnTarget = rarely() ? randomIntBetween(10, 200) : 0;
            if (targetThrottling == ReplicationLuceneIndex.UNKNOWN) {
                targetThrottling = throttledOnTarget;
            } else {
                targetThrottling += throttledOnTarget;
            }
            index.addTargetThrottling(throttledOnTarget);
            bytesToRecover -= toRecover;
            recoveredBytes += toRecover;
            if (file.reused() || file.fullyRecovered()) {
                filesToRecover.remove(file);
            }
        }

        if (completeRecovery) {
            assertThat(filesToRecover.size(), equalTo(0));
            index.stop();
            assertThat(index.time(), greaterThanOrEqualTo(0L));
        }

        logger.info("testing serialized information");
        streamShouldStop.set(true);
        backgroundReader.join();
        final ReplicationLuceneIndex lastRead = backgroundReader.lastRead();
        assertThat(lastRead.fileDetails().toArray(), arrayContainingInAnyOrder(index.fileDetails().toArray()));
        assertThat(lastRead.startTime(), equalTo(index.startTime()));
        if (completeRecovery) {
            assertThat(lastRead.time(), equalTo(index.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(index.time()));
        }
        assertThat(lastRead.stopTime(), equalTo(index.stopTime()));
        assertThat(lastRead.targetThrottling(), equalTo(index.targetThrottling()));
        assertThat(lastRead.sourceThrottling(), equalTo(index.sourceThrottling()));

        logger.info("testing post recovery");
        assertThat(index.totalBytes(), equalTo(totalFileBytes));
        assertThat(index.reusedBytes(), equalTo(totalReusedBytes));
        assertThat(index.totalRecoverBytes(), equalTo(totalFileBytes - totalReusedBytes));
        assertThat(index.totalFileCount(), equalTo(files.length));
        assertThat(index.reusedFileCount(), equalTo(totalReused));
        assertThat(index.totalRecoverFiles(), equalTo(files.length - totalReused));
        assertThat(index.recoveredFileCount(), equalTo(index.totalRecoverFiles() - filesToRecover.size()));
        assertThat(index.recoveredBytes(), equalTo(recoveredBytes));
        assertThat(index.targetThrottling().nanos(), equalTo(targetThrottling));
        assertThat(index.sourceThrottling().nanos(), equalTo(sourceThrottling));
        assertThat(index.bytesStillToRecover(), equalTo(totalFileBytes - totalReusedBytes - recoveredBytes));
        if (index.totalRecoverFiles() == 0) {
            assertThat((double) index.recoveredFilesPercent(), equalTo(100.0));
            assertThat((double) index.recoveredBytesPercent(), equalTo(100.0));
        } else {
            assertThat(
                (double) index.recoveredFilesPercent(),
                closeTo(100.0 * index.recoveredFileCount() / index.totalRecoverFiles(), 0.1)
            );
            assertThat((double) index.recoveredBytesPercent(), closeTo(100.0 * index.recoveredBytes() / index.totalRecoverBytes(), 0.1));
        }
    }

    public void testStageSequenceEnforcement() {
        final DiscoveryNode discoveryNode = new DiscoveryNode("1", buildNewFakeTransportAddress(), emptyMap(), emptySet(), Version.CURRENT);
        final AssertionError error = expectThrows(AssertionError.class, () -> {
            Stage[] stages = Stage.values();
            int i = randomIntBetween(0, stages.length - 1);
            int j = randomValueOtherThan(i, () -> randomIntBetween(0, stages.length - 1));
            Stage t = stages[i];
            stages[i] = stages[j];
            stages[j] = t;
            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                new ShardId("bla", "_na_", 0),
                discoveryNode.getId(),
                randomBoolean(),
                ShardRoutingState.INITIALIZING
            );
            RecoveryState state = new RecoveryState(
                shardRouting,
                discoveryNode,
                shardRouting.recoverySource().getType() == RecoverySource.Type.PEER ? discoveryNode : null
            );
            for (Stage stage : stages) {
                if (stage == Stage.FINALIZE) {
                    state.getIndex().setFileDetailsComplete();
                }
                state.setStage(stage);
            }
        });
        assertThat(error.getMessage(), startsWith("can't move recovery to stage"));
        // but reset should be always possible.
        Stage[] stages = Stage.values();
        int i = randomIntBetween(1, stages.length - 1);
        ArrayList<Stage> list = new ArrayList<>(Arrays.asList(Arrays.copyOfRange(stages, 0, i)));
        list.addAll(Arrays.asList(stages));
        ShardRouting shardRouting = TestShardRouting.newShardRouting(
            new ShardId("bla", "_na_", 0),
            discoveryNode.getId(),
            randomBoolean(),
            ShardRoutingState.INITIALIZING
        );
        RecoveryState state = new RecoveryState(
            shardRouting,
            discoveryNode,
            shardRouting.recoverySource().getType() == RecoverySource.Type.PEER ? discoveryNode : null
        );
        for (Stage stage : list) {
            state.setStage(stage);
            if (stage == Stage.INDEX) {
                state.getIndex().setFileDetailsComplete();
            }
        }

        assertThat(state.getStage(), equalTo(Stage.DONE));
    }

    public void testTranslog() throws Throwable {
        final Translog translog = new Translog();
        AtomicBoolean stop = new AtomicBoolean();
        Streamer<Translog> streamer = new Streamer<Translog>(stop, translog) {
            @Override
            Translog createObj(StreamInput in) throws IOException {
                return new Translog(in);
            }
        };

        // we don't need to test the time aspect, it's done in the timer test
        translog.start();
        assertThat(translog.recoveredOperations(), equalTo(0));
        assertThat(translog.totalOperations(), equalTo(Translog.UNKNOWN));
        assertThat(translog.totalOperationsOnStart(), equalTo(Translog.UNKNOWN));
        streamer.start();
        // force one
        streamer.serializeDeserialize();
        int ops = 0;
        int totalOps = 0;
        int totalOpsOnStart = randomIntBetween(10, 200);
        translog.totalOperationsOnStart(totalOpsOnStart);
        for (int i = scaledRandomIntBetween(10, 200); i > 0; i--) {
            final int iterationOps = randomIntBetween(1, 10);
            totalOps += iterationOps;
            translog.totalOperations(totalOps);
            assertThat((double) translog.recoveredPercent(), closeTo(100.0 * ops / totalOps, 0.1));
            for (int j = iterationOps; j > 0; j--) {
                ops++;
                translog.incrementRecoveredOperations();
                if (randomBoolean()) {
                    translog.decrementRecoveredOperations(1);
                    translog.incrementRecoveredOperations();
                }
            }
            assertThat(translog.recoveredOperations(), equalTo(ops));
            assertThat(translog.totalOperations(), equalTo(totalOps));
            assertThat(translog.recoveredPercent(), equalTo(100.f));
            assertThat(streamer.lastRead().recoveredOperations(), greaterThanOrEqualTo(0));
            assertThat(streamer.lastRead().recoveredOperations(), lessThanOrEqualTo(ops));
            assertThat(streamer.lastRead().totalOperations(), lessThanOrEqualTo(totalOps));
            assertThat(streamer.lastRead().totalOperationsOnStart(), lessThanOrEqualTo(totalOpsOnStart));
            assertThat(streamer.lastRead().recoveredPercent(), either(greaterThanOrEqualTo(0.f)).or(equalTo(-1.f)));
        }

        boolean stopped = false;
        if (randomBoolean()) {
            translog.stop();
            stopped = true;
        }

        if (randomBoolean()) {
            translog.reset();
            ops = 0;
            totalOps = Translog.UNKNOWN;
            totalOpsOnStart = Translog.UNKNOWN;
            assertThat(translog.recoveredOperations(), equalTo(0));
            assertThat(translog.totalOperationsOnStart(), equalTo(Translog.UNKNOWN));
            assertThat(translog.totalOperations(), equalTo(Translog.UNKNOWN));
        }

        stop.set(true);
        streamer.join();
        final Translog lastRead = streamer.lastRead();
        assertThat(lastRead.recoveredOperations(), equalTo(ops));
        assertThat(lastRead.totalOperations(), equalTo(totalOps));
        assertThat(lastRead.totalOperationsOnStart(), equalTo(totalOpsOnStart));
        assertThat(lastRead.startTime(), equalTo(translog.startTime()));
        assertThat(lastRead.stopTime(), equalTo(translog.stopTime()));

        if (stopped) {
            assertThat(lastRead.time(), equalTo(translog.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(translog.time()));
        }
    }

    public void testStart() throws IOException {
        final VerifyIndex verifyIndex = new VerifyIndex();
        AtomicBoolean stop = new AtomicBoolean();
        Streamer<VerifyIndex> streamer = new Streamer<VerifyIndex>(stop, verifyIndex) {
            @Override
            VerifyIndex createObj(StreamInput in) throws IOException {
                return new VerifyIndex(in);
            }
        };

        // we don't need to test the time aspect, it's done in the timer test
        verifyIndex.start();
        assertThat(verifyIndex.checkIndexTime(), equalTo(0L));
        // force one
        VerifyIndex lastRead = streamer.serializeDeserialize();
        assertThat(lastRead.checkIndexTime(), equalTo(0L));

        long took = randomLong();
        if (took < 0) {
            took = -took;
            took = Math.max(0L, took);

        }
        verifyIndex.checkIndexTime(took);
        assertThat(verifyIndex.checkIndexTime(), equalTo(took));

        boolean stopped = false;
        if (randomBoolean()) {
            verifyIndex.stop();
            stopped = true;
        }

        if (randomBoolean()) {
            verifyIndex.reset();
            took = 0;
            assertThat(verifyIndex.checkIndexTime(), equalTo(took));
        }

        lastRead = streamer.serializeDeserialize();
        assertThat(lastRead.checkIndexTime(), equalTo(took));
        assertThat(lastRead.startTime(), equalTo(verifyIndex.startTime()));
        assertThat(lastRead.stopTime(), equalTo(verifyIndex.stopTime()));

        if (stopped) {
            assertThat(lastRead.time(), equalTo(verifyIndex.time()));
        } else {
            assertThat(lastRead.time(), lessThanOrEqualTo(verifyIndex.time()));
        }
    }

    public void testConcurrentModificationIndexFileDetailsMap() throws InterruptedException {
        final ReplicationLuceneIndex index = new ReplicationLuceneIndex();
        final AtomicBoolean stop = new AtomicBoolean(false);
        Streamer<ReplicationLuceneIndex> readWriteIndex = new Streamer<ReplicationLuceneIndex>(stop, index) {
            @Override
            ReplicationLuceneIndex createObj(StreamInput in) throws IOException {
                return new ReplicationLuceneIndex(in);
            }
        };
        Thread modifyThread = new Thread() {
            @Override
            public void run() {
                for (int i = 0; i < 1000; i++) {
                    index.addFileDetail(randomAlphaOfLength(10), 100, true);
                }
                stop.set(true);
            }
        };
        readWriteIndex.start();
        modifyThread.start();
        modifyThread.join();
        readWriteIndex.join();
        assertThat(readWriteIndex.error.get(), equalTo(null));
    }

    public void testFileHashCodeAndEquals() {
        FileMetadata f = new FileMetadata("foo", randomIntBetween(0, 100), randomBoolean());
        FileMetadata anotherFile = new FileMetadata(f.name(), f.length(), f.reused());
        assertEquals(f, anotherFile);
        assertEquals(f.hashCode(), anotherFile.hashCode());
        int iters = randomIntBetween(10, 100);
        for (int i = 0; i < iters; i++) {
            f = new FileMetadata("foo", randomIntBetween(0, 100), randomBoolean());
            anotherFile = new FileMetadata(f.name(), randomIntBetween(0, 100), randomBoolean());
            if (f.equals(anotherFile)) {
                assertEquals(f.hashCode(), anotherFile.hashCode());
            } else if (f.hashCode() != anotherFile.hashCode()) {
                assertFalse(f.equals(anotherFile));
            }
        }
    }
}
