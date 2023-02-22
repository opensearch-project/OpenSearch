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

package org.opensearch.indices.recovery;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.RecoverySource;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.ShardId;
import org.opensearch.indices.replication.common.ReplicationState;
import org.opensearch.indices.replication.common.ReplicationLuceneIndex;
import org.opensearch.indices.replication.common.ReplicationTimer;

import java.io.IOException;
import java.util.Locale;

/**
 * Keeps track of state related to shard recovery.
 *
 * @opensearch.internal
 */
public class RecoveryState implements ReplicationState, ToXContentFragment, Writeable {

    /**
     * The stage of the recovery state
     *
     * @opensearch.internal
     */
    public enum Stage {
        INIT((byte) 0),

        /**
         * recovery of lucene files, either reusing local ones are copying new ones
         */
        INDEX((byte) 1),

        /**
         * potentially running check index
         */
        VERIFY_INDEX((byte) 2),

        /**
         * starting up the engine, replaying the translog
         */
        TRANSLOG((byte) 3),

        /**
         * performing final task after all translog ops have been done
         */
        FINALIZE((byte) 4),

        DONE((byte) 5);

        private static final Stage[] STAGES = new Stage[Stage.values().length];

        static {
            for (Stage stage : Stage.values()) {
                assert stage.id() < STAGES.length && stage.id() >= 0;
                STAGES[stage.id] = stage;
            }
        }

        private final byte id;

        Stage(byte id) {
            this.id = id;
        }

        public byte id() {
            return id;
        }

        public static Stage fromId(byte id) {
            if (id < 0 || id >= STAGES.length) {
                throw new IllegalArgumentException("No mapping for id [" + id + "]");
            }
            return STAGES[id];
        }
    }

    private Stage stage;

    private final ReplicationLuceneIndex index;
    private final Translog translog;
    private final VerifyIndex verifyIndex;
    private final ReplicationTimer timer;

    private RecoverySource recoverySource;
    private ShardId shardId;
    @Nullable
    private DiscoveryNode sourceNode;
    private DiscoveryNode targetNode;
    private boolean primary;

    public RecoveryState(ShardRouting shardRouting, DiscoveryNode targetNode, @Nullable DiscoveryNode sourceNode) {
        this(shardRouting, targetNode, sourceNode, new ReplicationLuceneIndex());
    }

    public RecoveryState(
        ShardRouting shardRouting,
        DiscoveryNode targetNode,
        @Nullable DiscoveryNode sourceNode,
        ReplicationLuceneIndex index
    ) {
        assert shardRouting.initializing() : "only allow initializing shard routing to be recovered: " + shardRouting;
        RecoverySource recoverySource = shardRouting.recoverySource();
        assert (recoverySource.getType() == RecoverySource.Type.PEER) == (sourceNode != null)
            : "peer recovery requires source node, recovery type: " + recoverySource.getType() + " source node: " + sourceNode;
        this.shardId = shardRouting.shardId();
        this.primary = shardRouting.primary();
        this.recoverySource = recoverySource;
        this.sourceNode = sourceNode;
        this.targetNode = targetNode;
        stage = Stage.INIT;
        this.index = index;
        translog = new Translog();
        verifyIndex = new VerifyIndex();
        timer = new ReplicationTimer();
        timer.start();
    }

    public RecoveryState(StreamInput in) throws IOException {
        timer = new ReplicationTimer(in);
        stage = Stage.fromId(in.readByte());
        shardId = new ShardId(in);
        recoverySource = RecoverySource.readFrom(in);
        targetNode = new DiscoveryNode(in);
        sourceNode = in.readOptionalWriteable(DiscoveryNode::new);
        index = new ReplicationLuceneIndex(in);
        translog = new Translog(in);
        verifyIndex = new VerifyIndex(in);
        primary = in.readBoolean();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
        out.writeByte(stage.id());
        shardId.writeTo(out);
        recoverySource.writeTo(out);
        targetNode.writeTo(out);
        out.writeOptionalWriteable(sourceNode);
        index.writeTo(out);
        translog.writeTo(out);
        verifyIndex.writeTo(out);
        out.writeBoolean(primary);
    }

    public ShardId getShardId() {
        return shardId;
    }

    public synchronized Stage getStage() {
        return this.stage;
    }

    protected void validateAndSetStage(Stage expected, Stage next) {
        if (stage != expected) {
            assert false : "can't move recovery to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])";
            throw new IllegalStateException(
                "can't move recovery to stage [" + next + "]. current stage: [" + stage + "] (expected [" + expected + "])"
            );
        }
        stage = next;
    }

    public synchronized void validateCurrentStage(Stage expected) {
        if (stage != expected) {
            assert false : "expected stage [" + expected + "]; but current stage is [" + stage + "]";
            throw new IllegalStateException("expected stage [" + expected + "] but current stage is [" + stage + "]");
        }
    }

    // synchronized is strictly speaking not needed (this is called by a single thread), but just to be safe
    public synchronized RecoveryState setStage(Stage stage) {
        switch (stage) {
            case INIT:
                // reinitializing stop remove all state except for start time
                this.stage = Stage.INIT;
                getIndex().reset();
                getVerifyIndex().reset();
                getTranslog().reset();
                break;
            case INDEX:
                validateAndSetStage(Stage.INIT, stage);
                getIndex().start();
                break;
            case VERIFY_INDEX:
                validateAndSetStage(Stage.INDEX, stage);
                getIndex().stop();
                getVerifyIndex().start();
                break;
            case TRANSLOG:
                validateAndSetStage(Stage.VERIFY_INDEX, stage);
                getVerifyIndex().stop();
                getTranslog().start();
                break;
            case FINALIZE:
                assert getIndex().bytesStillToRecover() >= 0 : "moving to stage FINALIZE without completing file details";
                validateAndSetStage(Stage.TRANSLOG, stage);
                getTranslog().stop();
                break;
            case DONE:
                validateAndSetStage(Stage.FINALIZE, stage);
                getTimer().stop();
                break;
            default:
                throw new IllegalArgumentException("unknown RecoveryState.Stage [" + stage + "]");
        }
        return this;
    }

    public ReplicationLuceneIndex getIndex() {
        return index;
    }

    public VerifyIndex getVerifyIndex() {
        return this.verifyIndex;
    }

    public Translog getTranslog() {
        return translog;
    }

    @Override
    public ReplicationTimer getTimer() {
        return timer;
    }

    public RecoverySource getRecoverySource() {
        return recoverySource;
    }

    /**
     * Returns recovery source node (only non-null if peer recovery)
     */
    @Nullable
    public DiscoveryNode getSourceNode() {
        return sourceNode;
    }

    public DiscoveryNode getTargetNode() {
        return targetNode;
    }

    public boolean getPrimary() {
        return primary;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field(Fields.ID, shardId.id());
        builder.field(Fields.TYPE, recoverySource.getType());
        builder.field(Fields.STAGE, stage.toString());
        builder.field(Fields.PRIMARY, primary);
        builder.timeField(Fields.START_TIME_IN_MILLIS, Fields.START_TIME, timer.startTime());
        if (timer.stopTime() > 0) {
            builder.timeField(Fields.STOP_TIME_IN_MILLIS, Fields.STOP_TIME, timer.stopTime());
        }
        builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(timer.time()));

        if (recoverySource.getType() == RecoverySource.Type.PEER) {
            builder.startObject(Fields.SOURCE);
            builder.field(Fields.ID, sourceNode.getId());
            builder.field(Fields.HOST, sourceNode.getHostName());
            builder.field(Fields.TRANSPORT_ADDRESS, sourceNode.getAddress().toString());
            builder.field(Fields.IP, sourceNode.getHostAddress());
            builder.field(Fields.NAME, sourceNode.getName());
            builder.endObject();
        } else {
            builder.startObject(Fields.SOURCE);
            recoverySource.addAdditionalFields(builder, params);
            builder.endObject();
        }

        builder.startObject(Fields.TARGET);
        builder.field(Fields.ID, targetNode.getId());
        builder.field(Fields.HOST, targetNode.getHostName());
        builder.field(Fields.TRANSPORT_ADDRESS, targetNode.getAddress().toString());
        builder.field(Fields.IP, targetNode.getHostAddress());
        builder.field(Fields.NAME, targetNode.getName());
        builder.endObject();

        builder.startObject(Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.TRANSLOG);
        translog.toXContent(builder, params);
        builder.endObject();

        builder.startObject(Fields.VERIFY_INDEX);
        verifyIndex.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    /**
     * Fields used in the recovery state
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String ID = "id";
        static final String TYPE = "type";
        static final String STAGE = "stage";
        static final String PRIMARY = "primary";
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String STOP_TIME = "stop_time";
        static final String STOP_TIME_IN_MILLIS = "stop_time_in_millis";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String SOURCE = "source";
        static final String HOST = "host";
        static final String TRANSPORT_ADDRESS = "transport_address";
        static final String IP = "ip";
        static final String NAME = "name";
        static final String TARGET = "target";
        static final String INDEX = "index";
        static final String TRANSLOG = "translog";
        static final String TOTAL_ON_START = "total_on_start";
        static final String VERIFY_INDEX = "verify_index";
        static final String RECOVERED = "recovered";
        static final String CHECK_INDEX_TIME = "check_index_time";
        static final String CHECK_INDEX_TIME_IN_MILLIS = "check_index_time_in_millis";
        static final String TOTAL = "total";
        static final String PERCENT = "percent";
    }

    /**
     * Verifys the lucene index
     *
     * @opensearch.internal
     */
    public static class VerifyIndex extends ReplicationTimer implements ToXContentFragment, Writeable {
        private volatile long checkIndexTime;

        public VerifyIndex() {}

        public VerifyIndex(StreamInput in) throws IOException {
            super(in);
            checkIndexTime = in.readVLong();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVLong(checkIndexTime);
        }

        public void reset() {
            super.reset();
            checkIndexTime = 0;
        }

        public long checkIndexTime() {
            return checkIndexTime;
        }

        public void checkIndexTime(long checkIndexTime) {
            this.checkIndexTime = checkIndexTime;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.humanReadableField(Fields.CHECK_INDEX_TIME_IN_MILLIS, Fields.CHECK_INDEX_TIME, new TimeValue(checkIndexTime));
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

    /**
     * The translog
     *
     * @opensearch.internal
     */
    public static class Translog extends ReplicationTimer implements ToXContentFragment, Writeable {
        public static final int UNKNOWN = -1;

        private int recovered;
        private int total = UNKNOWN;
        private int totalOnStart = UNKNOWN;
        private int totalLocal = UNKNOWN;

        public Translog() {}

        public Translog(StreamInput in) throws IOException {
            super(in);
            recovered = in.readVInt();
            total = in.readVInt();
            totalOnStart = in.readVInt();
            totalLocal = in.readVInt();
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeVInt(recovered);
            out.writeVInt(total);
            out.writeVInt(totalOnStart);
            out.writeVInt(totalLocal);
        }

        public synchronized void reset() {
            super.reset();
            recovered = 0;
            total = UNKNOWN;
            totalOnStart = UNKNOWN;
            totalLocal = UNKNOWN;
        }

        public synchronized void incrementRecoveredOperations() {
            recovered++;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total ["
                + total
                + "], recovered ["
                + recovered
                + "]";
        }

        public synchronized void incrementRecoveredOperations(int ops) {
            recovered += ops;
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total ["
                + total
                + "], recovered ["
                + recovered
                + "]";
        }

        public synchronized void decrementRecoveredOperations(int ops) {
            recovered -= ops;
            assert recovered >= 0 : "recovered operations must be non-negative. Because ["
                + recovered
                + "] after decrementing ["
                + ops
                + "]";
            assert total == UNKNOWN || total >= recovered : "total, if known, should be > recovered. total ["
                + total
                + "], recovered ["
                + recovered
                + "]";
        }

        /**
         * returns the total number of translog operations recovered so far
         */
        public synchronized int recoveredOperations() {
            return recovered;
        }

        /**
         * returns the total number of translog operations needed to be recovered at this moment.
         * Note that this can change as the number of operations grows during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperations() {
            return total;
        }

        public synchronized void totalOperations(int total) {
            this.total = totalLocal == UNKNOWN ? total : totalLocal + total;
            assert total == UNKNOWN || this.total >= recovered : "total, if known, should be > recovered. total ["
                + total
                + "], recovered ["
                + recovered
                + "]";
        }

        /**
         * returns the total number of translog operations to recovered, on the start of the recovery. Unlike {@link #totalOperations}
         * this does change during recovery.
         * <p>
         * A value of -1 ({@link RecoveryState.Translog#UNKNOWN} is return if this is unknown (typically a gateway recovery)
         */
        public synchronized int totalOperationsOnStart() {
            return this.totalOnStart;
        }

        public synchronized void totalOperationsOnStart(int total) {
            this.totalOnStart = totalLocal == UNKNOWN ? total : totalLocal + total;
        }

        /**
         * Sets the total number of translog operations to be recovered locally before performing peer recovery
         * @see IndexShard#recoverLocallyUpToGlobalCheckpoint()
         */
        public synchronized void totalLocal(int totalLocal) {
            assert totalLocal >= recovered : totalLocal + " < " + recovered;
            this.totalLocal = totalLocal;
        }

        public synchronized int totalLocal() {
            return totalLocal;
        }

        public synchronized float recoveredPercent() {
            if (total == UNKNOWN) {
                return -1.f;
            }
            if (total == 0) {
                return 100.f;
            }
            return recovered * 100.0f / total;
        }

        @Override
        public synchronized XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.field(Fields.RECOVERED, recovered);
            builder.field(Fields.TOTAL, total);
            builder.field(Fields.PERCENT, String.format(Locale.ROOT, "%1.1f%%", recoveredPercent()));
            builder.field(Fields.TOTAL_ON_START, totalOnStart);
            builder.humanReadableField(Fields.TOTAL_TIME_IN_MILLIS, Fields.TOTAL_TIME, new TimeValue(time()));
            return builder;
        }
    }

}
