/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.replication.common;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContentFragment;
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class ReplicationState implements ToXContentFragment, Writeable {

    protected ReplicationTimer timer;
    protected ReplicationLuceneIndex index;

    protected ReplicationState() {
        // Empty default constructor for subclasses
    }

    protected ReplicationState(ReplicationLuceneIndex index) {
        this.index = index;
        timer = new ReplicationTimer();
        timer.start();
    }

    public ReplicationTimer getTimer() {
        return timer;
    }

    public ReplicationLuceneIndex getIndex() {
        return index;
    }

    public ReplicationState(StreamInput in) throws IOException {
        timer = new ReplicationTimer(in);
        index = new ReplicationLuceneIndex(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        timer.writeTo(out);
        index.writeTo(out);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.timeField(ReplicationState.Fields.START_TIME_IN_MILLIS, ReplicationState.Fields.START_TIME, timer.startTime());
        if (timer.stopTime() > 0) {
            builder.timeField(ReplicationState.Fields.STOP_TIME_IN_MILLIS, ReplicationState.Fields.STOP_TIME, timer.stopTime());
        }
        builder.humanReadableField(
            ReplicationState.Fields.TOTAL_TIME_IN_MILLIS,
            ReplicationState.Fields.TOTAL_TIME,
            new TimeValue(timer.time())
        );

        builder.startObject(ReplicationState.Fields.INDEX);
        index.toXContent(builder, params);
        builder.endObject();

        return builder;
    }

    static final class Fields {
        static final String START_TIME = "start_time";
        static final String START_TIME_IN_MILLIS = "start_time_in_millis";
        static final String STOP_TIME = "stop_time";
        static final String STOP_TIME_IN_MILLIS = "stop_time_in_millis";
        static final String TOTAL_TIME = "total_time";
        static final String TOTAL_TIME_IN_MILLIS = "total_time_in_millis";
        static final String INDEX = "index";
    }

}
