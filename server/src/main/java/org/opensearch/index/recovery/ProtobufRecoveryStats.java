/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.recovery;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Recovery related statistics, starting at the shard level and allowing aggregation to
* indices and node level
*
* @opensearch.internal
*/
public class ProtobufRecoveryStats implements ToXContentFragment, ProtobufWriteable {

    private final AtomicInteger currentAsSource = new AtomicInteger();
    private final AtomicInteger currentAsTarget = new AtomicInteger();
    private final AtomicLong throttleTimeInNanos = new AtomicLong();

    public ProtobufRecoveryStats() {}

    public ProtobufRecoveryStats(CodedInputStream in) throws IOException {
        currentAsSource.set(in.readInt32());
        currentAsTarget.set(in.readInt32());
        throttleTimeInNanos.set(in.readInt64());
    }

    public void add(ProtobufRecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.currentAsSource.addAndGet(recoveryStats.currentAsSource());
            this.currentAsTarget.addAndGet(recoveryStats.currentAsTarget());
        }
        addTotals(recoveryStats);
    }

    public void addTotals(ProtobufRecoveryStats recoveryStats) {
        if (recoveryStats != null) {
            this.throttleTimeInNanos.addAndGet(recoveryStats.throttleTime().nanos());
        }
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a source
    */
    public int currentAsSource() {
        return currentAsSource.get();
    }

    /**
     * Number of ongoing recoveries for which a shard serves as a target
    */
    public int currentAsTarget() {
        return currentAsTarget.get();
    }

    /**
     * Total time recoveries waited due to throttling
    */
    public TimeValue throttleTime() {
        return TimeValue.timeValueNanos(throttleTimeInNanos.get());
    }

    public void incCurrentAsTarget() {
        currentAsTarget.incrementAndGet();
    }

    public void decCurrentAsTarget() {
        currentAsTarget.decrementAndGet();
    }

    public void incCurrentAsSource() {
        currentAsSource.incrementAndGet();
    }

    public void decCurrentAsSource() {
        currentAsSource.decrementAndGet();
    }

    public void addThrottleTime(long nanos) {
        throttleTimeInNanos.addAndGet(nanos);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.RECOVERY);
        builder.field(Fields.CURRENT_AS_SOURCE, currentAsSource());
        builder.field(Fields.CURRENT_AS_TARGET, currentAsTarget());
        builder.humanReadableField(Fields.THROTTLE_TIME_IN_MILLIS, Fields.THROTTLE_TIME, throttleTime());
        builder.endObject();
        return builder;
    }

    /**
     * Fields for recovery statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String RECOVERY = "recovery";
        static final String CURRENT_AS_SOURCE = "current_as_source";
        static final String CURRENT_AS_TARGET = "current_as_target";
        static final String THROTTLE_TIME = "throttle_time";
        static final String THROTTLE_TIME_IN_MILLIS = "throttle_time_in_millis";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt32NoTag(currentAsSource.get());
        out.writeInt32NoTag(currentAsTarget.get());
        out.writeInt64NoTag(throttleTimeInNanos.get());
    }

    @Override
    public String toString() {
        return "recoveryStats, currentAsSource ["
            + currentAsSource()
            + "],currentAsTarget ["
            + currentAsTarget()
            + "], throttle ["
            + throttleTime()
            + "]";
    }
}
