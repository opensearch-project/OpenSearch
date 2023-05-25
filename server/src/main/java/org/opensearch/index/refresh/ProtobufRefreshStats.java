/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.refresh;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

/**
 * Encapsulates stats for index refresh
*
* @opensearch.internal
*/
public class ProtobufRefreshStats implements ProtobufWriteable, ToXContentFragment {

    private long total;

    private long totalTimeInMillis;

    private long externalTotal;

    private long externalTotalTimeInMillis;

    /**
     * Number of waiting refresh listeners.
    */
    private int listeners;

    public ProtobufRefreshStats() {}

    public ProtobufRefreshStats(CodedInputStream in) throws IOException {
        total = in.readInt64();
        totalTimeInMillis = in.readInt64();
        externalTotal = in.readInt64();
        externalTotalTimeInMillis = in.readInt64();
        listeners = in.readInt32();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(total);
        out.writeInt64NoTag(totalTimeInMillis);
        out.writeInt64NoTag(externalTotal);
        out.writeInt64NoTag(externalTotalTimeInMillis);
        out.writeInt32NoTag(listeners);
    }

    public ProtobufRefreshStats(long total, long totalTimeInMillis, long externalTotal, long externalTotalTimeInMillis, int listeners) {
        this.total = total;
        this.totalTimeInMillis = totalTimeInMillis;
        this.externalTotal = externalTotal;
        this.externalTotalTimeInMillis = externalTotalTimeInMillis;
        this.listeners = listeners;
    }

    public void add(ProtobufRefreshStats refreshStats) {
        addTotals(refreshStats);
    }

    public void addTotals(ProtobufRefreshStats refreshStats) {
        if (refreshStats == null) {
            return;
        }
        this.total += refreshStats.total;
        this.totalTimeInMillis += refreshStats.totalTimeInMillis;
        this.externalTotal += refreshStats.externalTotal;
        this.externalTotalTimeInMillis += refreshStats.externalTotalTimeInMillis;
        this.listeners += refreshStats.listeners;
    }

    /**
     * The total number of refresh executed.
    */
    public long getTotal() {
        return this.total;
    }

    /*
    * The total number of external refresh executed.
    */
    public long getExternalTotal() {
        return this.externalTotal;
    }

    /**
     * The total time spent executing refreshes (in milliseconds).
    */
    public long getTotalTimeInMillis() {
        return this.totalTimeInMillis;
    }

    /**
     * The total time spent executing external refreshes (in milliseconds).
    */
    public long getExternalTotalTimeInMillis() {
        return this.externalTotalTimeInMillis;
    }

    /**
     * The total time refreshes have been executed.
    */
    public TimeValue getTotalTime() {
        return new TimeValue(totalTimeInMillis);
    }

    /**
     * The total time external refreshes have been executed.
    */
    public TimeValue getExternalTotalTime() {
        return new TimeValue(externalTotalTimeInMillis);
    }

    /**
     * The number of waiting refresh listeners.
    */
    public int getListeners() {
        return listeners;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("refresh");
        builder.field("total", total);
        builder.humanReadableField("total_time_in_millis", "total_time", getTotalTime());
        builder.field("external_total", externalTotal);
        builder.humanReadableField("external_total_time_in_millis", "external_total_time", getExternalTotalTime());
        builder.field("listeners", listeners);
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null || obj.getClass() != ProtobufRefreshStats.class) {
            return false;
        }
        ProtobufRefreshStats rhs = (ProtobufRefreshStats) obj;
        return total == rhs.total
            && totalTimeInMillis == rhs.totalTimeInMillis
            && externalTotal == rhs.externalTotal
            && externalTotalTimeInMillis == rhs.externalTotalTimeInMillis
            && listeners == rhs.listeners;
    }

    @Override
    public int hashCode() {
        return Objects.hash(total, totalTimeInMillis, externalTotal, externalTotalTimeInMillis, listeners);
    }
}
