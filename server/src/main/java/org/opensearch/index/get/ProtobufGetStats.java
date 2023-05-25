/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.index.get;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats for a search get
*
* @opensearch.api
*/
public class ProtobufGetStats implements ProtobufWriteable, ToXContentFragment {

    private long existsCount;
    private long existsTimeInMillis;
    private long missingCount;
    private long missingTimeInMillis;
    private long current;

    public ProtobufGetStats() {}

    public ProtobufGetStats(CodedInputStream in) throws IOException {
        existsCount = in.readInt64();
        existsTimeInMillis = in.readInt64();
        missingCount = in.readInt64();
        missingTimeInMillis = in.readInt64();
        current = in.readInt64();
    }

    public ProtobufGetStats(long existsCount, long existsTimeInMillis, long missingCount, long missingTimeInMillis, long current) {
        this.existsCount = existsCount;
        this.existsTimeInMillis = existsTimeInMillis;
        this.missingCount = missingCount;
        this.missingTimeInMillis = missingTimeInMillis;
        this.current = current;
    }

    public void add(ProtobufGetStats stats) {
        if (stats == null) {
            return;
        }
        current += stats.current;
        addTotals(stats);
    }

    public void addTotals(ProtobufGetStats stats) {
        if (stats == null) {
            return;
        }
        existsCount += stats.existsCount;
        existsTimeInMillis += stats.existsTimeInMillis;
        missingCount += stats.missingCount;
        missingTimeInMillis += stats.missingTimeInMillis;
        current += stats.current;
    }

    public long getCount() {
        return existsCount + missingCount;
    }

    public long getTimeInMillis() {
        return existsTimeInMillis + missingTimeInMillis;
    }

    public TimeValue getTime() {
        return new TimeValue(getTimeInMillis());
    }

    public long getExistsCount() {
        return this.existsCount;
    }

    public long getExistsTimeInMillis() {
        return this.existsTimeInMillis;
    }

    public TimeValue getExistsTime() {
        return new TimeValue(existsTimeInMillis);
    }

    public long getMissingCount() {
        return this.missingCount;
    }

    public long getMissingTimeInMillis() {
        return this.missingTimeInMillis;
    }

    public TimeValue getMissingTime() {
        return new TimeValue(missingTimeInMillis);
    }

    public long current() {
        return this.current;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.GET);
        builder.field(Fields.TOTAL, getCount());
        builder.humanReadableField(Fields.TIME_IN_MILLIS, Fields.TIME, getTime());
        builder.field(Fields.EXISTS_TOTAL, existsCount);
        builder.humanReadableField(Fields.EXISTS_TIME_IN_MILLIS, Fields.EXISTS_TIME, getExistsTime());
        builder.field(Fields.MISSING_TOTAL, missingCount);
        builder.humanReadableField(Fields.MISSING_TIME_IN_MILLIS, Fields.MISSING_TIME, getMissingTime());
        builder.field(Fields.CURRENT, current);
        builder.endObject();
        return builder;
    }

    /**
     * Fields for get statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String GET = "get";
        static final String TOTAL = "total";
        static final String TIME = "getTime";
        static final String TIME_IN_MILLIS = "time_in_millis";
        static final String EXISTS_TOTAL = "exists_total";
        static final String EXISTS_TIME = "exists_time";
        static final String EXISTS_TIME_IN_MILLIS = "exists_time_in_millis";
        static final String MISSING_TOTAL = "missing_total";
        static final String MISSING_TIME = "missing_time";
        static final String MISSING_TIME_IN_MILLIS = "missing_time_in_millis";
        static final String CURRENT = "current";
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(existsCount);
        out.writeInt64NoTag(existsTimeInMillis);
        out.writeInt64NoTag(missingCount);
        out.writeInt64NoTag(missingTimeInMillis);
        out.writeInt64NoTag(current);
    }
}
