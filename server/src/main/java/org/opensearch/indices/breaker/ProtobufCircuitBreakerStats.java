/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.indices.breaker;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Class encapsulating stats about the circuit breaker
*
* @opensearch.internal
*/
public class ProtobufCircuitBreakerStats implements ProtobufWriteable, ToXContentObject {

    private final String name;
    private final long limit;
    private final long estimated;
    private final long trippedCount;
    private final double overhead;

    public ProtobufCircuitBreakerStats(String name, long limit, long estimated, double overhead, long trippedCount) {
        this.name = name;
        this.limit = limit;
        this.estimated = estimated;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    public ProtobufCircuitBreakerStats(CodedInputStream in) throws IOException {
        this.limit = in.readInt64();
        this.estimated = in.readInt64();
        this.overhead = in.readDouble();
        this.trippedCount = in.readInt64();
        this.name = in.readString();
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        out.writeInt64NoTag(limit);
        out.writeInt64NoTag(estimated);
        out.writeDoubleNoTag(overhead);
        out.writeInt64NoTag(trippedCount);
        out.writeStringNoTag(name);
    }

    public String getName() {
        return this.name;
    }

    public long getLimit() {
        return this.limit;
    }

    public long getEstimated() {
        return this.estimated;
    }

    public long getTrippedCount() {
        return this.trippedCount;
    }

    public double getOverhead() {
        return this.overhead;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(name.toLowerCase(Locale.ROOT));
        builder.field(Fields.LIMIT, limit);
        builder.field(Fields.LIMIT_HUMAN, new ByteSizeValue(limit));
        builder.field(Fields.ESTIMATED, estimated);
        builder.field(Fields.ESTIMATED_HUMAN, new ByteSizeValue(estimated));
        builder.field(Fields.OVERHEAD, overhead);
        builder.field(Fields.TRIPPED_COUNT, trippedCount);
        builder.endObject();
        return builder;
    }

    @Override
    public String toString() {
        return "["
            + this.name
            + ",limit="
            + this.limit
            + "/"
            + new ByteSizeValue(this.limit)
            + ",estimated="
            + this.estimated
            + "/"
            + new ByteSizeValue(this.estimated)
            + ",overhead="
            + this.overhead
            + ",tripped="
            + this.trippedCount
            + "]";
    }

    /**
     * Fields used for statistics
    *
    * @opensearch.internal
    */
    static final class Fields {
        static final String LIMIT = "limit_size_in_bytes";
        static final String LIMIT_HUMAN = "limit_size";
        static final String ESTIMATED = "estimated_size_in_bytes";
        static final String ESTIMATED_HUMAN = "estimated_size";
        static final String OVERHEAD = "overhead";
        static final String TRIPPED_COUNT = "tripped";
    }
}
