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

package org.opensearch.core.indices.breaker;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.common.unit.ByteSizeValue;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Locale;

/**
 * Class encapsulating stats about the {@link org.opensearch.core.common.breaker.CircuitBreaker}
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class CircuitBreakerStats implements Writeable, ToXContentObject {

    /** The name of the circuit breaker */
    private final String name;
    /** The limit size in byte of the circuit breaker. Field : "limit_size_in_bytes" */
    private final long limit;
    /** The estimated size in byte of the breaker. Field : "estimated_size_in_bytes" */
    private final long estimated;
    /** The number of times the breaker has been tripped. Field : "tripped" */
    private final long trippedCount;
    /** The overhead of the breaker. Field : "overhead" */
    private final double overhead;

    /**
     * Constructs new instance
     *
     * @param name The name of the circuit breaker
     * @param limit The limit size in byte of the circuit breaker
     * @param estimated The estimated size in byte of the breaker
     * @param overhead The overhead of the breaker
     * @param trippedCount The number of times the breaker has been tripped
     * @see org.opensearch.core.common.breaker.CircuitBreaker
     */
    public CircuitBreakerStats(String name, long limit, long estimated, double overhead, long trippedCount) {
        this.name = name;
        this.limit = limit;
        this.estimated = estimated;
        this.trippedCount = trippedCount;
        this.overhead = overhead;
    }

    /**
     * Constructs new instance from the {@link StreamInput}
     *
     * @param in The StreamInput
     * @throws IOException if an error occurs while reading from the StreamInput
     * @see org.opensearch.core.common.breaker.CircuitBreaker
     * @see #writeTo(StreamOutput)
     */
    public CircuitBreakerStats(StreamInput in) throws IOException {
        this.limit = in.readLong();
        this.estimated = in.readLong();
        this.overhead = in.readDouble();
        this.trippedCount = in.readLong();
        this.name = in.readString();
    }

    /**
     * Writes this instance into a {@link StreamOutput}
     *
     * @param out The StreamOutput
     * @throws IOException if an error occurs while writing to the StreamOutput
     * @see #CircuitBreakerStats(StreamInput)
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeLong(limit);
        out.writeLong(estimated);
        out.writeDouble(overhead);
        out.writeLong(trippedCount);
        out.writeString(name);
    }

    /**
     * Returns the name of the circuit breaker
     * @return The name of the circuit breaker
     */
    public String getName() {
        return this.name;
    }

    /**
     * Returns the limit size in byte of the circuit breaker
     * @return The limit size in byte of the circuit breaker
     */
    public long getLimit() {
        return this.limit;
    }

    /**
     * Returns the estimated size in byte of the breaker
     * @return The estimated size in byte of the breaker
     */
    public long getEstimated() {
        return this.estimated;
    }

    /**
     * Returns the number of times the breaker has been tripped
     * @return The number of times the breaker has been tripped
     */
    public long getTrippedCount() {
        return this.trippedCount;
    }

    /**
     * Returns the overhead of the breaker
     * @return The overhead of the breaker
     */
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

    /**
     * Returns a String representation of this CircuitBreakerStats
     * @return "[name,limit=limit/limit_human,estimated=estimated/estimated_human,overhead=overhead,tripped=trippedCount]"
     */
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
