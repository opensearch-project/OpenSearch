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

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats class encapsulating all of the different circuit breaker stats
 *
 * @opensearch.internal
 */
public class AllCircuitBreakerStats implements Writeable, ToXContentFragment {

    /** AN array of all the circuit breaker stats */
    private final CircuitBreakerStats[] allStats;

    /**
     * Constructor
     *
     * @param allStats An array of all the circuit breaker stats
     */
    public AllCircuitBreakerStats(CircuitBreakerStats[] allStats) {
        this.allStats = allStats;
    }

    /**
     * Constructor from a StreamInput
     * @param in The StreamInput to read from
     * @throws IOException If an error occurs while reading from the StreamInput
     * @see #writeTo(StreamOutput)
     */
    public AllCircuitBreakerStats(StreamInput in) throws IOException {
        allStats = in.readArray(CircuitBreakerStats::new, CircuitBreakerStats[]::new);
    }

    /**
     * Write allStats to a StreamOutput
     * @param out The StreamOutput to write to
     * @throws IOException If an error occurs while writing to the StreamOutput
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray(allStats);
    }

    /**
     * Return allStats
     * @return allStats
     */
    public CircuitBreakerStats[] getAllStats() {
        return this.allStats;
    }

    /**
     * Return the stats for a specific circuit breaker
     * @param name The name of the circuit breaker
     * @return The {@link CircuitBreakerStats} for the circuit breaker. Null if the circuit breaker does not exist
     */
    public CircuitBreakerStats getStats(String name) {
        for (CircuitBreakerStats stats : allStats) {
            if (stats.getName().equals(name)) {
                return stats;
            }
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BREAKERS);
        for (CircuitBreakerStats stats : allStats) {
            if (stats != null) {
                stats.toXContent(builder, params);
            }
        }
        builder.endObject();
        return builder;
    }

    /**
     * Fields used for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String BREAKERS = "breakers";
    }
}
