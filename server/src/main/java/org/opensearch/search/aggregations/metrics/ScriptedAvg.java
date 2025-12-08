/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 *
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

/**
 * Represents a scripted average calculation containing a sum and count.
 *
 * @opensearch.internal
 */
public class ScriptedAvg implements Writeable {
    private double sum;
    private long count;

    /**
     * Constructor for ScriptedAvg
     *
     * @param sum   The sum of values
     * @param count The count of values
     */
    public ScriptedAvg(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    /**
     * Read from a stream.
     */
    public ScriptedAvg(StreamInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    public double getSum() {
        return sum;
    }

    public long getCount() {
        return count;
    }

}
