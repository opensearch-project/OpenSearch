/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.metrics;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;

import java.io.IOException;

public class ScriptedAvg implements Writeable {
    private double sum;
    private long count;

    public ScriptedAvg(double sum, long count) {
        this.sum = sum;
        this.count = count;
    }

    public ScriptedAvg(StreamInput in) throws IOException {
        this.sum = in.readDouble();
        this.count = in.readLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeDouble(sum);
        out.writeLong(count);
    }

    public double getSum() { return sum; }
    public long getCount() { return count; }
}
