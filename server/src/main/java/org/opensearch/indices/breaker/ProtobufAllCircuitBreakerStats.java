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
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.ProtobufWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Stats class encapsulating all of the different circuit breaker stats
*
* @opensearch.internal
*/
public class ProtobufAllCircuitBreakerStats implements ProtobufWriteable, ToXContentFragment {

    private final ProtobufCircuitBreakerStats[] allStats;

    public ProtobufAllCircuitBreakerStats(ProtobufCircuitBreakerStats[] allStats) {
        this.allStats = allStats;
    }

    public ProtobufAllCircuitBreakerStats(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        allStats = protobufStreamInput.readArray(ProtobufCircuitBreakerStats::new, ProtobufCircuitBreakerStats[]::new);
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        protobufStreamOutput.writeArray((o, v) -> v.writeTo(o), allStats);
    }

    public ProtobufCircuitBreakerStats[] getAllStats() {
        return this.allStats;
    }

    public ProtobufCircuitBreakerStats getStats(String name) {
        for (ProtobufCircuitBreakerStats stats : allStats) {
            if (stats.getName().equals(name)) {
                return stats;
            }
        }
        return null;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.BREAKERS);
        for (ProtobufCircuitBreakerStats stats : allStats) {
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
