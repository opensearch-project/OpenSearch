/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;

/**
 * Instruction node for broadcast-join build-side injection.
 *
 * <p>Carries the serialized build-side data that the coordinator gathered from the build stage
 * and a named-input id that the probe-side Substrait plan uses to reference it as a
 * {@code NamedScan}. The data-node handler decodes the bytes and registers them with the backend
 * as the source for {@code namedInputId} so the join's {@code NamedScan} resolves to the broadcast
 * payload rather than a stage-input partition stream.
 *
 * <p>Wire format:
 * <pre>
 *   String namedInputId
 *   int    buildSideIndex  (0 = left, 1 = right — indicates which join input is the broadcast side)
 *   byte[] broadcastData   (backend-specific serialization; for DataFusion today this is Arrow IPC
 *                           concatenated record batches sharing a single schema)
 * </pre>
 *
 * @opensearch.internal
 */
public class BroadcastInjectionInstructionNode implements InstructionNode {

    private final String namedInputId;
    private final int buildSideIndex;
    private final byte[] broadcastData;

    public BroadcastInjectionInstructionNode(String namedInputId, int buildSideIndex, byte[] broadcastData) {
        this.namedInputId = namedInputId;
        this.buildSideIndex = buildSideIndex;
        this.broadcastData = broadcastData;
    }

    public BroadcastInjectionInstructionNode(StreamInput in) throws IOException {
        this.namedInputId = in.readString();
        this.buildSideIndex = in.readVInt();
        this.broadcastData = in.readByteArray();
    }

    public String getNamedInputId() {
        return namedInputId;
    }

    public int getBuildSideIndex() {
        return buildSideIndex;
    }

    public byte[] getBroadcastData() {
        return broadcastData;
    }

    @Override
    public InstructionType type() {
        return InstructionType.INJECT_BROADCAST;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(namedInputId);
        out.writeVInt(buildSideIndex);
        out.writeByteArray(broadcastData);
    }
}
