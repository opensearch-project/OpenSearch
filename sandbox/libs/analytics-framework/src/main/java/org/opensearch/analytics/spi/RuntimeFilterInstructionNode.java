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
 * Delivers a join runtime filter's payload to a probe-side fragment.
 *
 * <p>The fragment's plan carries only {@code filterId}; this instruction carries the bytes. That
 * split exists because the filter's value is not known until the build side has run, whereas the
 * plan is built before it — the same reason {@link FilterDelegationInstructionNode} ships its
 * queries separately from the {@code index_filter(id)} calls that reference them.
 *
 * <p>It is also a performance requirement rather than a preference. Carrying the bitset as a plan
 * literal would make the backend rebuild it on every record batch, which for a megabyte-sized
 * filter over a fact-table scan is a large amount of copying; an id is resolved once.
 *
 * <p>Wire format:
 * <pre>
 *   int    filterId  (matches the id in the plan's filter predicate)
 *   byte[] payload   (backend-specific; for DataFusion today a raw SBBF bitset)
 * </pre>
 *
 * <p>Applying it is optional for correctness: a backend that cannot install the filter runs the
 * fragment unfiltered, which costs the optimization and not the result.
 *
 * @opensearch.internal
 */
public class RuntimeFilterInstructionNode implements InstructionNode {

    private final int filterId;
    private final byte[] payload;

    public RuntimeFilterInstructionNode(int filterId, byte[] payload) {
        this.filterId = filterId;
        this.payload = payload;
    }

    public RuntimeFilterInstructionNode(StreamInput in) throws IOException {
        this.filterId = in.readInt();
        this.payload = in.readByteArray();
    }

    public int getFilterId() {
        return filterId;
    }

    public byte[] getPayload() {
        return payload;
    }

    @Override
    public InstructionType type() {
        return InstructionType.INSTALL_RUNTIME_FILTER;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeInt(filterId);
        out.writeByteArray(payload);
    }

    @Override
    public String toString() {
        // Summarised: the payload is up to megabytes and this string reaches logs.
        return "RuntimeFilterInstructionNode[filterId=" + filterId + ", payloadBytes=" + (payload == null ? 0 : payload.length) + "]";
    }
}
