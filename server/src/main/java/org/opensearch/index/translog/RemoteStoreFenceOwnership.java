/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.translog;

import java.io.IOException;

/**
 * Fence-ownership operations, implemented only by translogs whose acknowledgement path is an object store.
 * <p>
 * Deliberately a separate interface rather than methods on {@link Translog}. A fence is an object-store construct: it
 * exists to stop a writer this node cannot see, and there is nothing for a local translog to hold, take over, or give
 * back. Putting these on the shared base would put remote-only surface on every local translog, and two of the three
 * cannot even be given an honest default there - "has a higher term superseded me" and "may I resume as primary" are
 * substantive answers, not no-ops, and a translog with no fence is not entitled to answer either.
 * <p>
 * Callers narrow to this interface, so the absence of a fence is a visible branch at the call site instead of a silent
 * default several classes away. See {@link org.opensearch.index.translog.transfer.RemoteStoreFence} for the protocol.
 *
 * @opensearch.internal
 */
public interface RemoteStoreFenceOwnership {

    /**
     * Whether a strictly higher primary term has taken the fence, meaning this copy has been superseded and must stop
     * mutating shared remote state.
     */
    boolean isRemoteStoreFenceSuperseded() throws IOException;

    /**
     * Hands fence ownership to a primary relocation target, as the final act before the primary context is handed off.
     * The source performs the transfer itself because it is the only writer at that point, having drained.
     */
    void transferFenceOwnership(String targetAllocationId) throws IOException;

    /**
     * Reclaims fence ownership after an aborted relocation handoff. Returns {@code true} when the target never wrote,
     * so this copy may resume as primary, and {@code false} when the target had already taken over and this copy must
     * stand down.
     */
    boolean revertFenceOwnership() throws IOException;
}
