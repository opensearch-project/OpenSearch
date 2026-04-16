/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;

/**
 * A single plan alternative for a {@link Stage}. Contains a resolved fragment
 * where every operator's viableBackends and every annotation's viableBackends
 * are narrowed to exactly one backend, plus the converted bytes produced by
 * the backend's FragmentConvertor.
 *
 * @param resolvedFragment fragment with all viableBackends narrowed to single choices
 * @param backendId        the primary backend for this plan
 * @param convertedBytes   backend-specific serialized plan bytes (null before conversion)
 * @opensearch.internal
 */
public record StagePlan(RelNode resolvedFragment, String backendId, byte[] convertedBytes) {

    /** Creates a StagePlan before conversion (bytes not yet available). */
    public StagePlan(RelNode resolvedFragment, String backendId) {
        this(resolvedFragment, backendId, null);
    }

    /** Returns a copy with converted bytes populated. */
    public StagePlan withConvertedBytes(byte[] bytes) {
        return new StagePlan(resolvedFragment, backendId, bytes);
    }
}
