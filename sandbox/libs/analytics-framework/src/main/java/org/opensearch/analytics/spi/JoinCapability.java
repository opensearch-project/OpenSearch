/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.core.JoinRelType;

import java.util.Set;

/**
 * A backend's join support: the join kinds it can execute and the storage formats those
 * joins apply to. The planner matches a query's required {@link JoinKind} against
 * {@link BackendCapabilityProvider#joinCapabilities()}.
 *
 * @opensearch.internal
 */
public record JoinCapability(Set<JoinKind> kinds, Set<String> formats) {

    /** Standard SQL join kinds. */
    public enum JoinKind {
        INNER,
        LEFT,
        RIGHT,
        FULL,
        SEMI,
        ANTI,
        CROSS;

        /** Maps a Calcite {@link JoinRelType} to its capability counterpart. */
        public static JoinKind fromCalcite(JoinRelType joinType) {
            return switch (joinType) {
                case INNER -> INNER;
                case LEFT -> LEFT;
                case RIGHT -> RIGHT;
                case FULL -> FULL;
                case SEMI -> SEMI;
                case ANTI -> ANTI;
                default -> throw new IllegalStateException("Unhandled JoinRelType: " + joinType);
            };
        }
    }
}
