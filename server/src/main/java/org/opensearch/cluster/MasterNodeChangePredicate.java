/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster;

import java.util.function.Predicate;

/**
 * Utility class to build a predicate that accepts cluster state changes
 *
 * @opensearch.internal
 * @deprecated As of 2.2, because supporting inclusive language, replaced by {@link ClusterManagerNodeChangePredicate}
 */
@Deprecated
public final class MasterNodeChangePredicate {

    private MasterNodeChangePredicate() {

    }

    public static Predicate<ClusterState> build(ClusterState currentState) {
        return ClusterManagerNodeChangePredicate.build(currentState);
    }
}
