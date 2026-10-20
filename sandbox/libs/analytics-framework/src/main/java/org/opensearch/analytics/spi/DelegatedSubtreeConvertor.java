/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Converts a maximal same-backend delegated RexNode subtree into the backend's
 * serialized delegation bytes. The subtree may be a single leaf RexCall or a
 * tree rooted at AND/OR/NOT whose every leaf targets this backend.
 *
 * <p>The backend walks the subtree once and emits bytes once. Inputs are NOT
 * pre-serialized leaves; the backend sees the raw Calcite tree.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface DelegatedSubtreeConvertor {

    /**
     * Convert a maximal same-backend RexNode subtree into delegation bytes.
     *
     * @param subtree      the RexNode subtree to convert. May contain
     *                     AnnotatedPredicate wrappers — implementations
     *                     must call {@code ap.unwrap()} to reach the original RexCall.
     * @param fieldStorage per-column storage metadata for resolving
     *                     RexInputRef indices to field names.
     * @return backend-specific serialized bytes for the entire subtree.
     */
    byte[] convertSubtree(RexNode subtree, List<FieldStorageInfo> fieldStorage);
}
