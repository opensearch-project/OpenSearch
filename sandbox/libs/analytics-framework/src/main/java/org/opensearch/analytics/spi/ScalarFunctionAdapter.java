/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;

import java.util.List;

/**
 * Per-function adapter that transforms a backend-agnostic scalar function
 * {@link RexCall} into a backend-compatible form. Registered by backends
 * alongside their capability declarations, keyed by {@link ScalarFunction}.
 *
 * <p>Example: {@code SIN(BIGINT)} → {@code SIN(CAST(BIGINT → DOUBLE))} because
 * Substrait only declares {@code sin(fp32)} and {@code sin(fp64)}.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface ScalarFunctionAdapter {

    /**
     * Adapt the given expression for backend compatibility. Returns the adapted
     * expression, or the original unchanged if no adaptation is needed.
     *
     * @param original     the backend-agnostic expression to adapt
     * @param fieldStorage positional field storage info from the operator's child,
     *                     indexed by {@link org.apache.calcite.rex.RexInputRef#getIndex()}
     */
    RexNode adapt(RexCall original, List<FieldStorageInfo> fieldStorage);
}
