/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.Set;

/**
 * Declares that a backend can evaluate a specific {@link WindowFunction} as part
 * of a {@link org.apache.calcite.rex.RexOver} inside a project. Window functions
 * are independent of {@link FieldType} because Substrait serializes them inline
 * with their argument expressions — the backend dispatches on the substrait
 * function reference at execution time, not on the Calcite operator's typed inputs.
 *
 * @opensearch.internal
 */
public record WindowCapability(WindowFunction function, Set<String> formats) {}
