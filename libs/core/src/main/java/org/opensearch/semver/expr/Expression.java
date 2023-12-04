/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver.expr;

import org.opensearch.Version;

/**
 * An evaluation expression.
 */
public interface Expression {

    /**
     * Evaluates an expression.
     *
     * @param version the version to evaluate
     * @return the result of the expression evaluation
     */
    boolean evaluate(final Version version);
}
