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
 * Expression to evaluate equality of versions.
 */
public class Equal implements Expression {

    /**
     * Checks if a given version matches a certain range version.
     *
     * @param rangeVersion the version specified in range
     * @param versionToEvaluate the version to evaluate
     * @return {@code true} if the versions are equal {@code false} otherwise
     */
    @Override
    public boolean evaluate(final Version rangeVersion, final Version versionToEvaluate) {
        return versionToEvaluate.equals(rangeVersion);
    }
}
