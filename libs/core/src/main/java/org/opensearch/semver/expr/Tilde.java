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
 * Expression to evaluate version compatibility allowing patch version variability.
 */
public class Tilde implements Expression {

    /**
     * Checks if the given version is compatible with a range version allowing for patch version variability.
     * Allows all versions starting from the rangeVersion upto next minor version (exclusive).
     * @param rangeVersion the version specified in range
     * @param versionToEvaluate the version to evaluate
     * @return {@code true} if the versions are compatible {@code false} otherwise
     */
    @Override
    public boolean evaluate(final Version rangeVersion, final Version versionToEvaluate) {
        Version lower = rangeVersion;
        Version upper = Version.fromString(rangeVersion.major + "." + (rangeVersion.minor + 1) + "." + 0);
        return versionToEvaluate.onOrAfter(lower) && versionToEvaluate.before(upper);
    }
}
