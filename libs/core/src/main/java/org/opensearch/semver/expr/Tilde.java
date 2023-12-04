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

    private final Version rangeVersion;

    /**
     * Constructs a {@code Tilde} expression with the given range version.
     *
     * @param rangeVersion rangeVersion
     */
    public Tilde(final Version rangeVersion) {
        this.rangeVersion = rangeVersion;
    }

    /**
     * Checks if the given input version is compatible with the rangeVersion allowing for patch version variability.
     * Allows all versions starting from the rangeVersion upto next minor version (exclusive).
     *
     * @param version the version to evaluate
     * @return {@code true} if the versions are compatible {@code false} otherwise
     */
    @Override
    public boolean evaluate(final Version version) {
        Version lower = rangeVersion;
        Version upper = Version.fromString(rangeVersion.major + "." + (rangeVersion.minor + 1) + "." + 0);
        return version.onOrAfter(lower) && version.before(upper);
    }
}
