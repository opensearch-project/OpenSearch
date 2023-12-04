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

    private final Version version;

    /**
     * Constructs a {@code Equal} expression with the given version.
     *
     * @param version given version
     */
    public Equal(final Version version) {
        this.version = version;
    }

    /**
     * Checks if the current version equals the member version.
     *
     * @param version the version to evaluate
     * @return {@code true} if the versions are equal {@code false} otherwise
     */
    @Override
    public boolean evaluate(final Version version) {
        return version.equals(this.version);
    }
}
