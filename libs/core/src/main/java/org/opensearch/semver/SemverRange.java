/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.semver;

import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.semver.expr.Equal;
import org.opensearch.semver.expr.Expression;
import org.opensearch.semver.expr.Tilde;

import java.io.IOException;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;

import static java.lang.String.format;
import static java.util.Arrays.stream;

/**
 * Represents a single semver range.
 */
public class SemverRange implements ToXContentFragment {

    private final Version rangeVersion;
    private final RangeOperator rangeOperator;

    public SemverRange(final Version rangeVersion, final RangeOperator rangeOperator) {
        this.rangeVersion = rangeVersion;
        this.rangeOperator = rangeOperator;
    }

    /**
     * Constructs a {@code SemverRange} from its string representation.
     * @param range given range
     * @return a {@code SemverRange}
     */
    public static SemverRange fromString(final String range) {
        Optional<RangeOperator> operator = stream(RangeOperator.values()).filter(
            rangeOperator -> rangeOperator != RangeOperator.DEFAULT && range.startsWith(rangeOperator.asString())
        ).findFirst();
        RangeOperator rangeOperator = operator.orElse(RangeOperator.DEFAULT);
        String version = range.replaceFirst(rangeOperator.asString(), "");
        if (!Version.stringHasLength(version)) {
            throw new IllegalArgumentException("Version cannot be empty");
        }
        return new SemverRange(Version.fromString(version), rangeOperator);
    }

    /**
     * Return the range operator for this range.
     * @return range operator
     */
    public RangeOperator getRangeOperator() {
        return rangeOperator;
    }

    /**
     * Return the version for this range.
     * @return the range version
     */
    public Version getRangeVersion() {
        return rangeVersion;
    }

    /**
     * Check if range is satisfied by given version string.
     *
     * @param version version to check
     * @return {@code true} if range is satisfied by version, {@code false} otherwise
     */
    public boolean isSatisfiedBy(final String version) {
        return isSatisfiedBy(Version.fromString(version));
    }

    /**
     * Check if range is satisfied by given version.
     *
     * @param version version to check
     * @return {@code true} if range is satisfied by version, {@code false} otherwise
     * @see #isSatisfiedBy(String)
     */
    public boolean isSatisfiedBy(final Version version) {
        Expression expression = null;
        switch (rangeOperator) {
            case DEFAULT:
            case EQ:
                expression = new Equal(rangeVersion);
                break;
            case TILDE:
                expression = new Tilde(rangeVersion);
                break;
            default:
                throw new RuntimeException(format(Locale.ROOT, "Unsupported range operator: %s", rangeOperator));
        }
        return expression.evaluate(version);
    }

    @Override
    public boolean equals(@Nullable final Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        SemverRange range = (SemverRange) o;
        return Objects.equals(rangeVersion, range.rangeVersion) && rangeOperator == range.rangeOperator;
    }

    @Override
    public int hashCode() {
        return Objects.hash(rangeVersion, rangeOperator);
    }

    @Override
    public String toString() {
        return rangeOperator.asString() + rangeVersion;
    }

    @Override
    public XContentBuilder toXContent(final XContentBuilder builder, final Params params) throws IOException {
        return builder.value(toString());
    }

    /**
     * A range operator.
     */
    public enum RangeOperator {

        EQ("="),
        TILDE("~"),
        DEFAULT("");

        private final String operator;

        RangeOperator(final String operator) {
            this.operator = operator;
        }

        /**
         * String representation of the range operator.
         *
         * @return range operator as string
         */
        public String asString() {
            return operator;
        }
    }
}
