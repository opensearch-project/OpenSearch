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
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.semver.expr.Caret;
import org.opensearch.semver.expr.Equal;
import org.opensearch.semver.expr.Expression;
import org.opensearch.semver.expr.Tilde;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;

import static java.util.Arrays.stream;

/**
 * Represents a single semver range that allows for specifying which {@code org.opensearch.Version}s satisfy the range.
 * It is composed of a range version and a range operator. Following are the supported operators:
 * <ul>
 *     <li>'=' Requires exact match with the range version. For example, =1.2.3 range would match only 1.2.3</li>
 *     <li>'~' Allows for patch version variability starting from the range version. For example, ~1.2.3 range would match versions greater than or equal to 1.2.3 but less than 1.3.0</li>
 *     <li>'^' Allows for patch and minor version variability starting from the range version. For example, ^1.2.3 range would match versions greater than or equal to 1.2.3 but less than 2.0.0</li>
 * </ul>
 *
 * @opensearch.api
 */
@PublicApi(since = "2.13.0")
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
        RangeOperator rangeOperator = RangeOperator.fromRange(range);
        String version = range.replaceFirst(rangeOperator.asEscapedString(), "");
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
     * @param versionToEvaluate version to check
     * @return {@code true} if range is satisfied by version, {@code false} otherwise
     */
    public boolean isSatisfiedBy(final String versionToEvaluate) {
        return isSatisfiedBy(Version.fromString(versionToEvaluate));
    }

    /**
     * Check if range is satisfied by given version.
     *
     * @param versionToEvaluate version to check
     * @return {@code true} if range is satisfied by version, {@code false} otherwise
     * @see #isSatisfiedBy(String)
     */
    public boolean isSatisfiedBy(final Version versionToEvaluate) {
        return this.rangeOperator.expression.evaluate(this.rangeVersion, versionToEvaluate);
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

        EQ("=", new Equal()),
        TILDE("~", new Tilde()),
        CARET("^", new Caret()),
        DEFAULT("", new Equal());

        private final String operator;
        private final Expression expression;

        RangeOperator(final String operator, final Expression expression) {
            this.operator = operator;
            this.expression = expression;
        }

        /**
         * String representation of the range operator.
         *
         * @return range operator as string
         */
        public String asString() {
            return operator;
        }

        /**
         * Escaped string representation of the range operator,
         * if operator is a regex character.
         *
         * @return range operator as escaped string, if operator is a regex character
         */
        public String asEscapedString() {
            if (Objects.equals(operator, "^")) {
                return "\\^";
            }
            return operator;
        }

        public static RangeOperator fromRange(final String range) {
            Optional<RangeOperator> rangeOperator = stream(values()).filter(
                operator -> operator != DEFAULT && range.startsWith(operator.asString())
            ).findFirst();
            return rangeOperator.orElse(DEFAULT);
        }
    }
}
