/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.rest.yaml.section;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.collect.Tuple;
import org.opensearch.core.xcontent.XContentLocation;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;

/**
 * Base class for all ordering assertions (gt/gte/lt/lte).
 * It provides:
 *  - shared YAML parse boilerplate (via {@link #parseOrderingAssertion})
 *  - numeric normalization so both sides are Comparable of the same class
 *  - common comparison logic based on a {@link Relation}
 */
public abstract class OrderingAssertion extends Assertion {
    protected OrderingAssertion(XContentLocation location, String field, Object expectedValue) {
        super(location, field, expectedValue);
    }

    private static final Logger logger = LogManager.getLogger(OrderingAssertion.class);

    protected enum Relation {
        GT,
        GTE,
        LT,
        LTE
    }

    protected abstract Relation relation();

    protected abstract String errorMessage();

    @FunctionalInterface
    protected interface OrderingAssertionFactory<T extends OrderingAssertion> {
        T create(XContentLocation location, String field, Object expectedValue);
    }

    /**
     * Common parser for {gt|gte|lt|lte}: { field: expectedValue }
     */
    protected static <T extends OrderingAssertion> T parseOrderingAssertion(
        XContentParser parser,
        Relation relation,
        OrderingAssertionFactory<T> factory
    ) throws IOException {
        XContentLocation location = parser.getTokenLocation();
        Tuple<String, Object> t = ParserUtils.parseTuple(parser);
        Object expected = t.v2();
        if (!(expected instanceof Comparable)) {
            throw new IllegalArgumentException(
                relation
                    + " section can only be used with objects that support natural ordering, found "
                    + expected.getClass().getSimpleName()
            );
        }
        return factory.create(location, t.v1(), expected);
    }

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    protected final void doAssert(Object actualValue, Object expectedValue) {
        logger.trace("assert that [{}] {} [{}] (field: [{}])", actualValue, relationToString(relation()), expectedValue, getField());

        Object castActual = convertActualValue(actualValue, expectedValue);
        Tuple<Object, Object> normalized = normalizePair(castActual, expectedValue);
        Object actualNormalized = normalized.v1();
        Object expectedNormalized = normalized.v2();

        assertThat(
            "value of [" + getField() + "] is not comparable (got [" + safeClass(actualNormalized) + "])",
            actualNormalized,
            instanceOf(Comparable.class)
        );
        assertThat(
            "expected value of [" + getField() + "] is not comparable (got [" + expectedNormalized.getClass() + "])",
            expectedNormalized,
            instanceOf(Comparable.class)
        );

        try {
            int cmp = ((Comparable) actualNormalized).compareTo(expectedNormalized);
            boolean ok = switch (relation()) {
                case GT -> cmp > 0;
                case GTE -> cmp >= 0;
                case LT -> cmp < 0;
                case LTE -> cmp <= 0;
            };
            if (!ok) {
                fail(errorMessage());
            }
        } catch (ClassCastException e) {
            fail("cast error while checking (" + errorMessage() + "): " + e);
        }
    }

    private static String relationToString(Relation r) {
        return switch (r) {
            case GT -> "is greater than";
            case GTE -> "is greater than or equal to";
            case LT -> "is less than";
            case LTE -> "is less than or equal to";
        };
    }

    /**
     * Normalize a pair to comparable types to avoid Integer vs Long / Double vs Integer ClassCastExceptions.
     * Rules:
     *  - If both are Numbers and any is floating (Float/Double), coerce both to Double.
     *  - Else if both are Numbers, coerce both to Long (widening smaller integral types).
     *  - Else return as-is (let Comparable semantics handle it).
     */
    private static Tuple<Object, Object> normalizePair(Object a, Object b) {
        if (a instanceof Number aNum && b instanceof Number bNum) {
            boolean isFloating = aNum instanceof Float || aNum instanceof Double || bNum instanceof Float || bNum instanceof Double;
            if (isFloating) {
                return Tuple.tuple(aNum.doubleValue(), bNum.doubleValue());
            } else {
                return Tuple.tuple(aNum.longValue(), bNum.longValue());
            }
        }
        return Tuple.tuple(a, b);
    }
}
