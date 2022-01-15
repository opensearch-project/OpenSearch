/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.aggregations.bucket.missing;

import org.opensearch.common.inject.Provider;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

import java.io.IOException;
import java.util.function.Predicate;

/**
 * Composite Aggregation Missing bucket order.
 *
 */
public enum MissingOrder implements Writeable {
    /**
     * missing first.
     */
    FIRST {
        @Override
        public <T, U> int compare(
            Provider<T> left, Predicate<T> leftIsMissing, Provider<U> right, Predicate<U> rightIsMissing, int reverseMul) {
            if (leftIsMissing.test(left.get())) {
                return rightIsMissing.test(right.get()) ? -1 : 0;
            } else if (rightIsMissing.test(right.get())) {
                return 1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

//        @Override public MissingOrder reverseOrder() {
//            return LAST;
//        }

        @Override public String toString() {
            return "_first";
        }
    },

    /**
     * missing last.
     */
    LAST {
        @Override
        public <T, U> int compare(
            Provider<T> left, Predicate<T> leftIsMissing, Provider<U> right, Predicate<U> rightIsMissing, int reverseMul) {
            if (leftIsMissing.test(left.get())) {
                return rightIsMissing.test(right.get()) ? 1 : 0;
            } else if (rightIsMissing.test(right.get())) {
                return -1;
            }
            return MISSING_ORDER_UNKNOWN;
        }

//        @Override
//        public MissingOrder reverseOrder() {
//            return FIRST;
//        }

        @Override
        public String toString() {
            return "_last";
        }
    },

    /**
     * Default: ASC missing first / DESC missing last
     */
    DEFAULT {
        @Override
        public <T, U> int compare(
            Provider<T> left, Predicate<T> leftIsMissing, Provider<U> right, Predicate<U> rightIsMissing, int reverseMul) {
            if (leftIsMissing.test(left.get())) {
                return rightIsMissing.test(right.get()) ? -1 * reverseMul : 0;
            } else if (rightIsMissing.test(right.get())) {
                return reverseMul;
            }
            return MISSING_ORDER_UNKNOWN;
        }
//
//        @Override public int compareToAnyValue(int reverseMul) {
//            return -1 * reverseMul;
//        }
//
//        @Override public MissingOrder reverseOrder() {
//            return DEFAULT_REVERSE;
//        }

        @Override public String toString() {
            return "default";
        }
    };

    /**
     * Default: ASC missing first / DESC missing last
     */
//    DEFAULT_REVERSE {
//        @Override public int compareToAnyValue(int reverseMul) {
//            return reverseMul;
//        }
//
//        @Override public MissingOrder reverseOrder() {
//            return DEFAULT;
//        }
//
//        @Override public String toString() {
//            return "default reverse";
//        }
//    };

    private static int MISSING_ORDER_UNKNOWN = Integer.MIN_VALUE;

    public static MissingOrder readFromStream(StreamInput in) throws IOException {
        return in.readEnum(MissingOrder.class);
    }

    @Override public void writeTo(StreamOutput out) throws IOException {
        out.writeEnum(this);
    }

    public static boolean isDefault(MissingOrder order) {return order == DEFAULT;}

    // Todo
    public static MissingOrder fromString(String op) {
        return DEFAULT;
    }

    public static boolean unknownOrder(int v) {
        return v == MISSING_ORDER_UNKNOWN;
    }

//    public abstract int compareToAnyValue(int reverseMul);

//    public abstract MissingOrder reverseOrder();

    public abstract <T, U> int compare(
        Provider<T> left, Predicate<T> leftIsMissing, Provider<U> right, Predicate<U> rightIsMissing, int reverseMul);
}
