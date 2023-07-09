/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.common;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * A set of utilities for numbers.
 *
 * @opensearch.internal
 */
public final class Numbers {
    public static final BigInteger MAX_UNSIGNED_LONG_VALUE = BigInteger.ONE.shiftLeft(64).subtract(BigInteger.ONE);
    public static final BigInteger MIN_UNSIGNED_LONG_VALUE = BigInteger.ZERO;

    public static final long MIN_UNSIGNED_LONG_VALUE_AS_LONG = MIN_UNSIGNED_LONG_VALUE.longValue();
    public static final long MAX_UNSIGNED_LONG_VALUE_AS_LONG = MAX_UNSIGNED_LONG_VALUE.longValue();

    private static final BigInteger MAX_LONG_VALUE = BigInteger.valueOf(Long.MAX_VALUE);
    private static final BigInteger MIN_LONG_VALUE = BigInteger.valueOf(Long.MIN_VALUE);

    private Numbers() {}

    /**
     * Converts a long to a byte array.
     *
     * @param val The long to convert to a byte array
     * @return The byte array converted
     */
    public static byte[] longToBytes(long val) {
        byte[] arr = new byte[8];
        arr[0] = (byte) (val >>> 56);
        arr[1] = (byte) (val >>> 48);
        arr[2] = (byte) (val >>> 40);
        arr[3] = (byte) (val >>> 32);
        arr[4] = (byte) (val >>> 24);
        arr[5] = (byte) (val >>> 16);
        arr[6] = (byte) (val >>> 8);
        arr[7] = (byte) (val);
        return arr;
    }

    /** Returns true if value is neither NaN nor infinite. */
    public static boolean isValidDouble(double value) {
        if (Double.isNaN(value) || Double.isInfinite(value)) {
            return false;
        }
        return true;
    }

    /** Return the long that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value. */
    public static long toLongExact(Number n) {
        if (n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long) {
            return n.longValue();
        } else if (n instanceof Float || n instanceof Double) {
            double d = n.doubleValue();
            if (d != Math.round(d)) {
                throw new IllegalArgumentException(n + " is not an integer value");
            }
            return n.longValue();
        } else if (n instanceof BigDecimal) {
            return ((BigDecimal) n).toBigIntegerExact().longValueExact();
        } else if (n instanceof BigInteger) {
            return ((BigInteger) n).longValueExact();
        } else {
            throw new IllegalArgumentException(
                "Cannot check whether [" + n + "] of class [" + n.getClass().getName() + "] is actually a long"
            );
        }
    }

    /** Return the {@link BigInteger} that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a {@link BigInteger} that stores the exact same
     *  value. */
    public static BigInteger toBigIntegerExact(Number n) {
        if (n instanceof Byte || n instanceof Short || n instanceof Integer || n instanceof Long) {
            return BigInteger.valueOf(n.longValue());
        } else if (n instanceof Float || n instanceof Double) {
            double d = n.doubleValue();
            if (d != Math.round(d)) {
                throw new IllegalArgumentException(n + " is not an integer value");
            }
            return BigInteger.valueOf(n.longValue());
        } else if (n instanceof BigDecimal) {
            return ((BigDecimal) n).toBigIntegerExact();
        } else if (n instanceof BigInteger) {
            return ((BigInteger) n);
        } else {
            throw new IllegalArgumentException("Cannot convert [" + n + "] of class [" + n.getClass().getName() + "] to a BigInteger");
        }
    }

    /** Return the unsigned long (as {@link BigInteger}) that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to an unsigned long that stores the exact same
     *  value. */
    public static BigInteger toUnsignedLongExact(Number value) {
        final BigInteger v = Numbers.toBigIntegerExact(value);

        if (v.compareTo(MAX_UNSIGNED_LONG_VALUE) > 0 || v.compareTo(MIN_UNSIGNED_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + value + "] is out of range for an unsigned long");
        }

        return v;
    }

    // weak bounds on the BigDecimal representation to allow for coercion
    private static BigDecimal BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE = BigDecimal.valueOf(Long.MAX_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE = BigDecimal.valueOf(Long.MIN_VALUE).subtract(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_GREATER_THAN_USIGNED_LONG_MAX_VALUE = new BigDecimal(MAX_UNSIGNED_LONG_VALUE).add(BigDecimal.ONE);
    private static BigDecimal BIGDECIMAL_LESS_THAN_USIGNED_LONG_MIN_VALUE = new BigDecimal(MIN_UNSIGNED_LONG_VALUE).subtract(
        BigDecimal.ONE
    );

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    public static long toLong(String stringValue, boolean coerce) {
        try {
            return Long.parseLong(stringValue);
        } catch (NumberFormatException e) {
            // we will try again with BigDecimal
        }

        final BigInteger bigIntegerValue;
        try {
            BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_LONG_MAX_VALUE) >= 0
                || bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(MAX_LONG_VALUE) > 0 || bigIntegerValue.compareTo(MIN_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for a long");
        }

        return bigIntegerValue.longValue();
    }

    /** Return the long that {@code stringValue} stores or throws an exception if the
     *  stored value cannot be converted to a long that stores the exact same
     *  value and {@code coerce} is false. */
    public static BigInteger toUnsignedLong(String stringValue, boolean coerce) {
        final BigInteger bigIntegerValue;
        try {
            BigDecimal bigDecimalValue = new BigDecimal(stringValue);
            if (bigDecimalValue.compareTo(BIGDECIMAL_GREATER_THAN_USIGNED_LONG_MAX_VALUE) >= 0
                || bigDecimalValue.compareTo(BIGDECIMAL_LESS_THAN_USIGNED_LONG_MIN_VALUE) <= 0) {
                throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for an unsigned long");
            }
            bigIntegerValue = coerce ? bigDecimalValue.toBigInteger() : bigDecimalValue.toBigIntegerExact();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException("Value [" + stringValue + "] has a decimal part");
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("For input string: \"" + stringValue + "\"");
        }

        if (bigIntegerValue.compareTo(MAX_UNSIGNED_LONG_VALUE) > 0 || bigIntegerValue.compareTo(MIN_UNSIGNED_LONG_VALUE) < 0) {
            throw new IllegalArgumentException("Value [" + stringValue + "] is out of range for an unsigned long");
        }

        return bigIntegerValue;
    }

    /** Return the int that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to an int that stores the exact same
     *  value. */
    public static int toIntExact(Number n) {
        return Math.toIntExact(toLongExact(n));
    }

    /** Return the short that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a short that stores the exact same
     *  value. */
    public static short toShortExact(Number n) {
        long l = toLongExact(n);
        if (l != (short) l) {
            throw new ArithmeticException("short overflow: " + l);
        }
        return (short) l;
    }

    /** Return the byte that {@code n} stores, or throws an exception if the
     *  stored value cannot be converted to a byte that stores the exact same
     *  value. */
    public static byte toByteExact(Number n) {
        long l = toLongExact(n);
        if (l != (byte) l) {
            throw new ArithmeticException("byte overflow: " + l);
        }
        return (byte) l;
    }

    /**
     * Return a BigInteger equal to the unsigned value of the
     * argument.
     */
    public static BigInteger toUnsignedBigInteger(long i) {
        if (i >= 0L) return BigInteger.valueOf(i);
        else {
            int upper = (int) (i >>> 32);
            int lower = (int) i;

            // return (upper << 32) + lower
            return (BigInteger.valueOf(Integer.toUnsignedLong(upper))).shiftLeft(32).add(BigInteger.valueOf(Integer.toUnsignedLong(lower)));
        }
    }

    /**
     * Convert unsigned long to double value (see please Guava's com.google.common.primitives.UnsignedLong),
     * this is faster then going through {@link #toUnsignedBigInteger(long)} conversion.
     */
    public static double unsignedLongToDouble(long value) {
        if (value >= 0) {
            return (double) value;
        }

        // The top bit is set, which means that the double value is going to come from the top 53 bits.
        // So we can ignore the bottom 11, except for rounding. We can unsigned-shift right 1, aka
        // unsigned-divide by 2, and convert that. Then we'll get exactly half of the desired double
        // value. But in the specific case where the bottom two bits of the original number are 01, we
        // want to replace that with 1 in the shifted value for correct rounding.
        return (double) ((value >>> 1) | (value & 1)) * 2.0;
    }
}
