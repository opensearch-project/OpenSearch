/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.hash;

import org.opensearch.common.annotation.InternalApi;

import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;

import static java.lang.Long.rotateRight;

/**
 * t1ha: Fast Positive Hash
 *
 * <p>
 * Implements <a href="https://github.com/erthink/t1ha#t1ha1--64-bits-baseline-fast-portable-hash">t1ha1</a>;
 * a fast portable hash function with reasonable quality for checksums, hash tables, and thin fingerprinting.
 *
 * <p>
 * To overcome language and performance limitations, this implementation differs slightly from the
 * <a href="https://github.com/erthink/t1ha/blob/master/src/t1ha1.c">reference implementation</a> in C++,
 * so the returned values may vary before JDK 18.
 *
 * <p>
 * Intended for little-endian systems but returns the same result on big-endian, albeit marginally slower.
 *
 * @opensearch.internal
 */
@InternalApi
public final class T1ha1 {
    private static final long SEED = System.nanoTime();
    private static final Mux64 MUX_64_IMPL = fastestMux64Impl();

    private static final VarHandle LONG_HANDLE = MethodHandles.byteArrayViewVarHandle(long[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle INT_HANDLE = MethodHandles.byteArrayViewVarHandle(int[].class, ByteOrder.LITTLE_ENDIAN);
    private static final VarHandle SHORT_HANDLE = MethodHandles.byteArrayViewVarHandle(short[].class, ByteOrder.LITTLE_ENDIAN);

    // "Magic" primes:
    private static final long p0 = 0xEC99BF0D8372CAABL;
    private static final long p1 = 0x82434FE90EDCEF39L;
    private static final long p2 = 0xD4F06DB99D67BE4BL;
    private static final long p3 = 0xBD9CACC22C6E9571L;
    private static final long p4 = 0x9C06FAF4D023E3ABL;
    private static final long p5 = 0xC060724A8424F345L;
    private static final long p6 = 0xCB5AF53AE3AAAC31L;

    // Rotations:
    private static final int s0 = 41;
    private static final int s1 = 17;
    private static final int s2 = 31;

    /**
     * No public constructor.
     */
    private T1ha1() {}

    /**
     * Returns the hash code for the specified range of the given {@code byte} array.
     * @param input the input byte array
     * @param offset the starting offset
     * @param length the length of the range
     * @return hash code
     */
    public static long hash(byte[] input, int offset, int length) {
        return hash(input, offset, length, SEED);
    }

    /**
     * Returns the hash code for the specified range of the given {@code byte} array.
     * @param input the input byte array
     * @param offset the starting offset
     * @param length the length of the range
     * @param seed customized seed
     * @return hash code
     */
    public static long hash(byte[] input, int offset, int length, long seed) {
        long a = seed;
        long b = length;

        if (length > 32) {
            long c = rotateRight(length, s1) + seed;
            long d = length ^ rotateRight(seed, s1);

            do {
                long w0 = fetch64(input, offset);
                long w1 = fetch64(input, offset + 8);
                long w2 = fetch64(input, offset + 16);
                long w3 = fetch64(input, offset + 24);

                long d02 = w0 ^ rotateRight(w2 + d, s1);
                long c13 = w1 ^ rotateRight(w3 + c, s1);
                c += a ^ rotateRight(w0, s0);
                d -= b ^ rotateRight(w1, s2);
                a ^= p1 * (d02 + w3);
                b ^= p0 * (c13 + w2);

                offset += 32;
                length -= 32;
            } while (length >= 32);

            a ^= p6 * (rotateRight(c, s1) + d);
            b ^= p5 * (rotateRight(d, s1) + c);
        }

        return h32(input, offset, length, a, b);
    }

    /**
     * Computes the hash of up to 32 bytes.
     * Constants in the switch expression are dense; JVM will use them as indices into a table of
     * instruction pointers (tableswitch instruction), making lookups really fast.
     */
    @SuppressWarnings("fallthrough")
    private static long h32(byte[] input, int offset, int length, long a, long b) {
        switch (length) {
            default:
                b += mux64(fetch64(input, offset), p4);
                offset += 8;
                length -= 8;
            case 24:
            case 23:
            case 22:
            case 21:
            case 20:
            case 19:
            case 18:
            case 17:
                a += mux64(fetch64(input, offset), p3);
                offset += 8;
                length -= 8;
            case 16:
            case 15:
            case 14:
            case 13:
            case 12:
            case 11:
            case 10:
            case 9:
                b += mux64(fetch64(input, offset), p2);
                offset += 8;
                length -= 8;
            case 8:
            case 7:
            case 6:
            case 5:
            case 4:
            case 3:
            case 2:
            case 1:
                a += mux64(tail64(input, offset, length), p1);
            case 0:
                // Final weak avalanche
                return mux64(rotateRight(a + b, s1), p4) + mix64(a ^ b, p0);
        }
    }

    /**
     * XOR the high and low parts of the full 128-bit product.
     */
    private static long mux64(long a, long b) {
        return MUX_64_IMPL.mux64(a, b);
    }

    /**
     * XOR-MUL-XOR bit-mixer.
     */
    private static long mix64(long a, long b) {
        a *= b;
        return a ^ rotateRight(a, s0);
    }

    /**
     * Reads "length" bytes starting at "offset" in little-endian order; returned as long.
     * It is assumed that the length is between 1 and 8 (inclusive); but no defensive checks are made as such.
     */
    private static long tail64(byte[] input, int offset, int length) {
        switch (length) {
            case 1:
                return fetch8(input, offset);
            case 2:
                return fetch16(input, offset);
            case 3:
                return fetch16(input, offset) | (fetch8(input, offset + 2) << 16);
            case 4:
                return fetch32(input, offset);
            case 5:
                return fetch32(input, offset) | (fetch8(input, offset + 4) << 32);
            case 6:
                return fetch32(input, offset) | (fetch16(input, offset + 4) << 32);
            case 7:
                // This is equivalent to:
                // return fetch32(input, offset) | (fetch16(input, offset + 4) << 32) | (fetch8(input, offset + 6) << 48);
                // But reading two ints overlapping by one byte is faster due to lesser instructions.
                return fetch32(input, offset) | (fetch32(input, offset + 3) << 24);
            default:
                return fetch64(input, offset);
        }
    }

    /**
     * Reads a 64-bit long.
     */
    private static long fetch64(byte[] input, int offset) {
        return (long) LONG_HANDLE.get(input, offset);
    }

    /**
     * Reads a 32-bit unsigned integer, returned as long.
     */
    private static long fetch32(byte[] input, int offset) {
        return (int) INT_HANDLE.get(input, offset) & 0xFFFFFFFFL;
    }

    /**
     * Reads a 16-bit unsigned short, returned as long.
     */
    private static long fetch16(byte[] input, int offset) {
        return (short) SHORT_HANDLE.get(input, offset) & 0xFFFFL;
    }

    /**
     * Reads an 8-bit unsigned byte, returned as long.
     */
    private static long fetch8(byte[] input, int offset) {
        return input[offset] & 0xFFL;
    }

    /**
     * The implementation of mux64.
     */
    @FunctionalInterface
    private interface Mux64 {
        long mux64(long a, long b);
    }

    /**
     * Provides the fastest available implementation of mux64 on this platform.
     *
     * <p>
     * Ideally, the following should be returned to match the reference implementation:
     * {@code Math.unsignedMultiplyHigh(a, b) ^ (a * b)}
     *
     * <p>
     * Since unsignedMultiplyHigh isn't available before JDK 18, and calculating it without intrinsics is quite slow,
     * the multiplyHigh method is used instead. Slight loss in quality is imperceptible for our use-case: a hash table.
     * {@code Math.multiplyHigh(a, b) ^ (a * b)}
     *
     * <p>
     * This indirection can be removed once we stop supporting older JDKs.
     */
    private static Mux64 fastestMux64Impl() {
        try {
            final MethodHandle unsignedMultiplyHigh = MethodHandles.publicLookup()
                .findStatic(Math.class, "unsignedMultiplyHigh", MethodType.methodType(long.class, long.class, long.class));
            return (a, b) -> {
                try {
                    return (long) unsignedMultiplyHigh.invokeExact(a, b) ^ (a * b);
                } catch (Throwable e) {
                    throw new RuntimeException(e);
                }
            };
        } catch (NoSuchMethodException e) {
            return (a, b) -> Math.multiplyHigh(a, b) ^ (a * b);
        } catch (IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }
}
