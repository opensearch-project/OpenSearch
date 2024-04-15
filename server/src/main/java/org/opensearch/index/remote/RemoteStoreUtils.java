/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.collect.Tuple;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Base64;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.function.Function;

/**
 * Utils for remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreUtils {
    public static final int LONG_MAX_LENGTH = String.valueOf(Long.MAX_VALUE).length();

    /**
     * URL safe base 64 character set. This must not be changed as this is used in deriving the base64 equivalent of binary.
     */
    static final char[] URL_BASE64_CHARSET = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_".toCharArray();

    /**
     * This method subtracts given numbers from Long.MAX_VALUE and returns a string representation of the result.
     * The resultant string is guaranteed to be of the same length that of Long.MAX_VALUE. If shorter, we add left padding
     * of 0s to the string.
     *
     * @param num number to get the inverted long string for
     * @return String value of Long.MAX_VALUE - num
     */
    public static String invertLong(long num) {
        if (num < 0) {
            throw new IllegalArgumentException("Negative long values are not allowed");
        }
        String invertedLong = String.valueOf(Long.MAX_VALUE - num);
        char[] characterArray = new char[LONG_MAX_LENGTH - invertedLong.length()];
        Arrays.fill(characterArray, '0');

        return new String(characterArray) + invertedLong;
    }

    /**
     * This method converts the given string into long and subtracts it from Long.MAX_VALUE
     *
     * @param str long in string format to be inverted
     * @return long value of the invert result
     */
    public static long invertLong(String str) {
        long num = Long.parseLong(str);
        if (num < 0) {
            throw new IllegalArgumentException("Strings representing negative long values are not allowed");
        }
        return Long.MAX_VALUE - num;
    }

    /**
     * Extracts the segment name from the provided segment file name
     *
     * @param filename Segment file name to parse
     * @return Name of the segment that the segment file belongs to
     */
    public static String getSegmentName(String filename) {
        // Segment file names follow patterns like "_0.cfe" or "_0_1_Lucene90_0.dvm".
        // Here, the segment name is "_0", which is the set of characters
        // starting with "_" until the next "_" or first ".".
        int endIdx = filename.indexOf('_', 1);
        if (endIdx == -1) {
            endIdx = filename.indexOf('.');
        }

        if (endIdx == -1) {
            throw new IllegalArgumentException("Unable to infer segment name for segment file " + filename);
        }

        return filename.substring(0, endIdx);
    }

    /**
     * @param mdFiles List of segment/translog metadata files
     * @param fn      Function to extract PrimaryTerm_Generation and Node Id from metadata file name .
     *                fn returns null if node id is not part of the file name
     */
    public static void verifyNoMultipleWriters(List<String> mdFiles, Function<String, Tuple<String, String>> fn) {
        Map<String, String> nodesByPrimaryTermAndGen = new HashMap<>();
        mdFiles.forEach(mdFile -> {
            Tuple<String, String> nodeIdByPrimaryTermAndGen = fn.apply(mdFile);
            if (nodeIdByPrimaryTermAndGen != null) {
                if (nodesByPrimaryTermAndGen.containsKey(nodeIdByPrimaryTermAndGen.v1())
                    && (!nodesByPrimaryTermAndGen.get(nodeIdByPrimaryTermAndGen.v1()).equals(nodeIdByPrimaryTermAndGen.v2()))) {
                    throw new IllegalStateException(
                        "Multiple metadata files from different nodes"
                            + "having same primary term and generations "
                            + nodeIdByPrimaryTermAndGen.v1()
                            + " detected "
                    );
                }
                nodesByPrimaryTermAndGen.put(nodeIdByPrimaryTermAndGen.v1(), nodeIdByPrimaryTermAndGen.v2());
            }
        });
    }

    /**
     * Converts an input hash which occupies 64 bits of space into Base64 (6 bits per character) String. This must not
     * be changed as it is used for creating path for storing remote store data on the remote store.
     * This converts the byte array to base 64 string. `/` is replaced with `_`, `+` is replaced with `-` and `=`
     * which is padded at the last is also removed. These characters are either used as delimiter or special character
     * requiring special handling in some vendors. The characters present in this base64 version are [A-Za-z0-9_-].
     * This must not be changed as it is used for creating path for storing remote store data on the remote store.
     */
    static String longToUrlBase64(long value) {
        byte[] hashBytes = ByteBuffer.allocate(Long.BYTES).putLong(value).array();
        String base64Str = Base64.getUrlEncoder().encodeToString(hashBytes);
        return base64Str.substring(0, base64Str.length() - 1);
    }

    static long urlBase64ToLong(String base64Str) {
        byte[] hashBytes = Base64.getUrlDecoder().decode(base64Str);
        return ByteBuffer.wrap(hashBytes).getLong();
    }

    /**
     * Converts an input hash which occupies 64 bits of memory into a composite encoded string. The string will have 2 parts -
     * 1. Base 64 string and 2. Binary String. We will use the first 6 bits for creating the base 64 string.
     * For the second part, the rest of the bits (of length {@code len}-6) will be used as is in string form.
     */
    static String longToCompositeBase64AndBinaryEncoding(long value, int len) {
        if (len < 7 || len > 64) {
            throw new IllegalArgumentException("In longToCompositeBase64AndBinaryEncoding, len must be between 7 and 64 (both inclusive)");
        }
        String binaryEncoding = String.format(Locale.ROOT, "%64s", Long.toBinaryString(value)).replace(' ', '0');
        String base64Part = binaryEncoding.substring(0, 6);
        String binaryPart = binaryEncoding.substring(6, len);
        int base64DecimalValue = Integer.valueOf(base64Part, 2);
        assert base64DecimalValue >= 0 && base64DecimalValue < 64;
        return URL_BASE64_CHARSET[base64DecimalValue] + binaryPart;
    }
}
