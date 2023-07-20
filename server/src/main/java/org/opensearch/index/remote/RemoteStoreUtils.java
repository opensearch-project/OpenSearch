/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.lucene.index.CorruptIndexException;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Utils for remote store
 *
 * @opensearch.internal
 */
public class RemoteStoreUtils {
    public static final int LONG_MAX_LENGTH = String.valueOf(Long.MAX_VALUE).length();

    /**
     * This method subtracts given numbers from Long.MAX_VALUE and returns a string representation of the result.
     * The resultant string is guaranteed to be of the same length that of Long.MAX_VALUE. If shorter, we add left padding
     * of 0s to the string.
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
     * Extracts the Lucene major version from the provided DocValuesUpdates file name
     * @param filename DocValuesUpdates file name to parse
     * @return Lucene major version that wrote the DocValuesUpdates file
     * @throws CorruptIndexException If the Lucene major version cannot be inferred
     */
    public static int getLuceneVersionForDocValuesUpdates(String filename) throws CorruptIndexException {
        // TODO: The following regex could work incorrectly if both major and minor versions are double-digits.
        // This is because the major and minor versions do not have a separator in the filename currently
        // (Lucence<major><minor>).
        // We may need to revisit this if the filename pattern is updated in future Lucene versions.
        Pattern docValuesUpdatesFileNamePattern = Pattern.compile("_\\d+_\\d+_Lucene(\\d+)\\d+_\\d+");

        Matcher matcher = docValuesUpdatesFileNamePattern.matcher(filename);
        if (matcher.find()) {
            return Integer.parseInt(matcher.group(1));
        } else {
            throw new CorruptIndexException("Unable to infer Lucene version for segment file " + filename, filename);
        }
    }
}
