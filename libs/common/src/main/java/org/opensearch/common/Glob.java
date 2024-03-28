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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

/**
 * Utility class for glob-like matching
 *
 * @opensearch.api
 */
public class Glob {

    /**
     * Match a String against the given pattern, supporting the following simple
     * pattern styles: "xxx*", "*xxx", "*xxx*" and "xxx*yyy" matches (with an
     * arbitrary number of pattern parts), as well as direct equality.
     *
     * @param pattern the pattern to match against
     * @param str     the String to match
     * @return whether the String matches the given pattern
     */
    public static boolean globMatch(String pattern, String str) {

        if (pattern == null || str == null) {
            return false;
        }

        int stringIndex = 0;
        int patternIndex = 0;
        int wildcardIndex = -1;
        int wildcardStringIndex = -1;

        while (stringIndex < str.length()) {
            // pattern and string match
            if (patternIndex < pattern.length() && str.charAt(stringIndex) == pattern.charAt(patternIndex)) {
                stringIndex++;
                patternIndex++;
            } else if (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
                // wildcard found
                wildcardIndex = patternIndex;
                patternIndex++;
                wildcardStringIndex = stringIndex;
            } else if (wildcardIndex != -1) {
                // last pattern pointer was a wildcard
                patternIndex = wildcardIndex + 1;
                wildcardStringIndex++;
                stringIndex = wildcardStringIndex;
            } else {
                // characters do not match
                return false;
            }
        }

        while (patternIndex < pattern.length() && pattern.charAt(patternIndex) == '*') {
            patternIndex++;
        }

        return patternIndex == pattern.length();
    }

}
