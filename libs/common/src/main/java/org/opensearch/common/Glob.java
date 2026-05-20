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
        int sIdx = 0, pIdx = 0, match = 0, wildcardIdx = -1;
        while (sIdx < str.length()) {
            // both chars matching, incrementing both pointers
            if (pIdx < pattern.length() && str.charAt(sIdx) == pattern.charAt(pIdx)) {
                sIdx++;
                pIdx++;
            } else if (pIdx < pattern.length() && pattern.charAt(pIdx) == '*') {
                // wildcard found, only incrementing pattern pointer
                wildcardIdx = pIdx;
                match = sIdx;
                pIdx++;
            } else if (wildcardIdx != -1) {
                // last pattern pointer was a wildcard, incrementing string pointer
                pIdx = wildcardIdx + 1;
                match++;
                sIdx = match;
            } else {
                // current pattern pointer is not a wildcard, last pattern pointer was also not a wildcard
                // characters do not match
                return false;
            }
        }

        // check for remaining characters in pattern
        while (pIdx < pattern.length() && pattern.charAt(pIdx) == '*') {
            pIdx++;
        }

        return pIdx == pattern.length();
    }

}
