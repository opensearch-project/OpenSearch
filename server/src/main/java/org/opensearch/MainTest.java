/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class MainTest {
    public static void main(String[] args) throws IOException {
        Map<String, String> mp = new HashMap<>();
        mp.compute(getKey(), (k, v) -> (v == null) ? "0" : "test");
    }

    private static String getKey() throws IOException {
        throw new IOException("Test Exception");
    }
}
