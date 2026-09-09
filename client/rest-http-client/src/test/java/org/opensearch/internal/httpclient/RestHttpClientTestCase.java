/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.internal.httpclient;

import com.carrotsearch.randomizedtesting.JUnit3MethodProvider;
import com.carrotsearch.randomizedtesting.MixWithSuiteName;
import com.carrotsearch.randomizedtesting.RandomizedTest;
import com.carrotsearch.randomizedtesting.annotations.SeedDecorators;
import com.carrotsearch.randomizedtesting.annotations.TestMethodProviders;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakAction;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakGroup;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakLingering;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakZombies;
import com.carrotsearch.randomizedtesting.annotations.TimeoutSuite;

import java.net.http.HttpHeaders;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

@TestMethodProviders({ JUnit3MethodProvider.class })
@SeedDecorators({ MixWithSuiteName.class }) // See LUCENE-3995 for rationale.
@ThreadLeakScope(ThreadLeakScope.Scope.SUITE)
@ThreadLeakGroup(ThreadLeakGroup.Group.MAIN)
@ThreadLeakAction({ ThreadLeakAction.Action.WARN, ThreadLeakAction.Action.INTERRUPT })
@ThreadLeakZombies(ThreadLeakZombies.Consequence.IGNORE_REMAINING_TESTS)
@ThreadLeakLingering(linger = 5000) // 5 sec lingering
@ThreadLeakFilters(filters = { HttpClientThreadLeakFilter.class, BouncyCastleThreadFilter.class })
@TimeoutSuite(millis = 2 * 60 * 60 * 1000)
public abstract class RestHttpClientTestCase extends RandomizedTest {
    /**
     * Assert that the actual headers are the expected ones given the original default and request headers. Some headers can be ignored,
     * for instance in case the http client is adding its own automatically.
     *
     * @param defaultHeaders the default headers set to the REST client instance
     * @param requestHeaders the request headers sent with a particular request
     * @param actualHeaders the actual headers as a result of the provided default and request headers
     * @param ignoreHeaders header keys to be ignored as they are not part of default nor request headers, yet they
     *                      will be part of the actual ones
     */
    protected static void assertHeaders(
        final Map<String, List<String>> defaultHeaders,
        final Map<String, List<String>> requestHeaders,
        final HttpHeaders actualHeaders,
        final Set<String> ignoreHeaders
    ) {
        final Map<String, List<String>> expectedHeaders = new TreeMap<>(String.CASE_INSENSITIVE_ORDER);
        final Set<String> requestHeaderKeys = new HashSet<>();
        for (final Map.Entry<String, List<String>> header : requestHeaders.entrySet()) {
            final String name = header.getKey();
            addValueToListEntry(expectedHeaders, name, header.getValue());
            requestHeaderKeys.add(name);
        }
        for (final Map.Entry<String, List<String>> defaultHeader : defaultHeaders.entrySet()) {
            final String name = defaultHeader.getKey();
            if (requestHeaderKeys.contains(name) == false) {
                addValueToListEntry(expectedHeaders, name, defaultHeader.getValue());
            }
        }
        Set<String> actualIgnoredHeaders = new TreeSet<>(String.CASE_INSENSITIVE_ORDER);
        for (final Map.Entry<String, List<String>> responseHeader : actualHeaders.map().entrySet()) {
            final String name = responseHeader.getKey();
            if (ignoreHeaders.contains(name)) {
                expectedHeaders.remove(name);
                actualIgnoredHeaders.add(name);
                continue;
            }
            final String value = responseHeader.getValue().getFirst();
            final List<String> values = expectedHeaders.get(name);
            assertNotNull("found response header [" + name + "] that wasn't originally sent: " + value, values);
            assertTrue("found incorrect response header [" + name + "]: " + value, values.remove(value));
            if (values.isEmpty()) {
                expectedHeaders.remove(name);
            }
        }
        assertEquals("some headers meant to be ignored were not part of the actual headers", ignoreHeaders, actualIgnoredHeaders);
        assertTrue("some headers that were sent weren't returned " + expectedHeaders, expectedHeaders.isEmpty());
    }

    private static void addValueToListEntry(final Map<String, List<String>> map, final String name, final List<String> values) {
        map.computeIfAbsent(name, k -> new ArrayList<>()).addAll(values);
    }
}
