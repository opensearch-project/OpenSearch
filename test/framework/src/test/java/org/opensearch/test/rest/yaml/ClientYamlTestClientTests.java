/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.test.rest.yaml;

import org.opensearch.Version;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.not;

public class ClientYamlTestClientTests extends OpenSearchTestCase {

    private static final String[] SEARCH_METHODS = new String[] { "GET", "POST", "QUERY" };

    /**
     * In a mixed-version cluster (minimum node version predates QUERY's 3.8.0 introduction) the randomizer must not send
     * QUERY, because it could reach an older node that rejects it. Guards the BWC regression.
     */
    public void testQueryIsNotRandomizedAgainstOlderClusters() {
        List<String> supported = ClientYamlTestClient.supportedMethodsForVersion(SEARCH_METHODS, Version.V_2_19_6);
        assertThat(supported, contains("GET", "POST"));
        assertThat(supported, not(contains("QUERY")));
    }

    /**
     * When every node supports QUERY (single-version current cluster) it remains eligible for randomization.
     */
    public void testQueryIsRandomizedAgainstCurrentClusters() {
        List<String> supported = ClientYamlTestClient.supportedMethodsForVersion(SEARCH_METHODS, Version.CURRENT);
        assertThat(supported, contains("GET", "POST", "QUERY"));
    }

    /**
     * Endpoints that do not advertise QUERY are unaffected regardless of cluster version.
     */
    public void testNonQueryMethodsAreUnaffected() {
        String[] methods = new String[] { "GET", "PUT", "DELETE" };
        assertThat(ClientYamlTestClient.supportedMethodsForVersion(methods, Version.V_2_19_6), contains("GET", "PUT", "DELETE"));
        assertThat(ClientYamlTestClient.supportedMethodsForVersion(methods, Version.CURRENT), contains("GET", "PUT", "DELETE"));
    }
}
