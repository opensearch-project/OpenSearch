/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity.remotecluster;

import org.junit.Before;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.WarningsHandler;
import org.opensearch.identity.IdentityConfigConstants;
import org.opensearch.identity.rest.IdentityRestConstants;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * Wrapper for remote cluster tests in identity
 */
public class IdentityRestTestCase extends OpenSearchRestTestCase {
    private final String identityIndex = IdentityConfigConstants.IDENTITY_DEFAULT_CONFIG_INDEX;
    public final String ENDPOINT;

    protected String getEndpointPrefix() {
        return IdentityRestConstants.IDENTITY_REST_REQUEST_PREFIX;
    }

    public IdentityRestTestCase() {
        ENDPOINT = getEndpointPrefix() + "/api";
    }

    @Before
    public void init() throws Exception {
        ensureIdentityIndexExists();
    }

    @Override
    protected boolean preserveIndicesUponCompletion() {
        return true; // setting true so identity index is not deleted upon test completion
    }

    /**
     * This warning is expected to be thrown as we are accessing identity index directly
     * @return the warning message to be expected
     */
    public RequestOptions options() {

        RequestOptions.Builder options = RequestOptions.DEFAULT.toBuilder();
        options.addHeader("Authorization", "Basic YWRtaW46YWRtaW4="); // admin:admin;
        options.setWarningsHandler(new NoopWarningsHandler()); // otherwise request fails with warnings about accessing system index
        return options.build();
    }

    private static class NoopWarningsHandler implements WarningsHandler {
        public NoopWarningsHandler() {}

        @Override
        public boolean warningsShouldFailRequest(List<String> warnings) {
            return false;
        }
    }

    protected void ensureIdentityIndexExists() throws IOException {
        // this will fail if default index name is changed in remote cluster
        Request request = new Request("GET", "/" + identityIndex);
        request.setOptions(options());
        Response response = adminClient().performRequest(request);
        assertEquals(response.getStatusLine().getStatusCode(), 200);
        Map<String, Object> responseAsMap = entityAsMap(response);
        assertTrue(responseAsMap.containsKey(identityIndex));
    }
}
