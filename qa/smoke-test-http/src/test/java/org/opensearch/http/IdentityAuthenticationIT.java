/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import org.junit.Assert;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.identity.shiro.ShiroIdentityPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.core.rest.RestStatus;

import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.transport.Netty4ModulePlugin;
import org.opensearch.transport.nio.NioTransportPlugin;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.core.StringContains.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, supportsDedicatedMasters = false, numDataNodes = 1)
public class IdentityAuthenticationIT extends HttpSmokeTestCase {

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.IDENTITY, "true")
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(OpenSearchTestCase.getTestTransportPlugin(), Netty4ModulePlugin.class, NioTransportPlugin.class, ShiroIdentityPlugin.class);
    }


    public void testBasicAuthSuccess() throws Exception {
        final Response response = createHealthRequest("Basic YWRtaW46YWRtaW4="); // admin:admin
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        Assert.assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.OK.getStatus()));
        Assert.assertThat(content, containsString("green"));
    }

    public void testBasicAuthUnauthorized_invalidHeader() throws Exception {
        final Response response = createHealthRequest("Basic aaaa"); // invalid username password
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        Assert.assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
        Assert.assertThat(content, containsString("Illegally formed basic authorization header"));
    }

    public void testBasicAuthUnauthorized_wrongPassword() throws Exception {
        final Response response = createHealthRequest("Basic YWRtaW46aW52YWxpZFBhc3N3b3Jk"); // admin:invalidPassword
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        Assert.assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
    }

    public void testBasicAuthUnauthorized_unknownUser() throws Exception {
        final Response response = createHealthRequest("Basic dXNlcjpkb2VzTm90RXhpc3Q="); // user:doesNotExist
        final String content = new String(response.getEntity().getContent().readAllBytes(), StandardCharsets.UTF_8);

        Assert.assertThat(content, response.getStatusLine().getStatusCode(), equalTo(RestStatus.UNAUTHORIZED.getStatus()));
    }

    private Response createHealthRequest(final String authorizationHeaderValue) throws Exception {
        final Request request = new Request("GET", "/_cluster/health");
        final RequestOptions options = RequestOptions.DEFAULT.toBuilder().addHeader("Authorization", authorizationHeaderValue).build();
        request.setOptions(options);

        try {
            final Response response = OpenSearchIntegTestCase.getRestClient().performRequest(request);
            return response;
        } catch (final ResponseException re) {
            return re.getResponse();
        }
    }
}
