/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.hamcrest.MatcherAssert;
import org.hamcrest.core.StringStartsWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountManagerTests extends OpenSearchTestCase {
    private Settings setting = Settings.builder().put("http.port", "9200").build();
    private String defaultAuthString = "admin:admin";
    private HttpClient httpClient = Mockito.mock(HttpClient.class);
    @SuppressWarnings("unchecked")
    private HttpResponse<String> httpResponse = Mockito.mock(HttpResponse.class);

    public void testShouldReturnServiceWhenFound() throws IOException, InterruptedException {
        ServiceAccountManager serviceAccountManager = new ServiceAccountManager(setting, httpClient);
        doReturn(httpResponse).when(httpClient).send(any(), any());
        when(httpResponse.statusCode()).thenReturn(200);
        ArgumentCaptor<HttpRequest> argument = ArgumentCaptor.forClass(HttpRequest.class);

        serviceAccountManager.getOrCreateServiceAccount(
            "existingService",
            Base64.getEncoder().encodeToString(defaultAuthString.getBytes(StandardCharsets.UTF_8))
        );

        verify(httpClient).send(argument.capture(), any());
        verify(httpClient, times(1)).send(any(), any());
        MatcherAssert.assertThat(argument.getValue().headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        MatcherAssert.assertThat(
            argument.getValue().headers().firstValue("Authorization").orElse(""),
            StringStartsWith.startsWith("Basic ")
        );
        MatcherAssert.assertThat(argument.getValue().method(), equalTo("GET"));
    }

    public void testShouldCreateServiceWhenNotFound() throws IOException, InterruptedException {
        ServiceAccountManager serviceAccountManager = new ServiceAccountManager(setting, httpClient);
        doReturn(httpResponse).when(httpClient).send(any(), any());
        when(httpResponse.statusCode()).thenReturn(404).thenReturn(200);
        ArgumentCaptor<HttpRequest> argument = ArgumentCaptor.forClass(HttpRequest.class);

        serviceAccountManager.getOrCreateServiceAccount(
            "nonExistingService",
            Base64.getEncoder().encodeToString(defaultAuthString.getBytes(StandardCharsets.UTF_8))
        );

        verify(httpClient, times(2)).send(argument.capture(), any());
        List<HttpRequest> httpRequests = argument.getAllValues();
        HttpRequest firstRequest = httpRequests.get(0);
        MatcherAssert.assertThat(firstRequest.headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        MatcherAssert.assertThat(firstRequest.headers().firstValue("Authorization").orElse(""), StringStartsWith.startsWith("Basic "));
        MatcherAssert.assertThat(firstRequest.method(), equalTo("GET"));

        HttpRequest secondRequest = httpRequests.get(1);
        MatcherAssert.assertThat(secondRequest.headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        MatcherAssert.assertThat(secondRequest.headers().firstValue("Authorization").orElse(""), StringStartsWith.startsWith("Basic "));
        MatcherAssert.assertThat(secondRequest.method(), equalTo("PUT"));
        MatcherAssert.assertThat(secondRequest.bodyPublisher().map(HttpRequest.BodyPublisher::contentLength).orElse(0L), is(65L));
    }
}
