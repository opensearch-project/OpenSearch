/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.hamcrest.core.StringStartsWith;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mockito;
import org.opensearch.common.settings.Settings;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;
import java.util.Base64;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ServiceAccountManagerTest {
    private Settings setting = Settings.builder().put("http.port", "9200").build();
    private String defaultAuthString = "admin:admin";
    private Path path = Path.of("");
    private HttpClient httpClient = Mockito.mock(HttpClient.class);
    private HttpResponse<String> httpResponse = Mockito.mock(HttpResponse.class);

    @Test
    public void shouldReturnServiceWhenFound() throws IOException, InterruptedException {
        ServiceAccountManager serviceAccountManager = new ServiceAccountManager(setting, path, httpClient);
        doReturn(httpResponse).when(httpClient).send(any(), any());
        when(httpResponse.statusCode()).thenReturn(200);
        ArgumentCaptor<HttpRequest> argument = ArgumentCaptor.forClass(HttpRequest.class);

        serviceAccountManager.getOrCreateServiceAccount(
            "existingService",
            Base64.getEncoder().encodeToString(defaultAuthString.getBytes())
        );

        verify(httpClient).send(argument.capture(), any());
        verify(httpClient, times(1)).send(any(), any());
        assertThat(argument.getValue().headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        assertThat(argument.getValue().headers().firstValue("Authorization").orElse(""), StringStartsWith.startsWith("Basic "));
        assertThat(argument.getValue().method(), equalTo("GET"));
    }

    @Test
    public void shouldCreateServiceWhenNotFound() throws IOException, InterruptedException {
        ServiceAccountManager serviceAccountManager = new ServiceAccountManager(setting, path, httpClient);
        doReturn(httpResponse).when(httpClient).send(any(), any());
        when(httpResponse.statusCode()).thenReturn(404).thenReturn(200);
        ArgumentCaptor<HttpRequest> argument = ArgumentCaptor.forClass(HttpRequest.class);

        serviceAccountManager.getOrCreateServiceAccount(
            "nonExistingService",
            Base64.getEncoder().encodeToString(defaultAuthString.getBytes())
        );

        verify(httpClient, times(2)).send(argument.capture(), any());
        List<HttpRequest> httpRequests = argument.getAllValues();
        HttpRequest firstRequest = httpRequests.get(0);
        assertThat(firstRequest.headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        assertThat(firstRequest.headers().firstValue("Authorization").orElse(""), StringStartsWith.startsWith("Basic "));
        assertThat(firstRequest.method(), equalTo("GET"));

        HttpRequest secondRequest = httpRequests.get(1);
        assertThat(secondRequest.headers().firstValue("Content-Type").orElse(""), equalTo("application/json"));
        assertThat(secondRequest.headers().firstValue("Authorization").orElse(""), StringStartsWith.startsWith("Basic "));
        assertThat(secondRequest.method(), equalTo("PUT"));
        assertThat(secondRequest.bodyPublisher().map(HttpRequest.BodyPublisher::contentLength).orElse(0L), is(65L));
    }

}
