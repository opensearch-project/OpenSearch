/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;


import com.fasterxml.jackson.databind.node.ObjectNode;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import org.opensearch.identity.tokens.AuthToken;
import org.opensearch.identity.tokens.BasicAuthToken;
import org.yaml.snakeyaml.external.biz.base64Coder.Base64Coder;

import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static org.opensearch.rest.RestStatus.NOT_FOUND;

public class RestServiceAccountManager implements ServiceAccountManager {

    private Settings settings;
    private final String url;
    public HttpClient httpClient;
    @Override
    public AuthToken resetServiceAccountToken(String principal) {

        String responseBody = null;
        try {
            responseBody = getOrCreateServiceAccount(new NamedPrincipal(principal).getName(),"Basic YWRtaW46YWRtaW4=");
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        BasicAuthToken authToken = new BasicAuthToken(responseBody);
        return authToken;
    }

    @Override
    public Boolean isValidToken(AuthToken token) {
        if (!(token instanceof BasicAuthToken)) {
            throw new RuntimeException("The Rest Service Account Manager cannot parse this type of token.");
        }
        // Take the auth token details
        BasicAuthToken baseToken = (BasicAuthToken) token;
        String password = baseToken.getPassword();
        String username = baseToken.getUser();

        // Make an auth string from it
        String authString = Base64Coder.encodeString(username + ":" + password);

        // Use for GET Request and verify token based on response
        int getResponse;
        try {
            getResponse = getRequestStatus(username, authString);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        if (getResponse == 200) { // Request succeeded
            return true;
        }
        return false;
    }

    @Override
    public void updateServiceAccount(ObjectNode contentAsNode) {

    }

    public RestServiceAccountManager(Settings settings) {
        this.settings = settings;
        this.url = buildUrl(settings);
        this.httpClient = HttpClient.newHttpClient();
    }

    public RestServiceAccountManager(Settings settings, HttpClient httpClient) {
        this.settings = settings;
        this.url = buildUrl(settings);
        this.httpClient = httpClient;
    }

    public String getOrCreateServiceAccount(String extensionId, String authenticationString) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + extensionId))
            .GET()
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + authenticationString)
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND.getStatus()) {
            request = HttpRequest.newBuilder(URI.create(url + extensionId))
                .PUT(ofString(createServiceAccountRequest(extensionId)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + authenticationString)
                .build();
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

        return response.body();
    }

    public int getRequestStatus(String extensionId, String authenticationString) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + extensionId))
            .GET()
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + authenticationString)
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND.getStatus()) {
            request = HttpRequest.newBuilder(URI.create(url + extensionId))
                .PUT(ofString(createServiceAccountRequest(extensionId)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + authenticationString)
                .build();
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

        return response.statusCode();
    }

    private String createServiceAccountRequest(String extensionId) {
        try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
            xContentBuilder.startObject();
            xContentBuilder.field("name", extensionId);
            xContentBuilder.field("attributes");
            xContentBuilder.startObject();
            xContentBuilder.field("isService", "true");
            xContentBuilder.field("isEnabled", "false");
            xContentBuilder.endObject().endObject();
            xContentBuilder.generator().flush();
            return xContentBuilder.getOutputStream().toString();
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static String buildUrl(Settings settings) {
        int port = settings.getAsInt("http.port", 9200);
        String host = settings.get("http.host", "localhost");
        return "https://" + host + ":" + port + "/_plugins/_security/api/internalusers/";
    }
}
