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

/**
 * A Service Account Manager that operates over the REST layer.
 *
 */
public class RestServiceAccountManager implements ServiceAccountManager {

    private Settings settings;
    private final String url;
    public HttpClient httpClient;

    /**
     * Construct a RestServiceAccountManager only by specifying settings
     * @param settings The settings used to construct the manager
     */
    public RestServiceAccountManager(Settings settings) {
        this.settings = settings;
        this.url = buildUrl(settings);
        this.httpClient = HttpClient.newHttpClient();
    }

    /**
     * Construct a RestServiceAccountManager from settings and an HttpClient
     * @param settings Settings used to configure the manager
     * @param httpClient The HttpClient used in the manager
     */
    public RestServiceAccountManager(Settings settings, HttpClient httpClient) {
        this.settings = settings;
        this.url = buildUrl(settings);
        this.httpClient = httpClient;
    }

    /**
     * Resets a service account token by resetting the password and creating a new basic auth header which is turned into a new Auth Token
     * @param serviceName The name associated with the service
     * @return A new auth token
     */
    @Override
    public AuthToken resetServiceAccountToken(String serviceName) {

        String responseBody = null;
        try {
            HttpRequest request = HttpRequest.newBuilder(URI.create(url + serviceName))
                .GET()
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + "YWRtaW46YWRtaW4=")
                .build();

            HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() == NOT_FOUND.getStatus()) {
                request = HttpRequest.newBuilder(URI.create(url + serviceName + "/authtoken"))
                    .POST(ofString(createServiceAccountRequest(serviceName)))
                    .header("Content-Type", "application/json")
                    .header("Authorization", "Basic " + "YWRtaW46YWRtaW4=")
                    .build();
                response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
            }
            responseBody = response.body();
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }

        BasicAuthToken authToken = new BasicAuthToken(responseBody);

        return authToken;
    }

    /**
     * Checks whether a given AuthToken is valid by executing a get request on the principal associated with the token and using the token as the authentication for the request
     * @param token The AuthToken to check
     * @return A boolean stating whether the token is valid
     */
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

    /**
     * Update a service account by passing in a objectNode with new information
     *
     * @param contentAsNode An ObjectNode with the new account details
     */
    @Override
    public void updateServiceAccount(ObjectNode contentAsNode) {}

    /**
     * Get or create a new service account
     * @param serviceName The name used for referencing the service
     * @param authenticationString An authentication string used to authorize the REST request
     * @return A string of the response body--this will be a new auth header for the account
     * @throws IOException
     * @throws InterruptedException
     */
    public String getOrCreateServiceAccount(String serviceName, String authenticationString) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + serviceName))
            .GET()
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + authenticationString)
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND.getStatus()) {
            request = HttpRequest.newBuilder(URI.create(url + serviceName))
                .PUT(ofString(createServiceAccountRequest(serviceName)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + authenticationString)
                .build();
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

        return response.body();
    }

    /**
     *
     * Calls a Get Request and returns the response code
     *
     * @param serviceName The name associated with the service in question
     * @param authenticationString
     * @return An int status code from the request
     * @throws IOException If connection fails
     * @throws InterruptedException If connection interrupted
     */
    public int getRequestStatus(String serviceName, String authenticationString) throws IOException, InterruptedException {
        HttpRequest request = HttpRequest.newBuilder(URI.create(url + serviceName))
            .GET()
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + authenticationString)
            .build();

        HttpResponse<String> response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND.getStatus()) {
            request = HttpRequest.newBuilder(URI.create(url + serviceName))
                .PUT(ofString(createServiceAccountRequest(serviceName)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + authenticationString)
                .build();
            response = httpClient.send(request, HttpResponse.BodyHandlers.ofString());
        }

        return response.statusCode();
    }

    /**
     *
     * Creates a template for a basic service account request
     * @param serviceName A name associated with the service
     * @return A string representing the base request body
     */
    private String createServiceAccountRequest(String serviceName) {
        try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
            xContentBuilder.startObject();
            xContentBuilder.field("name", serviceName);
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

    /**
     * Creates a URL with the route required to interact with the default security plugin implementation
     * @param settings Settings be configured
     * @return A string representing the route to the account APIs
     */
    private static String buildUrl(Settings settings) {
        int port = settings.getAsInt("http.port", 9200);
        String host = settings.get("http.host", "localhost");
        return "https://" + host + ":" + port + "/_plugins/_security/api/internalusers/";
    }
}
