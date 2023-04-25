/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.identity;

import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.file.Path;

import static java.net.http.HttpRequest.BodyPublishers.ofString;
import static org.opensearch.rest.RestStatus.NOT_FOUND;

/**
 * ServiceAccountManager hooks into security plugin endpoint
 */
public class ServiceAccountManager {

    private final String url;
    private Settings settings;
    private Path extensionsPath;

    public ServiceAccountManager(Settings settings, Path extensionsPath) {
        this.settings = settings;
        this.extensionsPath = extensionsPath;
        this.url = buildUrl(settings);
    }

    public String getOrCreateServiceAccount(String extensionId, String authenticationString) throws IOException, InterruptedException {
        HttpClient client = HttpClient.newHttpClient();

        HttpRequest request = HttpRequest.newBuilder(URI.create(url + extensionId))
            .GET()
            .header("Content-Type", "application/json")
            .header("Authorization", "Basic " + authenticationString)
            .build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        if (response.statusCode() == NOT_FOUND.getStatus()) {
            request = HttpRequest.newBuilder(URI.create(url + extensionId))
                .PUT(ofString(createServiceAccountRequest(extensionId)))
                .header("Content-Type", "application/json")
                .header("Authorization", "Basic " + authenticationString)
                .build();
            response = client.send(request, HttpResponse.BodyHandlers.ofString());
        }

        return response.body();
    }

    private String createServiceAccountRequest(String extensionId) {
        try (XContentBuilder xContentBuilder = XContentFactory.contentBuilder(XContentType.JSON)) {
            xContentBuilder.startObject();
            xContentBuilder.field("attributes");
            xContentBuilder.startObject();
            xContentBuilder.field("owner", extensionId);
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
