/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dashboards;

import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

public class AdvancedSettingsIT extends OpenSearchRestTestCase {

    private static final String INDEX_NAME = ".opensearch_dashboards";
    private static final String DOC_ID = "config:3.7.0";

    public void testWriteAndGetAdvancedSettings() throws IOException, ParseException {
        // Write settings via the advanced settings API
        Request writeRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        writeRequest.setJsonEntity("{\"theme:darkMode\":true,\"dateFormat\":\"YYYY-MM-DD\"}");
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), is(200));

        String writeBody = EntityUtils.toString(writeResponse.getEntity());
        assertThat(writeBody, containsString("theme:darkMode"));
        assertThat(writeBody, containsString("dateFormat"));

        // Refresh to make sure the document is searchable
        Request refreshRequest = new Request("GET", "/_opensearch_dashboards/" + INDEX_NAME + "/_refresh");
        client().performRequest(refreshRequest);

        // Get settings via the advanced settings API
        Request getRequest = new Request("GET", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));

        String getBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(getBody, containsString("theme:darkMode"));
        assertThat(getBody, containsString("YYYY-MM-DD"));
    }

    public void testUpdateAdvancedSettings() throws IOException, ParseException {
        // Create initial settings
        Request writeRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        writeRequest.setJsonEntity("{\"theme:darkMode\":false,\"dateFormat\":\"YYYY-MM-DD\"}");
        client().performRequest(writeRequest);

        // Update settings
        Request updateRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        updateRequest.setJsonEntity("{\"theme:darkMode\":true,\"dateFormat\":\"DD/MM/YYYY\"}");
        Response updateResponse = client().performRequest(updateRequest);
        assertThat(updateResponse.getStatusLine().getStatusCode(), is(200));

        // Refresh
        Request refreshRequest = new Request("GET", "/_opensearch_dashboards/" + INDEX_NAME + "/_refresh");
        client().performRequest(refreshRequest);

        // Verify updated settings
        Request getRequest = new Request("GET", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));

        String getBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(getBody, containsString("\"theme:darkMode\":true"));
        assertThat(getBody, containsString("DD/MM/YYYY"));
    }

    public void testGetNonExistentSettingsReturns404() throws IOException {
        ResponseException exception = expectThrows(ResponseException.class, () -> {
            Request getRequest = new Request("GET", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/config:nonexistent");
            client().performRequest(getRequest);
        });
        assertThat(exception.getResponse().getStatusLine().getStatusCode(), is(404));
    }

    public void testCreateOperationType() throws IOException, ParseException {
        // Write with CREATE operation
        Request writeRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        writeRequest.addParameter("operation", "CREATE");
        writeRequest.setJsonEntity("{\"theme:darkMode\":true}");
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), is(200));

        // Refresh
        Request refreshRequest = new Request("GET", "/_opensearch_dashboards/" + INDEX_NAME + "/_refresh");
        client().performRequest(refreshRequest);

        // Verify settings were created
        Request getRequest = new Request("GET", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));

        String getBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(getBody, containsString("theme:darkMode"));
    }

    public void testWriteWithEmptyBody() throws IOException, ParseException {
        Request writeRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + INDEX_NAME + "/" + DOC_ID);
        writeRequest.setJsonEntity("{}");
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), is(200));
    }

    public void testWriteAndGetWithDifferentIndex() throws IOException, ParseException {
        String altIndex = ".opensearch_dashboards_1";

        // Write to alternative index
        Request writeRequest = new Request("PUT", "/_opensearch_dashboards/advanced_settings/" + altIndex + "/" + DOC_ID);
        writeRequest.setJsonEntity("{\"notifications:banner\":\"Welcome to 3.7\"}");
        Response writeResponse = client().performRequest(writeRequest);
        assertThat(writeResponse.getStatusLine().getStatusCode(), is(200));

        // Refresh
        Request refreshRequest = new Request("GET", "/_opensearch_dashboards/" + altIndex + "/_refresh");
        client().performRequest(refreshRequest);

        // Get from alternative index
        Request getRequest = new Request("GET", "/_opensearch_dashboards/advanced_settings/" + altIndex + "/" + DOC_ID);
        Response getResponse = client().performRequest(getRequest);
        assertThat(getResponse.getStatusLine().getStatusCode(), is(200));

        String getBody = EntityUtils.toString(getResponse.getEntity());
        assertThat(getBody, containsString("notifications:banner"));
        assertThat(getBody, containsString("Welcome to 3.7"));
    }
}
