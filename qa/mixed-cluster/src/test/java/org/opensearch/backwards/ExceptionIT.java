/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.backwards;

import org.apache.http.util.EntityUtils;
import org.opensearch.Version;
import org.opensearch.client.Node;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Collections;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.http.HttpStatus.SC_NOT_FOUND;

public class ExceptionIT extends OpenSearchRestTestCase {
    private Set<String> assertedWarnings = new HashSet<>();

    public void testOpensearchException() throws Exception {
        logClusterNodes();

        Request request = new Request("GET", "/no_such_index");

        String expectedWarning = "[GET /_cat/master] is deprecated! Use [GET /_cat/cluster_manager] instead.";
        request.setOptions(expectWarnings(expectedWarning));

        for (Node node : client().getNodes()) {
            try {
                client().setNodes(Collections.singletonList(node));
                logger.info("node: {}", node.getHost());
                client().performRequest(request);
                fail();
            } catch (ResponseException e) {
                logger.debug(e.getMessage());
                Response response = e.getResponse();
                assertEquals(SC_NOT_FOUND, response.getStatusLine().getStatusCode());
                assertEquals("no_such_index", ObjectPath.createFromResponse(response).evaluate("error.index"));
            }
        }
    }

    private RequestOptions expectWarnings(String expectedWarning) {
        final RequestOptions.Builder builder = RequestOptions.DEFAULT.toBuilder();
        if (!assertedWarnings.contains(expectedWarning)) {
            builder.setWarningsHandler(w -> w.contains(expectedWarning) == false || w.size() != 1);
            assertedWarnings.add(expectedWarning);
        }
        return builder.build();
    }

    private void logClusterNodes() throws IOException {
        ObjectPath objectPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "_nodes")));
        Map<String, ?> nodes = objectPath.evaluate("nodes");
        Request request = new Request("GET", "_cat/master?h=id");
        String expectedWarning = "[GET /_cat/master] is deprecated! Use [GET /_cat/cluster_manager] instead.";
        request.setOptions(expectWarnings(expectedWarning));
        String master = EntityUtils.toString(client().performRequest(request).getEntity()).trim();
        logger.info("cluster discovered: master id='{}'", master);
        for (String id : nodes.keySet()) {
            logger.info("{}: id='{}', name='{}', version={}",
                objectPath.evaluate("nodes." + id + ".http.publish_address"),
                id,
                objectPath.evaluate("nodes." + id + ".name"),
                Version.fromString(objectPath.evaluate("nodes." + id + ".version"))
            );
        }
    }
}
