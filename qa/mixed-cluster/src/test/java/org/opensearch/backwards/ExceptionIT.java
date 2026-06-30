/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.backwards;

import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.ParseException;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.opensearch.Version;
import org.opensearch.client.Node;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.test.rest.OpenSearchRestTestCase;
import org.opensearch.test.rest.yaml.ObjectPath;

import java.io.IOException;
import java.util.Collections;
import java.util.Map;

public class ExceptionIT extends OpenSearchRestTestCase {
    public void testOpensearchException() throws Exception {
        logClusterNodes();

        Request request = new Request("GET", "/no_such_index");

        for (Node node : client().getNodes()) {
            try {
                client().setNodes(Collections.singletonList(node));
                logger.info("node: {}", node.getHost());
                client().performRequest(request);
                fail();
            } catch (ResponseException e) {
                logger.debug(e.getMessage());
                Response response = e.getResponse();
                assertEquals(HttpStatus.SC_NOT_FOUND, response.getStatusLine().getStatusCode());
                assertEquals("no_such_index", ObjectPath.createFromResponse(response).evaluate("error.index"));
            }
        }
    }

    private void logClusterNodes() throws IOException, ParseException {
        ObjectPath objectPath = ObjectPath.createFromResponse(client().performRequest(new Request("GET", "_nodes")));
        Map<String, ?> nodes = objectPath.evaluate("nodes");
        // As of 2.0, 'GET _cat/master' API is deprecated to promote inclusive language.
        // Allow the deprecation warning for the node running an older version.
        // TODO: Replace the API with 'GET _cat/cluster_manager' when dropping compatibility with 1.x versions.
        Request catRequest = new Request("GET", "_cat/master?h=id");
        catRequest.setOptions(expectWarningsOnce("[GET /_cat/master] is deprecated! Use [GET /_cat/cluster_manager] instead."));
        String clusterManager = EntityUtils.toString(client().performRequest(catRequest).getEntity()).trim();
        logger.info("cluster discovered: cluster-manager id='{}'", clusterManager);
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
