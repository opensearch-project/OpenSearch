/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.rest.action.admin.cluster;

import org.opensearch.action.admin.cluster.deployment.TransitionDeploymentAction;
import org.opensearch.action.admin.cluster.deployment.TransitionDeploymentRequest;
import org.opensearch.cluster.deployment.DeploymentState;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.rest.BaseRestHandler;
import org.opensearch.rest.RestRequest;
import org.opensearch.rest.action.RestToXContentListener;
import org.opensearch.transport.client.node.NodeClient;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static org.opensearch.rest.RestRequest.Method.POST;

/**
 * REST action for transitioning a deployment (drain/finish).
 *
 * @opensearch.internal
 */
public class RestTransitionDeploymentAction extends BaseRestHandler {

    @Override
    public List<Route> routes() {
        return Arrays.asList(new Route(POST, "/_cluster/deployment/{deployment_id}/{action}"));
    }

    @Override
    public String getName() {
        return "transition_deployment_action";
    }

    @Override
    protected RestChannelConsumer prepareRequest(RestRequest request, NodeClient client) throws IOException {
        String deploymentId = request.param("deployment_id");
        String action = request.param("action");

        TransitionDeploymentRequest transitionRequest = new TransitionDeploymentRequest();
        transitionRequest.deploymentId(deploymentId);
        DeploymentState targetState = DeploymentState.valueOf(action.toUpperCase(Locale.ROOT));
        transitionRequest.targetState(targetState);
        if (targetState == DeploymentState.DRAIN) {
            Map<String, String> attributes = parseAttributes(request);
            transitionRequest.attributes(attributes);
        }

        return channel -> client.execute(TransitionDeploymentAction.INSTANCE, transitionRequest, new RestToXContentListener<>(channel));
    }

    private static String extractAction(String path) {
        int lastSlash = path.lastIndexOf('/');
        if (lastSlash >= 0) {
            return path.substring(lastSlash + 1);
        }
        return path;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, String> parseAttributes(RestRequest request) throws IOException {
        if (!request.hasContent()) {
            return Collections.emptyMap();
        }
        try (XContentParser parser = request.contentParser()) {
            Map<String, Object> body = parser.map();
            Object attrs = body.get("attributes");
            if (attrs instanceof Map) {
                Map<String, Object> rawAttrs = (Map<String, Object>) attrs;
                Map<String, String> result = new java.util.HashMap<>();
                for (Map.Entry<String, Object> entry : rawAttrs.entrySet()) {
                    result.put(entry.getKey(), entry.getValue().toString());
                }
                return result;
            }
            return Collections.emptyMap();
        }
    }
}
