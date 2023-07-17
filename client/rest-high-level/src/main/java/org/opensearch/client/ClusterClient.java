/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.client;

import org.opensearch.action.ActionListener;
import org.opensearch.action.admin.cluster.health.ClusterHealthRequest;
import org.opensearch.action.admin.cluster.health.ClusterHealthResponse;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterGetSettingsResponse;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsRequest;
import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.cluster.RemoteInfoRequest;
import org.opensearch.client.cluster.RemoteInfoResponse;
import org.opensearch.client.indices.ComponentTemplatesExistRequest;
import org.opensearch.client.indices.DeleteComponentTemplateRequest;
import org.opensearch.client.indices.GetComponentTemplatesRequest;
import org.opensearch.client.indices.GetComponentTemplatesResponse;
import org.opensearch.client.indices.PutComponentTemplateRequest;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

import static java.util.Collections.emptySet;
import static java.util.Collections.singleton;

/**
 * A wrapper for the {@link RestHighLevelClient} that provides methods for accessing the Cluster API.
 */
public final class ClusterClient {
    private final RestHighLevelClient restHighLevelClient;

    ClusterClient(RestHighLevelClient restHighLevelClient) {
        this.restHighLevelClient = restHighLevelClient;
    }

    /**
     * Updates cluster wide specific settings using the Cluster Update Settings API.
     *
     * @param clusterUpdateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterUpdateSettingsResponse putSettings(ClusterUpdateSettingsRequest clusterUpdateSettingsRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            clusterUpdateSettingsRequest,
            ClusterRequestConverters::clusterPutSettings,
            options,
            ClusterUpdateSettingsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously updates cluster wide specific settings using the Cluster Update Settings API.
     *
     * @param clusterUpdateSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putSettingsAsync(
        ClusterUpdateSettingsRequest clusterUpdateSettingsRequest,
        RequestOptions options,
        ActionListener<ClusterUpdateSettingsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            clusterUpdateSettingsRequest,
            ClusterRequestConverters::clusterPutSettings,
            options,
            ClusterUpdateSettingsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Get the cluster wide settings using the Cluster Get Settings API.
     *
     * @param clusterGetSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterGetSettingsResponse getSettings(ClusterGetSettingsRequest clusterGetSettingsRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            clusterGetSettingsRequest,
            ClusterRequestConverters::clusterGetSettings,
            options,
            ClusterGetSettingsResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously get the cluster wide settings using the Cluster Get Settings API.
     *
     * @param clusterGetSettingsRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getSettingsAsync(
        ClusterGetSettingsRequest clusterGetSettingsRequest,
        RequestOptions options,
        ActionListener<ClusterGetSettingsResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            clusterGetSettingsRequest,
            ClusterRequestConverters::clusterGetSettings,
            options,
            ClusterGetSettingsResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Get cluster health using the Cluster Health API.
     *
     * <p>
     * If timeout occurred, {@link ClusterHealthResponse} will have isTimedOut() == true and status() == RestStatus.REQUEST_TIMEOUT
     * @param healthRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public ClusterHealthResponse health(ClusterHealthRequest healthRequest, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            healthRequest,
            ClusterRequestConverters::clusterHealth,
            options,
            ClusterHealthResponse::fromXContent,
            singleton(RestStatus.REQUEST_TIMEOUT.getStatus())
        );
    }

    /**
     * Asynchronously get cluster health using the Cluster Health API.
     *
     * If timeout occurred, {@link ClusterHealthResponse} will have isTimedOut() == true and status() == RestStatus.REQUEST_TIMEOUT
     * @param healthRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable healthAsync(
        ClusterHealthRequest healthRequest,
        RequestOptions options,
        ActionListener<ClusterHealthResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            healthRequest,
            ClusterRequestConverters::clusterHealth,
            options,
            ClusterHealthResponse::fromXContent,
            listener,
            singleton(RestStatus.REQUEST_TIMEOUT.getStatus())
        );
    }

    /**
     * Get the remote cluster information using the Remote cluster info API.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public RemoteInfoResponse remoteInfo(RemoteInfoRequest request, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            request,
            ClusterRequestConverters::remoteInfo,
            options,
            RemoteInfoResponse::fromXContent,
            singleton(RestStatus.REQUEST_TIMEOUT.getStatus())
        );
    }

    /**
     * Asynchronously get remote cluster information using the Remote cluster info API.
     *
     * @param request the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable remoteInfoAsync(RemoteInfoRequest request, RequestOptions options, ActionListener<RemoteInfoResponse> listener) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            ClusterRequestConverters::remoteInfo,
            options,
            RemoteInfoResponse::fromXContent,
            listener,
            singleton(RestStatus.REQUEST_TIMEOUT.getStatus())
        );
    }

    /**
     * Delete a component template using the Component Templates API
     *
     * @param req the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse deleteComponentTemplate(DeleteComponentTemplateRequest req, RequestOptions options) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            req,
            ClusterRequestConverters::deleteComponentTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously delete a component template using the Component Templates API
     *
     * @param request  the request
     * @param options  the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable deleteComponentTemplateAsync(
        DeleteComponentTemplateRequest request,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            request,
            ClusterRequestConverters::deleteComponentTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Puts a component template using the Component Templates API.
     *
     * @param putComponentTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public AcknowledgedResponse putComponentTemplate(PutComponentTemplateRequest putComponentTemplateRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            putComponentTemplateRequest,
            ClusterRequestConverters::putComponentTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously puts a component template using the Component Templates API.
     *
     * @param putComponentTemplateRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable putComponentTemplateAsync(
        PutComponentTemplateRequest putComponentTemplateRequest,
        RequestOptions options,
        ActionListener<AcknowledgedResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            putComponentTemplateRequest,
            ClusterRequestConverters::putComponentTemplate,
            options,
            AcknowledgedResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Gets component templates using the Components Templates API
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param getComponentTemplatesRequest the request
     * @return the response
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public GetComponentTemplatesResponse getComponentTemplate(
        GetComponentTemplatesRequest getComponentTemplatesRequest,
        RequestOptions options
    ) throws IOException {
        return restHighLevelClient.performRequestAndParseEntity(
            getComponentTemplatesRequest,
            ClusterRequestConverters::getComponentTemplates,
            options,
            GetComponentTemplatesResponse::fromXContent,
            emptySet()
        );
    }

    /**
     * Asynchronously gets component templates using the Components Templates API
     * @param getComponentTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable getComponentTemplateAsync(
        GetComponentTemplatesRequest getComponentTemplatesRequest,
        RequestOptions options,
        ActionListener<GetComponentTemplatesResponse> listener
    ) {
        return restHighLevelClient.performRequestAsyncAndParseEntity(
            getComponentTemplatesRequest,
            ClusterRequestConverters::getComponentTemplates,
            options,
            GetComponentTemplatesResponse::fromXContent,
            listener,
            emptySet()
        );
    }

    /**
     * Uses the Component Templates API to determine if component templates exist
     *
     * @param componentTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @return true if any index templates in the request exist, false otherwise
     * @throws IOException in case there is a problem sending the request or parsing back the response
     */
    public boolean existsComponentTemplate(ComponentTemplatesExistRequest componentTemplatesRequest, RequestOptions options)
        throws IOException {
        return restHighLevelClient.performRequest(
            componentTemplatesRequest,
            ClusterRequestConverters::componentTemplatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            emptySet()
        );
    }

    /**
     * Uses the Index Templates API to determine if index templates exist
     * @param componentTemplatesRequest the request
     * @param options the request options (e.g. headers), use {@link RequestOptions#DEFAULT} if nothing needs to be customized
     * @param listener the listener to be notified upon request completion. The listener will be called with the value {@code true}
     * @return cancellable that may be used to cancel the request
     */
    public Cancellable existsComponentTemplateAsync(
        ComponentTemplatesExistRequest componentTemplatesRequest,
        RequestOptions options,
        ActionListener<Boolean> listener
    ) {

        return restHighLevelClient.performRequestAsync(
            componentTemplatesRequest,
            ClusterRequestConverters::componentTemplatesExist,
            options,
            RestHighLevelClient::convertExistsResponse,
            listener,
            emptySet()
        );
    }
}
