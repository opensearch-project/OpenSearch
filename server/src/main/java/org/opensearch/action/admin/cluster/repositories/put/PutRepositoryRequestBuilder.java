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

package org.opensearch.action.admin.cluster.repositories.put;

import org.opensearch.action.admin.cluster.crypto.CryptoSettings;
import org.opensearch.action.support.master.AcknowledgedRequestBuilder;
import org.opensearch.action.support.master.AcknowledgedResponse;
import org.opensearch.client.OpenSearchClient;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentType;

import java.util.Map;

/**
 * Register repository request builder
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutRepositoryRequestBuilder extends AcknowledgedRequestBuilder<
    PutRepositoryRequest,
    AcknowledgedResponse,
    PutRepositoryRequestBuilder> {

    /**
     * Constructs register repository request
     */
    public PutRepositoryRequestBuilder(OpenSearchClient client, PutRepositoryAction action) {
        super(client, action, new PutRepositoryRequest());
    }

    /**
     * Constructs register repository request for the repository with a given name
     */
    public PutRepositoryRequestBuilder(OpenSearchClient client, PutRepositoryAction action, String name) {
        super(client, action, new PutRepositoryRequest(name));
    }

    /**
     * Sets the repository name
     *
     * @param name repository name
     * @return this builder
     */
    public PutRepositoryRequestBuilder setName(String name) {
        request.name(name);
        return this;
    }

    /**
     * Sets the repository type
     *
     * @param type repository type
     * @return this builder
     */
    public PutRepositoryRequestBuilder setType(String type) {
        request.type(type);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings builder
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Settings.Builder settings) {
        request.settings(settings);
        return this;
    }

    /**
     * Sets the repository settings in Json or Yaml format
     *
     * @param source repository settings
     * @param xContentType the content type of the source
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(String source, XContentType xContentType) {
        request.settings(source, xContentType);
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param source repository settings
     * @return this builder
     */
    public PutRepositoryRequestBuilder setSettings(Map<String, Object> source) {
        request.settings(source);
        return this;
    }

    /**
     * Sets whether or not repository should be verified after creation
     *
     * @param verify true if repository should be verified after registration, false otherwise
     * @return this builder
     */
    public PutRepositoryRequestBuilder setVerify(boolean verify) {
        request.verify(verify);
        return this;
    }

    /**
     * Sets the repository encryption settings
     *
     * @param cryptoSettings repository crypto settings builder
     * @return this builder
     */
    public PutRepositoryRequestBuilder setEncryptionSettings(CryptoSettings cryptoSettings) {
        request.cryptoSettings(cryptoSettings);
        return this;
    }
}
