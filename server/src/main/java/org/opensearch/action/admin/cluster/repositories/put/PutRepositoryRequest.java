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

import org.opensearch.Version;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.admin.cluster.crypto.CryptoSettings;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaType;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.action.ValidateActions.addValidationError;
import static org.opensearch.common.settings.Settings.Builder.EMPTY_SETTINGS;
import static org.opensearch.common.settings.Settings.readSettingsFromStream;
import static org.opensearch.common.settings.Settings.writeSettingsToStream;

/**
 * Register repository request.
 * <p>
 * Registers a repository with given name, type and settings. If the repository with the same name already
 * exists in the cluster, the new repository will replace the existing repository.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutRepositoryRequest extends AcknowledgedRequest<PutRepositoryRequest> implements ToXContentObject {

    private String name;

    private String type;

    private boolean verify = true;

    private Settings settings = EMPTY_SETTINGS;

    private CryptoSettings cryptoSettings;

    public PutRepositoryRequest(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        type = in.readString();
        settings = readSettingsFromStream(in);
        verify = in.readBoolean();

        if (in.getVersion().onOrAfter(Version.V_2_10_0)) {
            cryptoSettings = in.readOptionalWriteable(CryptoSettings::new);
        }
    }

    public PutRepositoryRequest() {}

    /**
     * Constructs a new put repository request with the provided name.
     */
    public PutRepositoryRequest(String name) {
        this.name = name;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (name == null) {
            validationException = addValidationError("name is missing", validationException);
        }
        if (type == null) {
            validationException = addValidationError("type is missing", validationException);
        }
        if (cryptoSettings != null) {
            validationException = cryptoSettings.validate();
        }
        return validationException;
    }

    /**
     * Sets the name of the repository.
     *
     * @param name repository name
     */
    public PutRepositoryRequest name(String name) {
        this.name = name;
        return this;
    }

    /**
     * The name of the repository.
     *
     * @return repository name
     */
    public String name() {
        return this.name;
    }

    /**
     * The type of the repository
     * <ul>
     * <li>"fs" - shared filesystem repository</li>
     * </ul>
     *
     * @param type repository type
     * @return this request
     */
    public PutRepositoryRequest type(String type) {
        this.type = type;
        return this;
    }

    /**
     * Returns repository type
     *
     * @return repository type
     */
    public String type() {
        return this.type;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings settings) {
        this.settings = settings;
        return this;
    }

    /**
     * Sets the repository settings
     *
     * @param settings repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Settings.Builder settings) {
        this.settings = settings.build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings in json or yaml format
     * @param mediaType the content type of the source
     * @return this request
     */
    public PutRepositoryRequest settings(String source, final MediaType mediaType) {
        this.settings = Settings.builder().loadFromSource(source, mediaType).build();
        return this;
    }

    /**
     * Sets the repository settings.
     *
     * @param source repository settings
     * @return this request
     */
    public PutRepositoryRequest settings(Map<String, Object> source) {
        this.settings = Settings.builder().loadFromMap(source).build();
        return this;
    }

    /**
     * Returns repository settings
     *
     * @return repository settings
     */
    public Settings settings() {
        return this.settings;
    }

    /**
     * Sets whether or not the repository should be verified after creation
     */
    public PutRepositoryRequest verify(boolean verify) {
        this.verify = verify;
        return this;
    }

    /**
     * Returns true if repository should be verified after creation
     */
    public boolean verify() {
        return this.verify;
    }

    /**
     * Sets the repository crypto settings
     *
     * @param cryptoSettings repository crypto settings
     * @return this request
     */
    public PutRepositoryRequest cryptoSettings(CryptoSettings cryptoSettings) {
        this.cryptoSettings = cryptoSettings;
        return this;
    }

    /**
     * Returns repository encryption settings
     *
     * @return repository encryption settings
     */
    public CryptoSettings cryptoSettings() {
        return cryptoSettings;
    }

    /**
     * Parses repository definition.
     *
     * @param repositoryDefinition repository definition
     */
    public PutRepositoryRequest source(Map<String, Object> repositoryDefinition) {
        for (Map.Entry<String, Object> entry : repositoryDefinition.entrySet()) {
            String name = entry.getKey();
            if (name.equals("type")) {
                type(entry.getValue().toString());
            } else if (name.equals("settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("Malformed settings section, should include an inner object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) entry.getValue();
                settings(sub);
            } else if (name.equals("crypto_settings")) {
                if (!(entry.getValue() instanceof Map)) {
                    throw new IllegalArgumentException("Malformed encryption_settings section, should include an inner object");
                }
                @SuppressWarnings("unchecked")
                Map<String, Object> sub = (Map<String, Object>) entry.getValue();
                CryptoSettings cryptoSettings = new CryptoSettings(sub);
                cryptoSettings(cryptoSettings);
            }
        }
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        out.writeString(type);
        writeSettingsToStream(settings, out);
        out.writeBoolean(verify);
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeOptionalWriteable(cryptoSettings);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("name", name);
        builder.field("type", type);

        builder.startObject("settings");
        settings.toXContent(builder, params);
        builder.endObject();

        builder.field("verify", verify);

        if (cryptoSettings != null) {
            builder.startObject("crypto_settings");
            cryptoSettings.toXContent(builder, params);
            builder.endObject();
        }

        builder.endObject();
        return builder;
    }
}
