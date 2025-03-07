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

package org.opensearch.action.admin.indices.template.put;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.IndicesRequest;
import org.opensearch.action.support.IndicesOptions;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.cluster.metadata.ComposableIndexTemplate;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * An action for putting a composable template into the cluster state
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class PutComposableIndexTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final PutComposableIndexTemplateAction INSTANCE = new PutComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/put";

    private PutComposableIndexTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * A request for putting a single index template into the cluster state
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Request extends ClusterManagerNodeRequest<Request> implements IndicesRequest {
        private final String name;
        @Nullable
        private String cause;
        private boolean create;
        private ComposableIndexTemplate indexTemplate;

        public Request(StreamInput in) throws IOException {
            super(in);
            this.name = in.readString();
            this.cause = in.readOptionalString();
            this.create = in.readBoolean();
            this.indexTemplate = new ComposableIndexTemplate(in);
        }

        /**
         * Constructs a new put index template request with the provided name.
         */
        public Request(String name) {
            this.name = name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
            out.writeOptionalString(cause);
            out.writeBoolean(create);
            this.indexTemplate.writeTo(out);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null || Strings.hasText(name) == false) {
                validationException = addValidationError("name is missing", validationException);
            }
            validationException = validateIndexTemplate(validationException);
            return validationException;
        }

        public ActionRequestValidationException validateIndexTemplate(@Nullable ActionRequestValidationException validationException) {
            if (indexTemplate == null) {
                validationException = addValidationError("an index template is required", validationException);
            } else {
                if (indexTemplate.indexPatterns().stream().anyMatch(Regex::isMatchAllPattern)) {
                    if (IndexMetadata.INDEX_HIDDEN_SETTING.exists(indexTemplate.template().settings())) {
                        validationException = addValidationError(
                            "global composable templates may not specify the setting " + IndexMetadata.INDEX_HIDDEN_SETTING.getKey(),
                            validationException
                        );
                    }
                }
                if (indexTemplate.priority() != null && indexTemplate.priority() < 0) {
                    validationException = addValidationError("index template priority must be >= 0", validationException);
                }
            }
            return validationException;
        }

        /**
         * The name of the index template.
         */
        public String name() {
            return this.name;
        }

        /**
         * Set to {@code true} to force only creation, not an update of an index template. If it already
         * exists, it will fail with an {@link IllegalArgumentException}.
         */
        public Request create(boolean create) {
            this.create = create;
            return this;
        }

        public boolean create() {
            return create;
        }

        /**
         * The cause for this index template creation.
         */
        public Request cause(@Nullable String cause) {
            this.cause = cause;
            return this;
        }

        @Nullable
        public String cause() {
            return this.cause;
        }

        /**
         * The index template that will be inserted into the cluster state
         */
        public Request indexTemplate(ComposableIndexTemplate template) {
            this.indexTemplate = template;
            return this;
        }

        public ComposableIndexTemplate indexTemplate() {
            return this.indexTemplate;
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder("PutTemplateV2Request[");
            sb.append("name=").append(name);
            sb.append(", cause=").append(cause);
            sb.append(", create=").append(create);
            sb.append(", index_template=").append(indexTemplate);
            sb.append("]");
            return sb.toString();
        }

        @Override
        public String[] indices() {
            return indexTemplate.indexPatterns().toArray(Strings.EMPTY_ARRAY);
        }

        @Override
        public IndicesOptions indicesOptions() {
            return IndicesOptions.strictExpand();
        }

        @Override
        public int hashCode() {
            return Objects.hash(name, cause, create, indexTemplate);
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            Request other = (Request) obj;
            return Objects.equals(this.name, other.name)
                && Objects.equals(this.cause, other.cause)
                && Objects.equals(this.indexTemplate, other.indexTemplate)
                && this.create == other.create;
        }
    }

}
