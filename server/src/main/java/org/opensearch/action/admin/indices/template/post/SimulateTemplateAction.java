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

package org.opensearch.action.admin.indices.template.post;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.ValidateActions;
import org.opensearch.action.admin.indices.template.put.PutComposableIndexTemplateAction;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeReadRequest;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

/**
 * An action for simulating the complete composed settings of the specified
 * index template name, or index template configuration
 *
 * @opensearch.internal
 */
public class SimulateTemplateAction extends ActionType<SimulateIndexTemplateResponse> {

    public static final SimulateTemplateAction INSTANCE = new SimulateTemplateAction();
    public static final String NAME = "indices:admin/index_template/simulate";

    private SimulateTemplateAction() {
        super(NAME, SimulateIndexTemplateResponse::new);
    }

    /**
     * Request for simulating a template action
     *
     * @opensearch.internal
     */
    public static class Request extends ClusterManagerNodeReadRequest<Request> {

        @Nullable
        private String templateName;

        @Nullable
        private PutComposableIndexTemplateAction.Request indexTemplateRequest;

        public Request() {}

        public Request(String templateName) {
            if (templateName == null) {
                throw new IllegalArgumentException("template name cannot be null");
            }
            this.templateName = templateName;
        }

        public Request(PutComposableIndexTemplateAction.Request indexTemplateRequest) {
            if (indexTemplateRequest == null) {
                throw new IllegalArgumentException("index template body must be present");
            }
            this.indexTemplateRequest = indexTemplateRequest;
        }

        public Request(StreamInput in) throws IOException {
            super(in);
            templateName = in.readOptionalString();
            indexTemplateRequest = in.readOptionalWriteable(PutComposableIndexTemplateAction.Request::new);
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeOptionalString(templateName);
            out.writeOptionalWriteable(indexTemplateRequest);
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (indexTemplateRequest != null) {
                validationException = indexTemplateRequest.validateIndexTemplate(validationException);
            }
            if (templateName == null && indexTemplateRequest == null) {
                validationException = ValidateActions.addValidationError(
                    "either index name or index template body must be specified for simulation",
                    validationException
                );
            }
            return validationException;
        }

        @Nullable
        public String getTemplateName() {
            return templateName;
        }

        @Nullable
        public PutComposableIndexTemplateAction.Request getIndexTemplateRequest() {
            return indexTemplateRequest;
        }

        public Request templateName(String templateName) {
            this.templateName = templateName;
            return this;
        }

        public Request indexTemplateRequest(PutComposableIndexTemplateAction.Request indexTemplateRequest) {
            this.indexTemplateRequest = indexTemplateRequest;
            return this;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Request that = (Request) o;
            return templateName.equals(that.templateName) && Objects.equals(indexTemplateRequest, that.indexTemplateRequest);
        }

        @Override
        public int hashCode() {
            return Objects.hash(templateName, indexTemplateRequest);
        }
    }
}
