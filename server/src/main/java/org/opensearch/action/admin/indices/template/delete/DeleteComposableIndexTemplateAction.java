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

package org.opensearch.action.admin.indices.template.delete;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ActionType;
import org.opensearch.action.support.clustermanager.AcknowledgedResponse;
import org.opensearch.action.support.clustermanager.ClusterManagerNodeRequest;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport action for deleting a composable index template
 *
 * @opensearch.internal
 */
public class DeleteComposableIndexTemplateAction extends ActionType<AcknowledgedResponse> {

    public static final DeleteComposableIndexTemplateAction INSTANCE = new DeleteComposableIndexTemplateAction();
    public static final String NAME = "indices:admin/index_template/delete";

    private DeleteComposableIndexTemplateAction() {
        super(NAME, AcknowledgedResponse::new);
    }

    /**
     * Inner Request class for deleting composable index template
     *
     * @opensearch.internal
     */
    public static class Request extends ClusterManagerNodeRequest<Request> {

        private String name;

        public Request(StreamInput in) throws IOException {
            super(in);
            name = in.readString();
        }

        public Request() {}

        /**
         * Constructs a new delete template request for the specified name.
         */
        public Request(String name) {
            this.name = name;
        }

        /**
         * Set the index template name to delete.
         */
        public Request name(String name) {
            this.name = name;
            return this;
        }

        @Override
        public ActionRequestValidationException validate() {
            ActionRequestValidationException validationException = null;
            if (name == null) {
                validationException = addValidationError("name is missing", validationException);
            }
            return validationException;
        }

        /**
         * The index template name to delete.
         */
        public String name() {
            return name;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            super.writeTo(out);
            out.writeString(name);
        }

        @Override
        public int hashCode() {
            return name.hashCode();
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
            return Objects.equals(other.name, this.name);
        }
    }
}
