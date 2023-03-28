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

package org.opensearch.action.admin.cluster.storedscripts;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.support.master.AcknowledgedRequest;
import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.script.StoredScriptSource;

import java.io.IOException;
import java.util.Objects;

import static org.opensearch.action.ValidateActions.addValidationError;

/**
 * Transport request for putting stored script
 *
 * @opensearch.internal
 */
public class PutStoredScriptRequest extends AcknowledgedRequest<PutStoredScriptRequest> implements ToXContentFragment {

    private String id;
    private String context;
    private BytesReference content;
    private XContentType xContentType;
    private StoredScriptSource source;

    public PutStoredScriptRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readOptionalString();
        content = in.readBytesReference();
        xContentType = in.readEnum(XContentType.class);
        context = in.readOptionalString();
        source = new StoredScriptSource(in);
    }

    public PutStoredScriptRequest() {
        super();
    }

    public PutStoredScriptRequest(String id, String context, BytesReference content, XContentType xContentType, StoredScriptSource source) {
        super();
        this.id = id;
        this.context = context;
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
        this.source = source;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;

        if (id == null || id.isEmpty()) {
            validationException = addValidationError("must specify id for stored script", validationException);
        } else if (id.contains("#")) {
            validationException = addValidationError("id cannot contain '#' for stored script", validationException);
        }

        if (content == null) {
            validationException = addValidationError("must specify code for stored script", validationException);
        }

        return validationException;
    }

    public String id() {
        return id;
    }

    public PutStoredScriptRequest id(String id) {
        this.id = id;
        return this;
    }

    public String context() {
        return context;
    }

    public PutStoredScriptRequest context(String context) {
        this.context = context;
        return this;
    }

    public BytesReference content() {
        return content;
    }

    public XContentType xContentType() {
        return xContentType;
    }

    public StoredScriptSource source() {
        return source;
    }

    /**
     * Set the script source and the content type of the bytes.
     */
    public PutStoredScriptRequest content(BytesReference content, XContentType xContentType) {
        this.content = content;
        this.xContentType = Objects.requireNonNull(xContentType);
        this.source = StoredScriptSource.parse(content, xContentType);
        return this;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(id);
        out.writeBytesReference(content);
        out.writeEnum(xContentType);
        out.writeOptionalString(context);
        source.writeTo(out);
    }

    @Override
    public String toString() {
        String source = "_na_";

        try {
            source = XContentHelper.convertToJson(content, false, xContentType);
        } catch (Exception e) {
            // ignore
        }

        return "put stored script {id ["
            + id
            + "]"
            + (context != null ? ", context [" + context + "]" : "")
            + ", content ["
            + source
            + "]}";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field("script");
        source.toXContent(builder, params);

        return builder;
    }
}
