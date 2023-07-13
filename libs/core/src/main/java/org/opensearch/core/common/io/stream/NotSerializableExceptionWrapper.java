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

package org.opensearch.core.common.io.stream;

import org.opensearch.ExceptionsHelper;
import org.opensearch.OpenSearchException;
import org.opensearch.core.rest.RestStatus;

import java.io.IOException;

/**
 * This exception can be used to wrap a given, not serializable exception
 * to serialize via {@link StreamOutput#writeException(Throwable)}.
 * This class will preserve the stacktrace as well as the suppressed exceptions of
 * the throwable it was created with instead of it's own. The stacktrace has no indication
 * of where this exception was created.
 *
 * @opensearch.internal
 */
public final class NotSerializableExceptionWrapper extends OpenSearchException {

    private final String name;
    private final RestStatus status;

    public NotSerializableExceptionWrapper(Throwable other) {
        super(OpenSearchException.getExceptionName(other) + ": " + other.getMessage(), other.getCause());
        this.name = OpenSearchException.getExceptionName(other);
        this.status = ExceptionsHelper.status(other);
        setStackTrace(other.getStackTrace());
        for (Throwable otherSuppressed : other.getSuppressed()) {
            addSuppressed(otherSuppressed);
        }
        if (other instanceof OpenSearchException) {
            OpenSearchException ex = (OpenSearchException) other;
            for (String key : ex.getHeaderKeys()) {
                this.addHeader(key, ex.getHeader(key));
            }
            for (String key : ex.getMetadataKeys()) {
                this.addMetadata(key, ex.getMetadata(key));
            }
        }
    }

    public NotSerializableExceptionWrapper(StreamInput in) throws IOException {
        super(in);
        name = in.readString();
        status = RestStatus.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(name);
        RestStatus.writeTo(out, status);
    }

    @Override
    protected String getExceptionName() {
        return name;
    }

    @Override
    public RestStatus status() {
        return status;
    }
}
