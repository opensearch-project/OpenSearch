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

package org.opensearch.action.get;

import org.opensearch.OpenSearchParseException;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.document.DocumentField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.ParsingException;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.get.GetResult;
import org.opensearch.transport.client.Client;

import java.io.IOException;
import java.util.Iterator;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;

/**
 * The response of a get action.
 *
 * @see GetRequest
 * @see Client#get(GetRequest)
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class GetResponse extends ActionResponse implements Iterable<DocumentField>, ToXContentObject {

    GetResult getResult;

    GetResponse(StreamInput in) throws IOException {
        super(in);
        getResult = new GetResult(in);
    }

    public GetResponse(GetResult getResult) {
        this.getResult = getResult;
    }

    /**
     * Does the document exists.
     */
    public boolean isExists() {
        return getResult.isExists();
    }

    /**
     * The index the document was fetched from.
     */
    public String getIndex() {
        return getResult.getIndex();
    }

    /**
     * The id of the document.
     */
    public String getId() {
        return getResult.getId();
    }

    /**
     * The version of the doc.
     */
    public long getVersion() {
        return getResult.getVersion();
    }

    /**
     * The sequence number assigned to the last operation that has changed this document, if found.
     */
    public long getSeqNo() {
        return getResult.getSeqNo();
    }

    /**
     * The primary term of the last primary that has changed this document, if found.
     */
    public long getPrimaryTerm() {
        return getResult.getPrimaryTerm();
    }

    /**
     * The source of the document if exists.
     */
    public byte[] getSourceAsBytes() {
        return getResult.source();
    }

    /**
     * Returns the internal source bytes, as they are returned without munging (for example,
     * might still be compressed).
     */
    public BytesReference getSourceInternal() {
        return getResult.internalSourceRef();
    }

    /**
     * Returns bytes reference, also un compress the source if needed.
     */
    public BytesReference getSourceAsBytesRef() {
        return getResult.sourceRef();
    }

    /**
     * Is the source empty (not available) or not.
     */
    public boolean isSourceEmpty() {
        return getResult.isSourceEmpty();
    }

    /**
     * The source of the document (as a string).
     */
    public String getSourceAsString() {
        return getResult.sourceAsString();
    }

    /**
     * The source of the document (As a map).
     */
    public Map<String, Object> getSourceAsMap() throws OpenSearchParseException {
        return getResult.sourceAsMap();
    }

    public Map<String, Object> getSource() {
        return getResult.getSource();
    }

    public Map<String, DocumentField> getFields() {
        return getResult.getFields();
    }

    public DocumentField getField(String name) {
        return getResult.field(name);
    }

    /**
     * @deprecated Use {@link GetResponse#getSource()} instead
     */
    @Deprecated
    public Iterator<DocumentField> iterator() {
        return getResult.iterator();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        return getResult.toXContent(builder, params);
    }

    /**
     * This method can be used to parse a {@link GetResponse} object when it has been printed out
     * as a xcontent using the {@link #toXContent(XContentBuilder, Params)} method.
     * <p>
     * For forward compatibility reason this method might not fail if it tries to parse a field it
     * doesn't know. But before returning the result it will check that enough information were
     * parsed to return a valid {@link GetResponse} instance and throws a {@link ParsingException}
     * otherwise. This is the case when we get a 404 back, which can be parsed as a normal
     * {@link GetResponse} with found set to false, or as an opensearch exception. The caller
     * of this method needs a way to figure out whether we got back a valid get response, which
     * can be done by catching ParsingException.
     *
     * @param parser {@link XContentParser} to parse the response from
     * @return a {@link GetResponse}
     * @throws IOException is an I/O exception occurs during the parsing
     */
    public static GetResponse fromXContent(XContentParser parser) throws IOException {
        GetResult getResult = GetResult.fromXContent(parser);

        // At this stage we ensure that we parsed enough information to return
        // a valid GetResponse instance. If it's not the case, we throw an
        // exception so that callers know it and can handle it correctly.
        if (getResult.getIndex() == null && getResult.getId() == null) {
            throw new ParsingException(
                parser.getTokenLocation(),
                String.format(Locale.ROOT, "Missing required fields [%s,%s]", GetResult._INDEX, GetResult._ID)
            );
        }
        return new GetResponse(getResult);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        getResult.writeTo(out);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        GetResponse getResponse = (GetResponse) o;
        return Objects.equals(getResult, getResponse.getResult);
    }

    @Override
    public int hashCode() {
        return Objects.hash(getResult);
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }
}
