/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ubi.ext;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentHelper;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

import java.io.EOFException;
import java.io.IOException;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class UbiParametersExtBuilderTests extends OpenSearchTestCase {

    public void testCtor() throws IOException {

        final UbiParametersExtBuilder builder = new UbiParametersExtBuilder();
        final UbiParameters parameters = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        builder.setParams(parameters);
        assertEquals(parameters, builder.getParams());

        final UbiParametersExtBuilder builder1 = new UbiParametersExtBuilder(new StreamInput() {
            @Override
            public byte readByte() throws IOException {
                return 0;
            }

            @Override
            public void readBytes(byte[] b, int offset, int len) throws IOException {

            }

            @Override
            public void close() throws IOException {

            }

            @Override
            public int available() throws IOException {
                return 0;
            }

            @Override
            protected void ensureCanReadBytes(int length) throws EOFException {

            }

            @Override
            public int read() throws IOException {
                return 0;
            }
        });

        assertNotNull(builder1);

    }

    public void testParse() throws IOException {
        XContentParser xcParser = mock(XContentParser.class);
        when(xcParser.nextToken()).thenReturn(XContentParser.Token.START_OBJECT).thenReturn(XContentParser.Token.END_OBJECT);
        UbiParametersExtBuilder builder = UbiParametersExtBuilder.parse(xcParser);
        assertNotNull(builder);
        assertNotNull(builder.getParams());
    }

    public void testXContentRoundTrip() throws IOException {
        UbiParameters param1 = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        UbiParametersExtBuilder extBuilder = new UbiParametersExtBuilder();
        extBuilder.setParams(param1);
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference serialized = XContentHelper.toXContent(extBuilder, xContentType, true);
        XContentParser parser = createParser(xContentType.xContent(), serialized);
        UbiParametersExtBuilder deserialized = UbiParametersExtBuilder.parse(parser);
        assertEquals(extBuilder, deserialized);
        UbiParameters parameters = deserialized.getParams();
        assertEquals("query_id", parameters.getQueryId());
        assertEquals("user_query", parameters.getUserQuery());
        assertEquals("client_id", parameters.getClientId());
        assertEquals("object_id", parameters.getObjectId());
    }

    public void testXContentRoundTripAllValues() throws IOException {
        UbiParameters param1 = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        UbiParametersExtBuilder extBuilder = new UbiParametersExtBuilder();
        extBuilder.setParams(param1);
        XContentType xContentType = randomFrom(XContentType.values());
        BytesReference serialized = XContentHelper.toXContent(extBuilder, xContentType, true);
        XContentParser parser = createParser(xContentType.xContent(), serialized);
        UbiParametersExtBuilder deserialized = UbiParametersExtBuilder.parse(parser);
        assertEquals(extBuilder, deserialized);
    }

    public void testStreamRoundTrip() throws IOException {
        UbiParameters param1 = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        UbiParametersExtBuilder extBuilder = new UbiParametersExtBuilder();
        extBuilder.setParams(param1);
        BytesStreamOutput bso = new BytesStreamOutput();
        extBuilder.writeTo(bso);
        UbiParametersExtBuilder deserialized = new UbiParametersExtBuilder(bso.bytes().streamInput());
        assertEquals(extBuilder, deserialized);
        UbiParameters parameters = deserialized.getParams();
        assertEquals("query_id", parameters.getQueryId());
        assertEquals("user_query", parameters.getUserQuery());
        assertEquals("client_id", parameters.getClientId());
        assertEquals("object_id", parameters.getObjectId());
    }

    public void testStreamRoundTripAllValues() throws IOException {
        UbiParameters param1 = new UbiParameters("query_id", "user_query", "client_id", "object_id");
        UbiParametersExtBuilder extBuilder = new UbiParametersExtBuilder();
        extBuilder.setParams(param1);
        BytesStreamOutput bso = new BytesStreamOutput();
        extBuilder.writeTo(bso);
        UbiParametersExtBuilder deserialized = new UbiParametersExtBuilder(bso.bytes().streamInput());
        assertEquals(extBuilder, deserialized);
    }

}
