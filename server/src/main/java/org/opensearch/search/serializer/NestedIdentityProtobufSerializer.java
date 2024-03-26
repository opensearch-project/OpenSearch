/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.serializer;

import org.opensearch.search.SearchHit.NestedIdentity;
import org.opensearch.server.proto.FetchSearchResultProto;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

/**
 * Serializer for {@link NestedIdentity} to/from protobuf.
 */
public class NestedIdentityProtobufSerializer implements NestedIdentitySerializer<InputStream> {

    @Override
    public NestedIdentity createNestedIdentity(InputStream inputStream) throws IOException {
        FetchSearchResultProto.SearchHit.NestedIdentity proto = FetchSearchResultProto.SearchHit.NestedIdentity.parseFrom(inputStream);
        String field;
        int offset;
        NestedIdentity child;
        if (proto.hasField()) {
            field = proto.getField();
        } else {
            field = null;
        }
        if (proto.hasOffset()) {
            offset = proto.getOffset();
        } else {
            offset = -1;
        }
        if (proto.hasChild()) {
            child = createNestedIdentity(new ByteArrayInputStream(proto.getChild().toByteArray()));
        } else {
            child = null;
        }
        return new NestedIdentity(field, offset, child);
    }

}
