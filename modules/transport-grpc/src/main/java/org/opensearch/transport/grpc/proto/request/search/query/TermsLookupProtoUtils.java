/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.indices.TermsLookup;

/**
 * Utility class for converting TermsLookup Protocol Buffers to OpenSearch objects.
 * This class provides methods to transform Protocol Buffer representations of terms lookups
 * into their corresponding OpenSearch TermsLookup implementations for search operations.
 */
public class TermsLookupProtoUtils {

    private TermsLookupProtoUtils() {
        // Utility class, no instances
    }

    /**
     * Converts a Protocol Buffer TermsLookup to an OpenSearch TermsLookup object.
     * Similar to {@link TermsLookup#parseTermsLookup(XContentParser)}
     *
     * @param termsLookupProto The Protocol Buffer TermsLookup object containing index, id, path, and optional routing/store values
     * @return A configured TermsLookup instance with the appropriate settings
     */
    protected static TermsLookup parseTermsLookup(org.opensearch.protobufs.TermsLookup termsLookupProto) {

        String index = termsLookupProto.getIndex();
        String id = termsLookupProto.getId();
        String path = termsLookupProto.getPath();

        TermsLookup termsLookup = new TermsLookup(index, id, path);

        if (termsLookupProto.hasRouting()) {
            termsLookup.routing(termsLookupProto.getRouting());
        }

        if (termsLookupProto.hasStore()) {
            termsLookup.store(termsLookupProto.getStore());
        }

        return termsLookup;
    }
}
