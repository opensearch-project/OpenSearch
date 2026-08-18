/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.query;

import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.transport.grpc.spi.QueryBuilderProtoConverterRegistry;

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
     * Convenience overload for lookups that cannot contain a nested query.
     */
    protected static TermsLookup parseTermsLookup(org.opensearch.protobufs.TermsLookup termsLookupProto) {
        return parseTermsLookup(termsLookupProto, null);
    }

    /**
     * Converts a Protocol Buffer TermsLookup to an OpenSearch TermsLookup object.
     * Similar to {@link TermsLookup#parseTermsLookup(XContentParser)}.
     * <p>
     * As of protobufs 1.7.0, terms are identified either by {@code id_2} or by a nested {@code query};
     * the deprecated {@code id} field is honored when the oneof is not set.
     *
     * @param termsLookupProto the Protocol Buffer TermsLookup object
     * @param registry used to convert a {@code query} lookup; may be null when the lookup cannot contain a query
     * @return A configured TermsLookup instance
     */
    @SuppressWarnings("deprecation")
    protected static TermsLookup parseTermsLookup(
        org.opensearch.protobufs.TermsLookup termsLookupProto,
        QueryBuilderProtoConverterRegistry registry
    ) {

        String index = termsLookupProto.getIndex();
        String path = termsLookupProto.getPath();

        String id = null;
        QueryBuilder query = null;

        switch (termsLookupProto.getTermsLookupCase()) {
            case ID_2:
                id = termsLookupProto.getId2();
                break;
            case QUERY:
                if (registry == null) {
                    throw new IllegalArgumentException("A query converter registry is required to parse a query-based terms lookup");
                }
                query = registry.fromProto(termsLookupProto.getQuery());
                break;
            case TERMSLOOKUP_NOT_SET:
            default:
                // Backwards compatibility: fall back to the deprecated `id` field.
                id = termsLookupProto.getId();
                break;
        }

        TermsLookup termsLookup = new TermsLookup(index, id, path, query);

        if (termsLookupProto.hasRouting()) {
            termsLookup.routing(termsLookupProto.getRouting());
        }

        if (termsLookupProto.hasStore()) {
            termsLookup.store(termsLookupProto.getStore());
        }

        return termsLookup;
    }
}
