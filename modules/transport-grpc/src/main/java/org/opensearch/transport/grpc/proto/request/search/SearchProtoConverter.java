/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.transport.grpc.proto.request.search;

import org.apache.lucene.util.BytesRef;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.MatchAllQueryBuilder;
import org.opensearch.index.query.MatchNoneQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.indices.TermsLookup;
import org.opensearch.search.builder.SearchSourceBuilder;

/**
 * Converts a server-internal {@link SearchRequest} into a {@code protobufs.SearchRequest}.
 *
 * <p>This is the inverse of {@link SearchRequestProtoUtils#prepareRequest}, which lives next door. Co-locating both
 * directions enables round-trip parity tests that catch divergence as the schema evolves.
 *
 * <p><b>Current scope</b> (intentionally narrow for the PoC):
 * <ul>
 *   <li>Top-level {@code indices}.</li>
 *   <li>Body fields: {@code from}, {@code size}, {@code query}.</li>
 *   <li>Query types: {@code match_all}, {@code match_none}, {@code term}, {@code terms} — the four query types currently
 *       wired up on the cluster's gRPC parsing path. Anything else throws {@link UnsupportedOperationException}.</li>
 * </ul>
 *
 * <p>The full {@code SearchRequest} surface (indices_options, routing, preference, scroll, search_type, aggregations,
 * sort, suggest, source filtering, highlight, …) is not covered here.
 */
public final class SearchProtoConverter {

    private SearchProtoConverter() {}

    /**
     * Converts a server-internal {@link SearchRequest} to its protobuf equivalent.
     *
     * @param serverReq the request to convert
     * @return the equivalent {@code protobufs.SearchRequest}
     * @throws UnsupportedOperationException if the body uses a query type not yet supported by this converter
     */
    public static org.opensearch.protobufs.SearchRequest toProto(SearchRequest serverReq) {
        org.opensearch.protobufs.SearchRequest.Builder builder = org.opensearch.protobufs.SearchRequest.newBuilder();

        if (serverReq.indices() != null) {
            for (String idx : serverReq.indices()) {
                builder.addIndex(idx);
            }
        }

        SearchSourceBuilder src = serverReq.source();
        if (src != null) {
            builder.setSearchRequestBody(toProtoBody(src));
        }

        return builder.build();
    }

    private static org.opensearch.protobufs.SearchRequestBody toProtoBody(SearchSourceBuilder src) {
        org.opensearch.protobufs.SearchRequestBody.Builder body = org.opensearch.protobufs.SearchRequestBody.newBuilder();

        if (src.from() >= 0) body.setFrom(src.from());
        if (src.size() >= 0) body.setSize(src.size());

        QueryBuilder q = src.query();
        if (q != null) {
            body.setQuery(toProtoQuery(q));
        }
        // Sort, source filtering, highlight, aggs, etc. are deliberately not handled here.
        return body.build();
    }

    private static org.opensearch.protobufs.QueryContainer toProtoQuery(QueryBuilder q) {
        org.opensearch.protobufs.QueryContainer.Builder container = org.opensearch.protobufs.QueryContainer.newBuilder();

        if (q instanceof MatchAllQueryBuilder) {
            container.setMatchAll(toMatchAll((MatchAllQueryBuilder) q));
        } else if (q instanceof MatchNoneQueryBuilder) {
            container.setMatchNone(toMatchNone((MatchNoneQueryBuilder) q));
        } else if (q instanceof TermQueryBuilder) {
            container.setTerm(toTerm((TermQueryBuilder) q));
        } else if (q instanceof TermsQueryBuilder) {
            container.setTerms(toTerms((TermsQueryBuilder) q));
        } else {
            throw new UnsupportedOperationException("query type not yet supported: " + q.getClass().getSimpleName());
        }

        return container.build();
    }

    private static org.opensearch.protobufs.MatchAllQuery toMatchAll(MatchAllQueryBuilder ma) {
        org.opensearch.protobufs.MatchAllQuery.Builder b = org.opensearch.protobufs.MatchAllQuery.newBuilder();
        if (ma.boost() != 1.0f) b.setBoost(ma.boost());
        if (ma.queryName() != null) b.setXName(ma.queryName());
        return b.build();
    }

    private static org.opensearch.protobufs.MatchNoneQuery toMatchNone(MatchNoneQueryBuilder mn) {
        org.opensearch.protobufs.MatchNoneQuery.Builder b = org.opensearch.protobufs.MatchNoneQuery.newBuilder();
        if (mn.boost() != 1.0f) b.setBoost(mn.boost());
        if (mn.queryName() != null) b.setXName(mn.queryName());
        return b.build();
    }

    private static org.opensearch.protobufs.TermQuery toTerm(TermQueryBuilder tq) {
        org.opensearch.protobufs.TermQuery.Builder b = org.opensearch.protobufs.TermQuery.newBuilder()
            .setField(tq.fieldName())
            .setValue(toFieldValue(tq.value()));
        if (tq.boost() != 1.0f) b.setBoost(tq.boost());
        if (tq.queryName() != null) b.setXName(tq.queryName());
        if (tq.caseInsensitive()) b.setCaseInsensitive(true);
        return b.build();
    }

    private static org.opensearch.protobufs.TermsQuery toTerms(TermsQueryBuilder tsq) {
        org.opensearch.protobufs.TermsQuery.Builder b = org.opensearch.protobufs.TermsQuery.newBuilder();
        if (tsq.boost() != 1.0f) b.setBoost(tsq.boost());
        if (tsq.queryName() != null) b.setXName(tsq.queryName());

        org.opensearch.protobufs.TermsQueryField.Builder field = org.opensearch.protobufs.TermsQueryField.newBuilder();
        if (tsq.termsLookup() != null) {
            field.setLookup(toTermsLookup(tsq.termsLookup()));
        } else {
            org.opensearch.protobufs.FieldValueArray.Builder arr = org.opensearch.protobufs.FieldValueArray.newBuilder();
            for (Object v : tsq.values()) {
                arr.addFieldValueArray(toFieldValue(v));
            }
            field.setValue(arr);
        }
        b.putTerms(tsq.fieldName(), field.build());
        return b.build();
    }

    private static org.opensearch.protobufs.TermsLookup toTermsLookup(TermsLookup lookup) {
        org.opensearch.protobufs.TermsLookup.Builder b = org.opensearch.protobufs.TermsLookup.newBuilder()
            .setIndex(lookup.index())
            .setId(lookup.id())
            .setPath(lookup.path());
        if (lookup.routing() != null) b.setRouting(lookup.routing());
        return b.build();
    }

    /**
     * Converts an arbitrary value to a proto {@code FieldValue}. Server-side query builders carry values as
     * {@link Object}; concrete types are {@code String}, {@code Long}, {@code Integer}, {@code Double}, {@code Float},
     * {@code Boolean}, or {@link BytesRef}. {@code BytesRef} is unwrapped to its UTF-8 string form, since the proto
     * forward parser does not accept it directly.
     *
     * @throws IllegalArgumentException if {@code value} is none of the supported runtime types
     */
    private static org.opensearch.protobufs.FieldValue toFieldValue(Object value) {
        org.opensearch.protobufs.FieldValue.Builder b = org.opensearch.protobufs.FieldValue.newBuilder();
        if (value == null) {
            b.setNullValue(org.opensearch.protobufs.NullValue.NULL_VALUE_NULL);
        } else if (value instanceof BytesRef) {
            b.setString(((BytesRef) value).utf8ToString());
        } else if (value instanceof String) {
            b.setString((String) value);
        } else if (value instanceof Boolean) {
            b.setBool((Boolean) value);
        } else if (value instanceof Number) {
            Number num = (Number) value;
            org.opensearch.protobufs.GeneralNumber.Builder n = org.opensearch.protobufs.GeneralNumber.newBuilder();
            if (num instanceof Long || num instanceof Integer || num instanceof Short || num instanceof Byte) {
                n.setInt64Value(num.longValue());
            } else {
                n.setDoubleValue(num.doubleValue());
            }
            b.setGeneralNumber(n);
        } else {
            throw new IllegalArgumentException("unsupported FieldValue type: " + value.getClass().getName());
        }
        return b.build();
    }
}
