/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.pipeline.common;

import org.apache.lucene.search.BooleanClause;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.Map;

/**
 * Search processor that adds routing based on ACL fields in the query.
 */
public class AclRoutingSearchProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /**
     * The type name for this processor.
     */
    public static final String TYPE = "acl_routing_search";
    private static final Base64.Encoder BASE64_ENCODER = Base64.getUrlEncoder().withoutPadding();

    private final String aclField;
    private final boolean extractFromQuery;

    /**
     * Constructor for AclRoutingSearchProcessor.
     *
     * @param tag processor tag
     * @param description processor description
     * @param ignoreFailure whether to ignore failures
     * @param aclField the field to extract ACL values from
     * @param extractFromQuery whether to extract ACL values from query
     */
    public AclRoutingSearchProcessor(String tag, String description, boolean ignoreFailure, String aclField, boolean extractFromQuery) {
        super(tag, description, ignoreFailure);
        this.aclField = aclField;
        this.extractFromQuery = extractFromQuery;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        if (!extractFromQuery || request.source() == null) {
            return request;
        }

        QueryBuilder query = request.source().query();
        if (query == null) {
            return request;
        }

        List<String> aclValues = extractAclValues(query);
        if (aclValues.isEmpty()) {
            return request;
        }

        // Generate routing values
        String[] routingValues = aclValues.stream().map(this::generateRoutingValue).toArray(String[]::new);

        // Set routing on the request
        request.routing(routingValues);

        return request;
    }

    private List<String> extractAclValues(QueryBuilder query) {
        List<String> aclValues = new ArrayList<>();

        query.visit(new QueryBuilderVisitor() {
            @Override
            public void accept(QueryBuilder qb) {
                if (qb instanceof TermQueryBuilder) {
                    TermQueryBuilder termQuery = (TermQueryBuilder) qb;
                    if (aclField.equals(termQuery.fieldName())) {
                        aclValues.add(termQuery.value().toString());
                    }
                } else if (qb instanceof TermsQueryBuilder) {
                    TermsQueryBuilder termsQuery = (TermsQueryBuilder) qb;
                    if (aclField.equals(termsQuery.fieldName())) {
                        termsQuery.values().forEach(value -> aclValues.add(value.toString()));
                    }
                }
            }

            @Override
            public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
                return this;
            }
        });

        return aclValues;
    }

    private String generateRoutingValue(String aclValue) {
        // Use MurmurHash3 for consistent hashing (same as ingest processor)
        byte[] bytes = aclValue.getBytes(StandardCharsets.UTF_8);
        MurmurHash3.Hash128 hash = MurmurHash3.hash128(bytes, 0, bytes.length, 0, new MurmurHash3.Hash128());

        // Convert to base64 for routing value
        byte[] hashBytes = new byte[16];
        System.arraycopy(longToBytes(hash.h1), 0, hashBytes, 0, 8);
        System.arraycopy(longToBytes(hash.h2), 0, hashBytes, 8, 8);

        return BASE64_ENCODER.encodeToString(hashBytes);
    }

    private byte[] longToBytes(long value) {
        byte[] result = new byte[8];
        for (int i = 7; i >= 0; i--) {
            result[i] = (byte) (value & 0xFF);
            value >>= 8;
        }
        return result;
    }

    /**
     * Factory for creating ACL routing search processors.
     */
    public static class Factory implements Processor.Factory<SearchRequestProcessor> {

        /**
         * Constructor for Factory.
         */
        public Factory() {}

        @Override
        public AclRoutingSearchProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            PipelineContext pipelineContext
        ) throws Exception {
            String aclField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "acl_field");
            boolean extractFromQuery = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "extract_from_query", true);

            return new AclRoutingSearchProcessor(tag, description, ignoreFailure, aclField, extractFromQuery);
        }
    }
}
