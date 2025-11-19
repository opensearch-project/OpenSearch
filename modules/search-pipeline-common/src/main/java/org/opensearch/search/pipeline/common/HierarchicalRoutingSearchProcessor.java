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
import org.opensearch.core.common.Strings;
import org.opensearch.index.query.PrefixQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilderVisitor;
import org.opensearch.index.query.TermQueryBuilder;
import org.opensearch.index.query.TermsQueryBuilder;
import org.opensearch.index.query.WildcardQueryBuilder;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.search.pipeline.AbstractProcessor;
import org.opensearch.search.pipeline.Processor;
import org.opensearch.search.pipeline.SearchRequestProcessor;

import java.nio.charset.StandardCharsets;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * A search request processor that automatically adds routing to search requests
 * based on hierarchical path information found in queries.
 *
 * This processor works in conjunction with the HierarchicalRoutingProcessor
 * (ingest pipeline) to optimize searches by routing them only to shards
 * that contain documents with matching path prefixes.
 *
 * Example: A query with filter "path:/company/engineering/*" will only
 * search shards containing documents routed with "/company/engineering" prefix.
 */
public class HierarchicalRoutingSearchProcessor extends AbstractProcessor implements SearchRequestProcessor {

    /** The processor type identifier */
    public static final String TYPE = "hierarchical_routing_search";

    private final String pathField;
    private final int anchorDepth;
    private final String pathSeparator;
    private final boolean enableAutoDetection;
    private final java.util.regex.Pattern pathSeparatorPattern;
    private final java.util.regex.Pattern multiSeparatorPattern;

    HierarchicalRoutingSearchProcessor(
        String tag,
        String description,
        boolean ignoreFailure,
        String pathField,
        int anchorDepth,
        String pathSeparator,
        boolean enableAutoDetection
    ) {
        super(tag, description, ignoreFailure);
        this.pathField = pathField;
        this.anchorDepth = anchorDepth;
        this.pathSeparator = pathSeparator;
        this.enableAutoDetection = enableAutoDetection;
        this.pathSeparatorPattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(pathSeparator));
        this.multiSeparatorPattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(pathSeparator) + "{2,}");
    }

    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public SearchRequest processRequest(SearchRequest request) throws Exception {
        // Skip if routing is already explicitly set
        if (request.routing() != null && !request.routing().isEmpty()) {
            return request;
        }

        Set<String> routingValues = new HashSet<>();

        // Extract path information from the search request using visitor pattern
        if (request.source() != null && request.source().query() != null) {
            PathExtractionVisitor visitor = new PathExtractionVisitor(routingValues);
            request.source().query().visit(visitor);
        }

        // If we found path information, compute routing and apply it
        if (!routingValues.isEmpty()) {
            Set<String> computedRouting = new HashSet<>();
            for (String path : routingValues) {
                String routingValue = computeRoutingValue(path);
                if (routingValue != null) {
                    computedRouting.add(routingValue);
                }
            }

            if (!computedRouting.isEmpty()) {
                // Join multiple routing values with comma
                String routing = String.join(",", computedRouting);
                request.routing(routing);
            }
        }

        return request;
    }

    /**
     * Visitor implementation for extracting hierarchical paths from queries
     */
    private class PathExtractionVisitor implements QueryBuilderVisitor {
        private final Set<String> paths;

        PathExtractionVisitor(Set<String> paths) {
            this.paths = paths;
        }

        @Override
        public void accept(QueryBuilder qb) {
            if (qb instanceof TermQueryBuilder termQuery) {
                if (pathField.equals(termQuery.fieldName())) {
                    paths.add(termQuery.value().toString());
                }
            } else if (qb instanceof TermsQueryBuilder termsQuery) {
                if (pathField.equals(termsQuery.fieldName())) {
                    for (Object value : termsQuery.values()) {
                        paths.add(value.toString());
                    }
                }
            } else if (qb instanceof PrefixQueryBuilder prefixQuery) {
                if (pathField.equals(prefixQuery.fieldName())) {
                    paths.add(prefixQuery.value());
                }
            } else if (qb instanceof WildcardQueryBuilder wildcardQuery) {
                if (pathField.equals(wildcardQuery.fieldName())) {
                    String pattern = wildcardQuery.value();
                    String pathPrefix = extractPrefixFromWildcard(pattern);
                    if (pathPrefix != null && !pathPrefix.isEmpty()) {
                        paths.add(pathPrefix);
                    }
                }
            }
            // The visitor pattern will automatically handle other query types
        }

        @Override
        public QueryBuilderVisitor getChildVisitor(BooleanClause.Occur occur) {
            // Only process MUST and FILTER clauses as they restrict results
            // SHOULD and MUST_NOT don't guarantee document presence on specific shards
            if (occur == BooleanClause.Occur.MUST || occur == BooleanClause.Occur.FILTER) {
                return this;
            }
            // Return a no-op visitor for SHOULD and MUST_NOT clauses
            return QueryBuilderVisitor.NO_OP_VISITOR;
        }
    }

    /**
     * Extracts the stable prefix from a wildcard pattern
     * Example: "/company/engineering/*" -> "/company/engineering"
     */
    private String extractPrefixFromWildcard(String pattern) {
        if (pattern == null) return null;

        int wildcardIndex = pattern.indexOf('*');
        int questionIndex = pattern.indexOf('?');

        int firstWildcard = -1;
        if (wildcardIndex >= 0) firstWildcard = wildcardIndex;
        if (questionIndex >= 0 && (firstWildcard < 0 || questionIndex < firstWildcard)) {
            firstWildcard = questionIndex;
        }

        if (firstWildcard >= 0) {
            return pattern.substring(0, firstWildcard);
        }

        return pattern; // No wildcards, return as-is
    }

    /**
     * Computes routing value from path using same logic as ingest processor
     */
    private String computeRoutingValue(String path) {
        if (Strings.isNullOrEmpty(path)) {
            return null;
        }

        String normalizedPath = normalizePath(path);
        String[] segments = pathSeparatorPattern.split(normalizedPath);
        String anchor = extractAnchor(segments, anchorDepth);

        // Use MurmurHash3 for consistent, fast hashing (same as ingest processor)
        byte[] anchorBytes = anchor.getBytes(StandardCharsets.UTF_8);
        long hash = MurmurHash3.hash128(anchorBytes, 0, anchorBytes.length, 0, new MurmurHash3.Hash128()).h1;

        return String.valueOf(hash == Long.MIN_VALUE ? 0L : (hash < 0 ? -hash : hash));
    }

    /**
     * Normalizes path by removing leading/trailing separators and
     * collapsing multiple consecutive separators (same as ingest processor)
     */
    private String normalizePath(String path) {
        String normalized = path.trim();

        // Remove leading separators
        while (normalized.startsWith(pathSeparator)) {
            normalized = normalized.substring(pathSeparator.length());
        }

        // Remove trailing separators
        while (normalized.endsWith(pathSeparator)) {
            normalized = normalized.substring(0, normalized.length() - pathSeparator.length());
        }

        // Replace multiple consecutive separators with single separator
        normalized = multiSeparatorPattern.matcher(normalized).replaceAll(pathSeparator);

        return normalized;
    }

    /**
     * Extracts anchor from path segments using specified depth
     * (same as ingest processor)
     */
    private String extractAnchor(String[] segments, int depth) {
        StringBuilder anchor = new StringBuilder();
        int effectiveDepth = Math.min(depth, segments.length);
        int addedSegments = 0;

        for (int i = 0; i < effectiveDepth && addedSegments < depth; i++) {
            if (!Strings.isNullOrEmpty(segments[i])) {
                if (addedSegments > 0) {
                    anchor.append(pathSeparator);
                }
                anchor.append(segments[i]);
                addedSegments++;
            }
        }

        // If no valid segments found, use a default
        if (anchor.length() == 0) {
            return "_root";
        }

        return anchor.toString();
    }

    /**
     * Factory for creating HierarchicalRoutingSearchProcessor instances
     */
    public static final class Factory implements Processor.Factory<SearchRequestProcessor> {

        /**
         * Creates a new Factory instance
         */
        public Factory() {}

        /**
         * Creates a new HierarchicalRoutingSearchProcessor instance
         *
         * @param processorFactories available processor factories
         * @param tag processor tag
         * @param description processor description
         * @param ignoreFailure whether to ignore failures
         * @param config processor configuration
         * @param pipelineContext pipeline context
         * @return new HierarchicalRoutingSearchProcessor instance
         * @throws Exception if configuration is invalid
         */
        @Override
        public HierarchicalRoutingSearchProcessor create(
            Map<String, Processor.Factory<SearchRequestProcessor>> processorFactories,
            String tag,
            String description,
            boolean ignoreFailure,
            Map<String, Object> config,
            Processor.PipelineContext pipelineContext
        ) throws Exception {

            String pathField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "path_field");
            int anchorDepth = ConfigurationUtils.readIntProperty(TYPE, tag, config, "anchor_depth", 2);
            String pathSeparator = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "path_separator");
            boolean enableAutoDetection = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "enable_auto_detection", true);

            // Set default path separator if not provided
            if (pathSeparator == null) {
                pathSeparator = "/";
            }

            // Validation
            if (anchorDepth <= 0) {
                throw newConfigurationException(TYPE, tag, "anchor_depth", "must be greater than 0");
            }

            if (Strings.isNullOrEmpty(pathSeparator)) {
                throw newConfigurationException(TYPE, tag, "path_separator", "cannot be null or empty");
            }

            if (Strings.isNullOrEmpty(pathField)) {
                throw newConfigurationException(TYPE, tag, "path_field", "cannot be null or empty");
            }

            return new HierarchicalRoutingSearchProcessor(
                tag,
                description,
                ignoreFailure,
                pathField,
                anchorDepth,
                pathSeparator,
                enableAutoDetection
            );
        }
    }
}
