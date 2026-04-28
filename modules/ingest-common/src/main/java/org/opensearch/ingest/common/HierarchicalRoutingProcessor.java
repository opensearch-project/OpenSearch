/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.common.Nullable;
import org.opensearch.common.hash.MurmurHash3;
import org.opensearch.core.common.Strings;
import org.opensearch.ingest.AbstractProcessor;
import org.opensearch.ingest.ConfigurationUtils;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;

import java.nio.charset.StandardCharsets;
import java.util.Map;

import static org.opensearch.ingest.ConfigurationUtils.newConfigurationException;

/**
 * Processor that sets document routing based on hierarchical path structure.
 *
 * This processor extracts a path from a specified field, normalizes it,
 * and uses a configurable depth of path segments to compute a routing value
 * using MurmurHash3 for consistent distribution across shards.
 *
 * Introduced in OpenSearch 3.2.0 to enable intelligent document co-location
 * based on hierarchical path structures.
 */
public final class HierarchicalRoutingProcessor extends AbstractProcessor {

    public static final String TYPE = "hierarchical_routing";

    private final String pathField;
    private final int anchorDepth;
    private final String pathSeparator;
    private final boolean ignoreMissing;
    private final boolean overrideExisting;
    private final java.util.regex.Pattern pathSeparatorPattern;
    private final java.util.regex.Pattern multiSeparatorPattern;

    HierarchicalRoutingProcessor(
        String tag,
        @Nullable String description,
        String pathField,
        int anchorDepth,
        String pathSeparator,
        boolean ignoreMissing,
        boolean overrideExisting
    ) {
        super(tag, description);
        this.pathField = pathField;
        this.anchorDepth = anchorDepth;
        this.pathSeparator = pathSeparator;
        this.ignoreMissing = ignoreMissing;
        this.overrideExisting = overrideExisting;
        this.pathSeparatorPattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(pathSeparator));
        this.multiSeparatorPattern = java.util.regex.Pattern.compile(java.util.regex.Pattern.quote(pathSeparator) + "{2,}");
    }

    @Override
    public IngestDocument execute(IngestDocument document) throws Exception {
        // Check if routing already exists and we shouldn't override
        if (!overrideExisting) {
            try {
                Object existingRouting = document.getFieldValue("_routing", Object.class, true);
                if (existingRouting != null) {
                    return document;
                }
            } catch (Exception e) {
                // Field doesn't exist, continue with processing
            }
        }

        Object pathValue = document.getFieldValue(pathField, Object.class, ignoreMissing);

        if (pathValue == null && ignoreMissing) {
            return document;
        }

        if (pathValue == null) {
            throw new IllegalArgumentException("field [" + pathField + "] doesn't exist");
        }

        String path = pathValue.toString();
        if (Strings.isNullOrEmpty(path)) {
            if (ignoreMissing) {
                return document;
            }
            throw new IllegalArgumentException("field [" + pathField + "] is null or empty");
        }

        String routingValue = computeRoutingValue(path);
        document.setFieldValue("_routing", routingValue);

        return document;
    }

    /**
     * Computes the routing value from the given path by normalizing,
     * extracting the anchor segments, and hashing.
     */
    private String computeRoutingValue(String path) {
        String normalizedPath = normalizePath(path);
        String[] segments = pathSeparatorPattern.split(normalizedPath);
        String anchor = extractAnchor(segments, anchorDepth);

        // Use MurmurHash3 for consistent, fast hashing
        byte[] anchorBytes = anchor.getBytes(StandardCharsets.UTF_8);
        long hash = MurmurHash3.hash128(anchorBytes, 0, anchorBytes.length, 0, new MurmurHash3.Hash128()).h1;

        return String.valueOf(hash == Long.MIN_VALUE ? 0L : (hash < 0 ? -hash : hash));
    }

    /**
     * Normalizes the path by removing leading/trailing separators and
     * collapsing multiple consecutive separators.
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
     * Extracts the anchor from path segments using the specified depth.
     * Empty segments are filtered out.
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

    @Override
    public String getType() {
        return TYPE;
    }

    public static final class Factory implements Processor.Factory {

        @Override
        public HierarchicalRoutingProcessor create(
            Map<String, Processor.Factory> processorFactories,
            String tag,
            @Nullable String description,
            Map<String, Object> config
        ) throws Exception {

            String pathField = ConfigurationUtils.readStringProperty(TYPE, tag, config, "path_field");
            int anchorDepth = ConfigurationUtils.readIntProperty(TYPE, tag, config, "anchor_depth", 2);
            String pathSeparator = ConfigurationUtils.readOptionalStringProperty(TYPE, tag, config, "path_separator");
            boolean ignoreMissing = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "ignore_missing", false);
            boolean overrideExisting = ConfigurationUtils.readBooleanProperty(TYPE, tag, config, "override_existing", true);

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

            return new HierarchicalRoutingProcessor(
                tag,
                description,
                pathField,
                anchorDepth,
                pathSeparator,
                ignoreMissing,
                overrideExisting
            );
        }
    }
}
