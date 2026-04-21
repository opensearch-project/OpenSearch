/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.regex.Regex;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

/**
 * A dynamic property defines a pattern-based mapping: any field whose name matches
 * the pattern is mapped using the same configuration without updating the index mapping.
 * Similar to Solr dynamic fields; avoids cluster state updates for high-throughput ingestion.
 *
 * <p><b>Operational note:</b> When a document field matches a dynamic property, ingest builds
 * {@link FieldMapper} instances and indexes using that configuration <em>without</em> persisting
 * new field definitions into the cluster state mapping (no mapping-update round trip for that
 * field). That is intentional for performance and matches Solr-style dynamic fields.
 *
 * <p>The pattern key {@code *} alone is not allowed in {@code dynamic_properties}; each pattern must
 * contain {@code *} as part of a more specific glob (e.g. {@code *_i}). To default-type every unknown
 * field name with one rule, use {@code dynamic_templates} on the root mapping instead.
 *
 * <p><b>Mapping merge (PUT mapping / templates):</b> For {@link MapperService.MergeReason#MAPPING_UPDATE}
 * and {@link MapperService.MergeReason#INDEX_TEMPLATE}, incoming {@code dynamic_properties} are
 * <strong>merged by pattern key</strong> with the existing root mapping: new pattern keys are added;
 * the same pattern string as an existing entry replaces that entry’s mapping. A partial body that only
 * lists one pattern therefore does <em>not</em> remove other patterns (unlike a full replacement of the
 * entire mapping document). {@link MapperService.MergeReason#MAPPING_RECOVERY} applies the persisted
 * mapping as a whole. Implementation: {@link RootObjectMapper#doMerge}.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class DynamicProperty implements ToXContentObject {

    private final String pattern;
    private final Map<String, Object> mapping;

    public DynamicProperty(String pattern, Map<String, Object> mapping) {
        this.pattern = pattern;
        this.mapping = mapping;
    }

    /** Returns the wildcard pattern (e.g. {@code *_i}, {@code *}). */
    public String getPattern() {
        return pattern;
    }

    /** Returns the mapping configuration for matching fields (e.g. type and options). */
    public Map<String, Object> getMapping() {
        return mapping;
    }

    /**
     * Returns whether the given name matches this dynamic property's pattern.
     * At ingest and query, the name is the <strong>full dotted path</strong> from the mapping root
     * (e.g. {@code parent.count_i}), not only the leaf segment, so patterns may include {@code .} in
     * the matched text. Uses the same simple wildcard matching as dynamic_templates.
     */
    public boolean matches(String fieldName) {
        return Regex.simpleMatch(pattern, fieldName);
    }

    /**
     * Returns a copy of the mapping suitable for parsing a concrete field. {@code fieldName} is the
     * <strong>leaf</strong> segment (e.g. {@code count_i} when the full path is {@code parent.count_i});
     * {@code {name}} placeholders substitute that leaf. Pattern matching uses the full dotted path
     * (see {@link #matches}).
     */
    public Map<String, Object> mappingForName(String fieldName) {
        return processMap(mapping, fieldName);
    }

    /**
     * Deep-copies mapping options, substituting {@code {name}} in each map key and in string values.
     * Nested maps are processed recursively so keys at every level get the same substitution.
     * Non-map, non-string values (numbers, booleans, lists, etc.) are left unchanged.
     */
    private Map<String, Object> processMap(Map<String, Object> map, String fieldName) {
        Map<String, Object> result = new TreeMap<>();
        for (Map.Entry<String, Object> entry : map.entrySet()) {
            String key = entry.getKey().replace("{name}", fieldName);
            Object value = entry.getValue();
            if (value instanceof Map<?, ?> rawNested) {
                Map<String, Object> nested = new TreeMap<>();
                for (Map.Entry<?, ?> e : rawNested.entrySet()) {
                    if ((e.getKey() instanceof String) == false) {
                        throw new IllegalArgumentException(
                            "dynamic_property nested mapping must use string keys; got [" + e.getKey() + "]"
                        );
                    }
                    nested.put((String) e.getKey(), e.getValue());
                }
                value = processMap(nested, fieldName);
            } else if (value instanceof String) {
                value = value.toString().replace("{name}", fieldName);
            }
            result.put(key, value);
        }
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.field(pattern, new TreeMap<>(mapping));
        return builder;
    }
}
