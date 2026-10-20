/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.action.support.IndicesOptions;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.AliasMetadata;
import org.opensearch.cluster.metadata.IndexAbstraction;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.IndexNameExpressionResolver;
import org.opensearch.cluster.metadata.MappingMetadata;
import org.opensearch.core.common.Strings;
import org.opensearch.index.IndexNotFoundException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.SortedMap;

/**
 * Resolves a table name (concrete index or alias) to the list of {@link IndexMetadata}
 * behind it, validating mapping compatibility across all backing indices.
 *
 * <p>Used by the planner where a {@code TableScan}'s qualified name needs to be expanded
 * to the concrete indices that will actually serve the query. A concrete-index name returns
 * a singleton list; an alias returns the list of backing indices after validating that
 * every field present in any backing index has the same declared type in all others.
 *
 * <p>Aliases that declare a filter are rejected — the planner does not weave alias filters
 * into the query plan today, so honoring such an alias would silently return more rows than
 * the user asked for. Data streams are likewise rejected for now.
 *
 * <p>Index expressions (comma lists, wildcards, exclusions, date-math) are resolved through the
 * cluster's {@link IndexNameExpressionResolver} and unioned, with the same schema-compatibility
 * check applied across all matches.
 *
 * @opensearch.internal
 */
public final class IndexResolution {

    private final String requestedName;
    private final List<IndexMetadata> concreteIndices;

    private IndexResolution(String requestedName, List<IndexMetadata> concreteIndices) {
        this.requestedName = requestedName;
        this.concreteIndices = List.copyOf(concreteIndices);
    }

    /** The name the caller asked to resolve — either a concrete index or an alias. */
    public String requestedName() {
        return requestedName;
    }

    /** Backing concrete indices. Always size ≥ 1 for a successful resolution. */
    public List<IndexMetadata> concreteIndices() {
        return concreteIndices;
    }

    public List<String> concreteIndexNames() {
        return concreteIndices.stream().map(im -> im.getIndex().getName()).toList();
    }

    /** Sum of {@code getNumberOfShards()} across all backing indices. */
    public int totalShardCount() {
        return concreteIndices.stream().mapToInt(IndexMetadata::getNumberOfShards).sum();
    }

    /**
     * Convenience overload for literal names (concrete index or alias) and test contexts.
     * Wildcard/comma expressions require the {@link #resolve(String, ClusterState,
     * IndexNameExpressionResolver)} overload — without a resolver they fall through to the
     * not-found error.
     */
    public static IndexResolution resolve(String name, ClusterState clusterState) {
        return resolve(name, clusterState, null);
    }

    /**
     * Resolves {@code name} against the cluster state. Literal concrete indices and aliases are
     * resolved directly (with filter-alias rejection and cross-member schema validation). Wildcard,
     * comma-separated, exclusion, and date-math expressions are delegated to the supplied
     * {@link IndexNameExpressionResolver} — the same canonical resolution every other search path
     * uses, so analytics honors {@code IndicesOptions}, hidden/system-index rules, exclusions, and
     * security index filtering rather than a bespoke matcher.
     *
     * <p>Throws on missing names, filter aliases, data streams, and schema mismatches across the
     * resolved members.
     */
    public static IndexResolution resolve(String name, ClusterState clusterState, IndexNameExpressionResolver resolver) {
        SortedMap<String, IndexAbstraction> lookup = clusterState.metadata().getIndicesLookup();
        IndexAbstraction abstraction = lookup == null ? null : lookup.get(name);
        if (abstraction != null) {
            return switch (abstraction.getType()) {
                case CONCRETE_INDEX -> resolveConcrete(name, abstraction.getIndices());
                case ALIAS -> resolveAlias(name, abstraction.getIndices());
                case DATA_STREAM -> resolveDataStream(name, abstraction.getIndices());
            };
        }
        // Not a literal name: treat as an index expression and resolve through the canonical
        // OpenSearch resolver so the concrete set matches every other search path. Schema
        // compatibility across the resolved union is validated just like an alias.
        if (resolver != null) {
            // Comma-split before resolving: concreteIndexNames treats each vararg as one
            // expression (the REST layer normally splits first), so a single "a,b" string would
            // otherwise be read as a literal name. Splitting also lets the resolver honor
            // exclusions across tokens (e.g. "test*,-test1").
            String[] expressions = Strings.splitStringByCommaToArray(name);
            String[] concrete;
            try {
                // includeDataStreams=true so wildcards and comma-lists matching a data stream NAME
                // expand to its backings — the resolver excludes data streams from wildcard
                // expansion otherwise. Literal data stream names take the abstraction short-circuit
                // above (resolveDataStream) and skip the resolver entirely.
                concrete = resolver.concreteIndexNames(clusterState, IndicesOptions.lenientExpandOpen(), true, expressions);
            } catch (IndexNotFoundException e) {
                throw new IllegalArgumentException("Index or alias [" + name + "] not found in cluster state", e);
            }
            List<IndexMetadata> indices = new ArrayList<>(concrete.length);
            for (String concreteName : concrete) {
                IndexMetadata index = clusterState.metadata().index(concreteName);
                if (index != null) {
                    indices.add(index);
                }
            }
            if (!indices.isEmpty()) {
                validateSchemaCompatibility(name, indices);
                return new IndexResolution(name, indices);
            }
        }
        // Fallback: some test contexts stub {@code metadata().index(name)} without populating
        // {@code getIndicesLookup()}. Treat that as a concrete-index hit so this helper is a
        // drop-in replacement for the prior {@code metadata().index(name)} lookup in those
        // contexts. Real cluster state always populates the lookup, so production resolution
        // (including aliases) flows through the branch above.
        IndexMetadata direct = clusterState.metadata().index(name);
        if (direct != null) {
            return new IndexResolution(name, List.of(direct));
        }
        throw new IllegalArgumentException("Index or alias [" + name + "] not found in cluster state");
    }

    private static IndexResolution resolveConcrete(String name, List<IndexMetadata> backing) {
        // Reject closed indices on the literal-name path to match the alias path's behavior
        // (which uses lenientExpandOpen). The wildcard / expression path resolves through the
        // canonical OpenSearch resolver, which also excludes closed indices — so all three paths
        // converge on "closed indices are not searchable through analytics-engine".
        for (IndexMetadata index : backing) {
            if (index.getState() != IndexMetadata.State.OPEN) {
                throw new IllegalArgumentException("Index [" + name + "] is closed");
            }
        }
        return new IndexResolution(name, backing);
    }

    private static IndexResolution resolveDataStream(String dataStreamName, List<IndexMetadata> backing) {
        // Same closed-filter and schema-compat semantics as aliases. Data stream backings are
        // managed by the rollover lifecycle and should have identical mappings, but manual
        // mapping amendments can drift — so validate. Skip the filter-aliases check (irrelevant
        // for data streams) and the per-error wording mentions "data stream" for clarity.
        List<IndexMetadata> open = new ArrayList<>(backing.size());
        for (IndexMetadata index : backing) {
            if (index.getState() == IndexMetadata.State.OPEN) {
                open.add(index);
            }
        }
        if (open.isEmpty()) {
            throw new IllegalArgumentException("Data stream [" + dataStreamName + "] resolves only to closed indices");
        }
        validateSchemaCompatibility(dataStreamName, open);
        return new IndexResolution(dataStreamName, open);
    }

    private static IndexResolution resolveAlias(String aliasName, List<IndexMetadata> backing) {
        // Skip closed members — they cannot serve a search, and the wildcard/expression path
        // (lenientExpandOpen) already excludes them, so the alias path must match.
        List<IndexMetadata> open = new ArrayList<>(backing.size());
        for (IndexMetadata index : backing) {
            if (index.getState() == IndexMetadata.State.OPEN) {
                open.add(index);
            }
        }
        if (open.isEmpty()) {
            throw new IllegalArgumentException("Alias [" + aliasName + "] resolves only to closed indices");
        }
        // Filter aliases would silently narrow the result set — reject up-front because the
        // planner does not weave the filter into the Calcite plan.
        for (IndexMetadata index : open) {
            AliasMetadata aliasMd = index.getAliases().get(aliasName);
            if (aliasMd != null && aliasMd.filteringRequired()) {
                throw new IllegalArgumentException(
                    "Alias ["
                        + aliasName
                        + "] declares a filter on index ["
                        + index.getIndex().getName()
                        + "]; "
                        + "filter aliases are not yet supported by analytics queries"
                );
            }
        }
        // Schema-compat check: every field that appears in any backing index must have the same
        // declared type across all that mention it. Differs from OpenSearch core's per-shard
        // tolerance — analytics queries plan once against a single row type, so divergence has
        // to be caught before planning.
        validateSchemaCompatibility(aliasName, open);
        return new IndexResolution(aliasName, open);
    }

    @SuppressWarnings("unchecked")
    private static void validateSchemaCompatibility(String aliasName, List<IndexMetadata> backing) {
        // Walks only top-level "properties" — a conflict on a nested object's leaf (e.g. a.b long
        // vs keyword) is not caught here and is left to the data node's by-name binding check.
        // field name → (declared type, first index that declared it)
        record Decl(String type, String sourceIndex) {
        }
        java.util.Map<String, Decl> declared = new java.util.LinkedHashMap<>();
        for (IndexMetadata index : backing) {
            MappingMetadata mm = index.mapping();
            if (mm == null) {
                continue;
            }
            Map<String, Object> source = mm.sourceAsMap();
            Object props = source.get("properties");
            if (!(props instanceof Map<?, ?> propsMap)) {
                continue;
            }
            for (Map.Entry<?, ?> entry : propsMap.entrySet()) {
                String fieldName = String.valueOf(entry.getKey());
                Object fieldDef = entry.getValue();
                if (!(fieldDef instanceof Map<?, ?> fieldMap)) {
                    continue;
                }
                String type = Objects.toString(fieldMap.get("type"), null);
                if (type == null) {
                    continue;
                }
                Decl previous = declared.get(fieldName);
                if (previous == null) {
                    declared.put(fieldName, new Decl(type, index.getIndex().getName()));
                    // Compare the mapped Calcite type, not the raw OpenSearch type string: the schema
                    // builder collapses text/keyword → VARCHAR, long/scaled_float → BIGINT, etc., so a
                    // union over those is compatible. Two unsupported types both map to null (dropped
                    // from the schema) and are likewise treated as compatible.
                } else if (!Objects.equals(
                    OpenSearchSchemaBuilder.mapFieldType(previous.type),
                    OpenSearchSchemaBuilder.mapFieldType(type)
                )) {
                    throw new IllegalArgumentException(
                        "Alias ["
                            + aliasName
                            + "] resolves to indices with incompatible field types: ["
                            + fieldName
                            + "] is ["
                            + previous.type
                            + "] in ["
                            + previous.sourceIndex
                            + "] but ["
                            + type
                            + "] in ["
                            + index.getIndex().getName()
                            + "]"
                    );
                }
            }
        }
    }

    @Override
    public String toString() {
        return "IndexResolution{requested=" + requestedName + ", concrete=" + concreteIndexNames() + "}";
    }
}
