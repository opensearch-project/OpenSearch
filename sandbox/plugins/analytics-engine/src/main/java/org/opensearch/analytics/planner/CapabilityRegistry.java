/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.BackendCapabilityProvider;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.EngineCapability;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ScanCapability;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Pre-indexed capability lookups for planner rules. Built once at plugin startup,
 * shared across all queries. All indexes eagerly constructed in constructor.
 *
 * <p>Single-format lookups return the stored list directly — no allocation at query time.
 * Multi-format aggregations build a new list by collecting across entries.
 *
 * @opensearch.internal
 */
public class CapabilityRegistry {

    private final List<AnalyticsSearchBackendPlugin> backends;
    // O(1) backend lookup by name
    private final Map<String, AnalyticsSearchBackendPlugin> backendsByName = new HashMap<>();

    // Per-capability indexes: (capability key, format) → backends
    // Shape: Map<Key, Map<format, List<backendName>>>
    private final Map<ScanKey, Map<String, List<String>>> scanIndex = new HashMap<>();
    private final Map<ScalarKey, Map<String, List<String>>> filterIndex = new HashMap<>();
    private final Map<AggregateKey, Map<String, List<String>>> aggregateIndex = new HashMap<>();
    private final Map<ScalarKey, Map<String, List<String>>> scalarIndex = new HashMap<>();
    // Backends that declared supportsLiteralEvaluation=true for a (function, fieldType)
    private final Map<ScalarKey, List<String>> literalScalarIndex = new HashMap<>();
    // Opaque operations keyed by name (e.g. "painless") rather than a typed key
    private final Map<String, Map<String, List<String>>> opaqueIndex = new HashMap<>();

    // Non-format-scoped indexes
    private final Map<EngineCapability, List<String>> operatorIndex = new HashMap<>();
    private final Map<DelegationType, List<String>> delegationSupporters = new HashMap<>();
    private final Map<DelegationType, List<String>> delegationAcceptors = new HashMap<>();
    private final Map<FullTextParamKey, Set<String>> fullTextParamIndex = new HashMap<>();

    private final Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory;

    // Backends that declared any capability for each operator — O(1) membership check
    private final Set<String> scanCapableBackends = new HashSet<>();
    private final Set<String> filterCapableBackends = new HashSet<>();
    private final Set<String> aggregateCapableBackends = new HashSet<>();
    private final Set<String> projectCapableBackends = new HashSet<>();

    public CapabilityRegistry(
        List<AnalyticsSearchBackendPlugin> backends,
        Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory
    ) {
        this.backends = backends;
        this.fieldStorageFactory = fieldStorageFactory;
        for (AnalyticsSearchBackendPlugin backend : backends) {
            String name = backend.name();
            backendsByName.put(name, backend);
            BackendCapabilityProvider caps = backend.getCapabilityProvider();

            for (EngineCapability cap : caps.supportedEngineCapabilities()) {
                operatorIndex.computeIfAbsent(cap, k -> new ArrayList<>()).add(name);
            }
            for (DelegationType type : caps.supportedDelegations()) {
                delegationSupporters.computeIfAbsent(type, k -> new ArrayList<>()).add(name);
            }
            for (DelegationType type : caps.acceptedDelegations()) {
                delegationAcceptors.computeIfAbsent(type, k -> new ArrayList<>()).add(name);
            }
            for (ScanCapability cap : caps.scanCapabilities()) {
                for (FieldType fieldType : cap.supportedFieldTypes()) {
                    addToFormatMap(scanIndex, new ScanKey(cap.getClass(), fieldType), cap.formats(), name);
                }
                scanCapableBackends.add(name);
            }
            for (FilterCapability cap : caps.filterCapabilities()) {
                switch (cap) {
                    case FilterCapability.Standard standard -> {
                        for (FieldType fieldType : standard.fieldTypes()) {
                            addToFormatMap(filterIndex, new ScalarKey(standard.function(), fieldType), standard.formats(), name);
                        }
                    }
                    case FilterCapability.FullText fullText -> {
                        addToFormatMap(filterIndex, new ScalarKey(fullText.function(), fullText.fieldType()), fullText.formats(), name);
                        fullTextParamIndex.put(
                            new FullTextParamKey(fullText.function(), fullText.fieldType(), name),
                            fullText.supportedParams()
                        );
                    }
                }
                filterCapableBackends.add(name);
            }
            for (AggregateCapability cap : caps.aggregateCapabilities()) {
                for (FieldType fieldType : cap.fieldTypes()) {
                    addToFormatMap(aggregateIndex, new AggregateKey(cap.function(), fieldType), cap.formats(), name);
                }
                aggregateCapableBackends.add(name);
            }
            for (ProjectCapability cap : caps.projectCapabilities()) {
                switch (cap) {
                    case ProjectCapability.Scalar scalar -> {
                        for (FieldType fieldType : scalar.fieldTypes()) {
                            addToFormatMap(scalarIndex, new ScalarKey(scalar.function(), fieldType), scalar.formats(), name);
                            if (scalar.supportsLiteralEvaluation()) {
                                literalScalarIndex.computeIfAbsent(new ScalarKey(scalar.function(), fieldType), k -> new ArrayList<>())
                                    .add(name);
                            }
                        }
                    }
                    case ProjectCapability.Opaque opaque -> {
                        Map<String, List<String>> formatMap = opaqueIndex.computeIfAbsent(opaque.name(), k -> new HashMap<>());
                        for (String format : opaque.formats()) {
                            formatMap.computeIfAbsent(format, k -> new ArrayList<>()).add(name);
                        }
                    }
                }
                projectCapableBackends.add(name);
            }
        }
    }

    // ---- Operator / delegation lookups ----

    public List<String> operatorBackends(EngineCapability capability) {
        return operatorIndex.getOrDefault(capability, List.of());
    }

    public List<String> delegationSupporters(DelegationType type) {
        return delegationSupporters.getOrDefault(type, List.of());
    }

    public List<String> delegationAcceptors(DelegationType type) {
        return delegationAcceptors.getOrDefault(type, List.of());
    }

    // ---- Capable-backend sets ----

    public Set<String> scanCapableBackends() {
        return scanCapableBackends;
    }

    public Set<String> filterCapableBackends() {
        return filterCapableBackends;
    }

    public Set<String> aggregateCapableBackends() {
        return aggregateCapableBackends;
    }

    public Set<String> projectCapableBackends() {
        return projectCapableBackends;
    }

    // ---- Scan lookups ----

    public List<String> scanBackends(Class<? extends ScanCapability> kind, FieldType fieldType, String format) {
        return scanIndex.getOrDefault(new ScanKey(kind, fieldType), Map.of()).getOrDefault(format, List.of());
    }

    // ---- Single-format lookups ----

    public List<String> filterBackends(ScalarFunction function, FieldType fieldType, String format) {
        return filterIndex.getOrDefault(new ScalarKey(function, fieldType), Map.of()).getOrDefault(format, List.of());
    }

    public List<String> aggregateBackends(AggregateFunction function, FieldType fieldType, String format) {
        return aggregateIndex.getOrDefault(new AggregateKey(function, fieldType), Map.of()).getOrDefault(format, List.of());
    }

    public boolean isOpaqueOperation(String name) {
        return opaqueIndex.containsKey(name);
    }

    // ---- Field-level lookups (iterates all formats a field has) ----

    /** All backends that can filter on this field across all its storage formats. */
    public List<String> filterBackendsForField(ScalarFunction function, FieldStorageInfo field) {
        FieldType fieldType = field.getFieldType();
        List<String> result = new ArrayList<>();
        for (String format : field.getDocValueFormats()) {
            result.addAll(filterBackends(function, fieldType, format));
        }
        for (String format : field.getIndexFormats()) {
            result.addAll(filterBackends(function, fieldType, format));
        }
        return result;
    }

    /** All backends that can aggregate on this field across all its storage formats. */
    public List<String> aggregateBackendsForField(AggregateFunction function, FieldStorageInfo field) {
        FieldType fieldType = field.getFieldType();
        List<String> result = new ArrayList<>();
        for (String format : field.getDocValueFormats()) {
            result.addAll(aggregateBackends(function, fieldType, format));
        }
        return result;
    }

    /** All backends that can scan this field's doc values across all its formats. */
    public List<String> scanBackendsForField(FieldStorageInfo field) {
        FieldType fieldType = field.getFieldType();
        List<String> result = new ArrayList<>();
        for (String format : field.getDocValueFormats()) {
            result.addAll(scanBackends(ScanCapability.DocValues.class, fieldType, format));
        }
        return result;
    }

    // ---- Any-format lookups ----

    public List<String> aggregateBackendsAnyFormat(AggregateFunction function, FieldType fieldType) {
        return allBackends(aggregateIndex.getOrDefault(new AggregateKey(function, fieldType), Map.of()));
    }

    public List<String> scalarBackendsAnyFormat(ScalarFunction function, FieldType fieldType) {
        return allBackends(scalarIndex.getOrDefault(new ScalarKey(function, fieldType), Map.of()));
    }

    /** Backends that declared {@code supportsLiteralEvaluation=true} for this (function, fieldType). */
    public List<String> literalScalarBackends(ScalarFunction function, FieldType fieldType) {
        return literalScalarIndex.getOrDefault(new ScalarKey(function, fieldType), List.of());
    }

    public List<String> opaqueBackendsAnyFormat(String name) {
        return allBackends(opaqueIndex.getOrDefault(name, Map.of()));
    }

    // ---- Annotation handling ----

    /**
     * Whether a candidate backend can handle an annotated expression — either natively
     * (candidate is in the annotation's viable backends) or via delegation (candidate
     * supports delegation and at least one viable backend accepts it).
     */
    public boolean canHandle(String candidate, List<String> annotationViable, DelegationType delegationType) {
        if (annotationViable.contains(candidate)) return true;
        return delegationSupporters(delegationType).contains(candidate)
            && annotationViable.stream().anyMatch(delegationAcceptors(delegationType)::contains);
    }

    // ---- Backend access ----

    public List<AnalyticsSearchBackendPlugin> getBackends() {
        return backends;
    }

    public AnalyticsSearchBackendPlugin getBackend(String name) {
        AnalyticsSearchBackendPlugin backend = backendsByName.get(name);
        if (backend == null) {
            throw new IllegalArgumentException("No backend found with name [" + name + "]");
        }
        return backend;
    }

    public FieldStorageResolver resolveFieldStorage(IndexMetadata indexMetadata) {
        return fieldStorageFactory.apply(indexMetadata);
    }

    // ---- Helpers ----

    private static List<String> allBackends(Map<String, List<String>> formatMap) {
        List<String> result = new ArrayList<>();
        for (List<String> names : formatMap.values()) {
            result.addAll(names);
        }
        return result;
    }

    private static <K> void addToFormatMap(Map<K, Map<String, List<String>>> index, K key, Set<String> formats, String backendName) {
        Map<String, List<String>> formatMap = index.computeIfAbsent(key, k -> new HashMap<>());
        for (String format : formats) {
            formatMap.computeIfAbsent(format, k -> new ArrayList<>()).add(backendName);
        }
    }

    // ---- Keys ----

    private record ScanKey(Class<? extends ScanCapability> kind, FieldType fieldType) {
    }

    private record AggregateKey(AggregateFunction function, FieldType fieldType) {
    }

    private record ScalarKey(ScalarFunction function, FieldType fieldType) {
    }

    private record FullTextParamKey(ScalarFunction function, FieldType fieldType, String backendName) {
    }
}
