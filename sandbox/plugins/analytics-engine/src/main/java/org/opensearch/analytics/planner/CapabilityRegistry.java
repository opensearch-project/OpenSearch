/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import org.opensearch.analytics.planner.rel.ShuffleImpl;
import org.opensearch.analytics.spi.AggregateCapability;
import org.opensearch.analytics.spi.AggregateFunction;
import org.opensearch.analytics.spi.AnalyticsSearchBackendPlugin;
import org.opensearch.analytics.spi.DelegationType;
import org.opensearch.analytics.spi.FieldType;
import org.opensearch.analytics.spi.FilterCapability;
import org.opensearch.analytics.spi.FilterOperator;
import org.opensearch.analytics.spi.OperatorCapability;
import org.opensearch.analytics.spi.ProjectCapability;
import org.opensearch.analytics.spi.ScalarFunction;
import org.opensearch.analytics.spi.ShuffleCapability;
import org.opensearch.analytics.spi.WindowCapability;
import org.opensearch.analytics.spi.WindowFunction;
import org.opensearch.cluster.metadata.IndexMetadata;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Function;

/**
 * Pre-indexed capability lookups for planner rules. Built once at plugin startup,
 * shared across all queries. All indexes eagerly constructed in constructor.
 *
 * <p>Single-format lookups return precomputed lists — no allocation, no iteration
 * at query time. Callers collect across multiple formats when needed.
 *
 * @opensearch.internal
 */
public class CapabilityRegistry {

    private final List<AnalyticsSearchBackendPlugin> backends;
    private final Map<FilterKey, Map<String, List<String>>> filterIndex = new HashMap<>();
    private final Map<AggregateKey, Map<String, List<String>>> aggregateIndex = new HashMap<>();
    private final Map<ScalarKey, Map<String, List<String>>> scalarIndex = new HashMap<>();
    private final Map<WindowKey, Map<String, List<String>>> windowIndex = new HashMap<>();
    private final Map<String, Map<String, List<String>>> opaqueIndex = new HashMap<>();
    private final Map<OperatorCapability, List<String>> operatorIndex = new HashMap<>();
    private final Map<DelegationType, List<String>> delegationSupporters = new HashMap<>();
    private final Map<DelegationType, List<String>> delegationAcceptors = new HashMap<>();
    private final Map<FullTextParamKey, Set<String>> fullTextParamIndex = new HashMap<>();
    // format → [backends that support SCAN for this format]
    private final Map<String, List<String>> scanFormatIndex = new HashMap<>();
    // backendName → ShuffleCapabilities
    private final Map<String, Set<ShuffleCapability>> shuffleCapabilities = new HashMap<>();
    // (backendName, OperatorCapability) → can operate on Arrow from another backend
    private final Map<String, Set<OperatorCapability>> arrowCompatibleIndex = new HashMap<>();
    private final Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory;

    public CapabilityRegistry(List<AnalyticsSearchBackendPlugin> backends,
                              Function<IndexMetadata, FieldStorageResolver> fieldStorageFactory,
                              Map<String, List<String>> scanFormats) {
        this.backends = backends;
        this.fieldStorageFactory = fieldStorageFactory;
        this.scanFormatIndex.putAll(scanFormats);
        for (AnalyticsSearchBackendPlugin backend : backends) {
            String name = backend.name();

            for (OperatorCapability cap : backend.supportedOperators()) {
                operatorIndex.computeIfAbsent(cap, k -> new ArrayList<>()).add(name);
            }
            for (DelegationType type : backend.supportedDelegations()) {
                delegationSupporters.computeIfAbsent(type, k -> new ArrayList<>()).add(name);
            }
            for (DelegationType type : backend.acceptedDelegations()) {
                delegationAcceptors.computeIfAbsent(type, k -> new ArrayList<>()).add(name);
            }

            for (FilterCapability cap : backend.filterCapabilities()) {
                switch (cap) {
                    case FilterCapability.Standard standard -> addToFormatMap(filterIndex,
                        new FilterKey(standard.operator(), standard.fieldType()),
                        standard.formats(), name);
                    case FilterCapability.FullText fullText -> {
                        addToFormatMap(filterIndex,
                            new FilterKey(fullText.operator(), fullText.fieldType()),
                            fullText.formats(), name);
                        fullTextParamIndex.put(
                            new FullTextParamKey(fullText.operator(), fullText.fieldType(), name),
                            fullText.supportedParams());
                    }
                    case FilterCapability.Expression expression -> addToFormatMap(filterIndex,
                        new FilterKey(FilterOperator.EXPRESSION, null),
                        expression.formats(), name);
                }
            }

            for (AggregateCapability cap : backend.aggregateCapabilities()) {
                addToFormatMap(aggregateIndex, new AggregateKey(cap.function(), cap.fieldType()),
                    cap.formats(), name);
            }

            for (ProjectCapability cap : backend.projectCapabilities()) {
                switch (cap) {
                    case ProjectCapability.Scalar scalar -> addToFormatMap(scalarIndex,
                        new ScalarKey(scalar.function(), scalar.fieldType()),
                        scalar.formats(), name);
                    case ProjectCapability.Opaque opaque -> {
                        Map<String, List<String>> formatMap = opaqueIndex.computeIfAbsent(
                            opaque.name(), k -> new HashMap<>());
                        for (String format : opaque.formats()) {
                            formatMap.computeIfAbsent(format, k -> new ArrayList<>()).add(name);
                        }
                    }
                }
            }

            for (WindowCapability cap : backend.windowCapabilities()) {
                addToFormatMap(windowIndex, new WindowKey(cap.function(), cap.fieldType()),
                    cap.formats(), name);
            }

            // Scan format index — populated later via setStorageBackends()

            // Shuffle capabilities
            if (!backend.supportedShuffleCapabilities().isEmpty()) {
                shuffleCapabilities.put(name, backend.supportedShuffleCapabilities());
            }
            // Arrow-compatible operators
            if (!backend.arrowCompatibleOperators().isEmpty()) {
                arrowCompatibleIndex.put(name, backend.arrowCompatibleOperators());
            }
        }
    }

    // ---- Eager lookups ----

    public List<String> operatorBackends(OperatorCapability capability) {
        return operatorIndex.getOrDefault(capability, List.of());
    }

    public List<String> delegationSupporters(DelegationType type) {
        return delegationSupporters.getOrDefault(type, List.of());
    }

    public List<String> delegationAcceptors(DelegationType type) {
        return delegationAcceptors.getOrDefault(type, List.of());
    }

    // ---- Single-format lookups (no allocation) ----

    public List<String> filterBackends(FilterOperator operator, FieldType fieldType, String format) {
        Map<String, List<String>> formatMap = filterIndex.getOrDefault(
            new FilterKey(operator, fieldType), Map.of());
        return formatMap.getOrDefault(format, List.of());
    }

    public List<String> aggregateBackends(AggregateFunction function, FieldType fieldType, String format) {
        Map<String, List<String>> formatMap = aggregateIndex.getOrDefault(
            new AggregateKey(function, fieldType), Map.of());
        return formatMap.getOrDefault(format, List.of());
    }

    public List<String> scalarBackends(ScalarFunction function, FieldType fieldType, String format) {
        Map<String, List<String>> formatMap = scalarIndex.getOrDefault(
            new ScalarKey(function, fieldType), Map.of());
        return formatMap.getOrDefault(format, List.of());
    }

    public List<String> windowBackends(WindowFunction function, FieldType fieldType, String format) {
        Map<String, List<String>> formatMap = windowIndex.getOrDefault(
            new WindowKey(function, fieldType), Map.of());
        return formatMap.getOrDefault(format, List.of());
    }

    public List<String> opaqueBackends(String name, String format) {
        Map<String, List<String>> formatMap = opaqueIndex.getOrDefault(name, Map.of());
        return formatMap.getOrDefault(format, List.of());
    }

    /** Checks supported params for a full-text filter backend. */
    public Set<String> fullTextParams(FilterOperator operator, FieldType fieldType, String backendName) {
        return fullTextParamIndex.getOrDefault(
            new FullTextParamKey(operator, fieldType, backendName), Set.of());
    }

    public boolean isOpaqueOperation(String name) {
        return opaqueIndex.containsKey(name);
    }

    /** Backends that support SCAN for the given format. */
    public List<String> scanBackends(String format) {
        return scanFormatIndex.getOrDefault(format, List.of());
    }

    /** Backends viable for scan across any of the given formats. */
    public List<String> scanBackends(List<String> formats) {
        List<String> result = new ArrayList<>();
        for (String format : formats) {
            for (String name : scanBackends(format)) {
                if (!result.contains(name)) {
                    result.add(name);
                }
            }
        }
        return result;
    }

    /** Returns shuffle capabilities for a backend, or empty set if none. */
    public Set<ShuffleCapability> getShuffleCapabilities(String backendName) {
        return shuffleCapabilities.getOrDefault(backendName, Set.of());
    }

    /** Whether the backend can execute this operator on Arrow batches from another backend's output. */
    public boolean isArrowCompatible(String backendName, OperatorCapability operator) {
        return arrowCompatibleIndex.getOrDefault(backendName, Set.of()).contains(operator);
    }

    /** Returns the analytics backends. */
    public List<AnalyticsSearchBackendPlugin> getBackends() {
        return backends;
    }

    /** Builds a FieldStorageResolver for the given index. */
    public FieldStorageResolver resolveFieldStorage(IndexMetadata indexMetadata) {
        return fieldStorageFactory.apply(indexMetadata);
    }

    /** All backends that support this filter operator on this field type, any format. */
    public List<String> filterBackendsAnyFormat(FilterOperator operator, FieldType fieldType) {
        return allBackends(filterIndex.getOrDefault(new FilterKey(operator, fieldType), Map.of()));
    }

    /** All backends that support this aggregate function on this field type, any format. */
    public List<String> aggregateBackendsAnyFormat(AggregateFunction function, FieldType fieldType) {
        return allBackends(aggregateIndex.getOrDefault(new AggregateKey(function, fieldType), Map.of()));
    }

    /** All backends that support this scalar function on this field type, any format. */
    public List<String> scalarBackendsAnyFormat(ScalarFunction function, FieldType fieldType) {
        Map<String, List<String>> formatMap = scalarIndex.getOrDefault(
            new ScalarKey(function, fieldType), Map.of());
        return allBackends(formatMap);
    }

    /** All backends that support this opaque operation in any format. For project rule. */
    public List<String> opaqueBackendsAnyFormat(String name) {
        Map<String, List<String>> formatMap = opaqueIndex.getOrDefault(name, Map.of());
        return allBackends(formatMap);
    }

    /** Collects unique backend names across all formats in a format map. */
    private static List<String> allBackends(Map<String, List<String>> formatMap) {
        List<String> result = new ArrayList<>();
        for (List<String> backends : formatMap.values()) {
            for (String name : backends) {
                if (!result.contains(name)) {
                    result.add(name);
                }
            }
        }
        return result;
    }

    // ---- Helpers ----

    private static <K> void addToFormatMap(Map<K, Map<String, List<String>>> index,
                                            K key, Set<String> formats, String backendName) {
        Map<String, List<String>> formatMap = index.computeIfAbsent(key, k -> new HashMap<>());
        for (String format : formats) {
            formatMap.computeIfAbsent(format, k -> new ArrayList<>()).add(backendName);
        }
    }

    // ---- Keys ----

    private record FilterKey(FilterOperator operator, FieldType fieldType) {}
    private record AggregateKey(AggregateFunction function, FieldType fieldType) {}
    private record ScalarKey(ScalarFunction function, FieldType fieldType) {}
    private record WindowKey(WindowFunction function, FieldType fieldType) {}
    private record FullTextParamKey(FilterOperator operator, FieldType fieldType, String backendName) {}
}
