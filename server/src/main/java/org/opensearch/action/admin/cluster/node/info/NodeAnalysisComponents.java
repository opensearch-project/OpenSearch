/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.node.info;

import org.opensearch.common.collect.Tuple;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.service.ReportingService;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.index.analysis.AnalysisRegistry;
import org.opensearch.plugins.AnalysisPlugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.plugins.PluginsService;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import static java.util.Collections.unmodifiableList;
import static java.util.Collections.unmodifiableSortedSet;

/**
 * Information about node analysis components.
 *
 * Every node in the cluster contains several analysis components. Some are preconfigured, the rest come from
 * {@link AnalysisPlugin}s installed on this node (such as org.opensearch.analysis.common.CommonAnalysisModulePlugin).
 *
 * @see org.opensearch.index.analysis.AnalysisRegistry
 * @see org.opensearch.indices.analysis.AnalysisModule
 *
 * @opensearch.internal
 * @opensearch.experimental
 */
public class NodeAnalysisComponents implements ReportingService.Info {

    private final SortedSet<String> analyzersIds;

    private final SortedSet<String> tokenizersIds;

    private final SortedSet<String> tokenFiltersIds;

    private final SortedSet<String> charFiltersIds;

    private final SortedSet<String> normalizersIds;

    private final List<NodeAnalysisComponents.AnalysisPluginComponents> nodeAnalysisPlugins;

    public SortedSet<String> getAnalyzersIds() {
        return this.analyzersIds;
    }

    public SortedSet<String> getTokenizersIds() {
        return this.tokenizersIds;
    }

    public SortedSet<String> getTokenFiltersIds() {
        return this.tokenFiltersIds;
    }

    public SortedSet<String> getCharFiltersIds() {
        return this.charFiltersIds;
    }

    public SortedSet<String> getNormalizersIds() {
        return this.normalizersIds;
    }

    public List<NodeAnalysisComponents.AnalysisPluginComponents> getNodeAnalysisPlugins() {
        return nodeAnalysisPlugins;
    }

    public NodeAnalysisComponents(AnalysisRegistry analysisRegistry, PluginsService pluginsService) {
        List<NodeAnalysisComponents.AnalysisPluginComponents> nodeAnalysisPlugins = new ArrayList<>();
        List<Tuple<PluginInfo, AnalysisPlugin>> analysisPlugins = pluginsService.filterPluginsForPluginInfo(AnalysisPlugin.class);
        for (Tuple<PluginInfo, AnalysisPlugin> plugin : analysisPlugins) {
            nodeAnalysisPlugins.add(
                new NodeAnalysisComponents.AnalysisPluginComponents(
                    plugin.v1().getName(),
                    plugin.v1().getClassname(),
                    plugin.v2().getAnalyzers().keySet(),
                    plugin.v2().getTokenizers().keySet(),
                    plugin.v2().getTokenFilters().keySet(),
                    plugin.v2().getCharFilters().keySet(),
                    plugin.v2().getHunspellDictionaries().keySet()
                )
            );
        }
        this.analyzersIds = ensureSorted(analysisRegistry.getNodeAnalyzersKeys());
        this.tokenizersIds = ensureSorted(analysisRegistry.getNodeTokenizersKeys());
        this.tokenFiltersIds = ensureSorted(analysisRegistry.getNodeTokenFiltersKeys());
        this.charFiltersIds = ensureSorted(analysisRegistry.getNodeCharFiltersKeys());
        this.normalizersIds = ensureSorted(analysisRegistry.getNodeNormalizersKeys());
        this.nodeAnalysisPlugins = ensureNonEmpty(nodeAnalysisPlugins);
    }

    public NodeAnalysisComponents(
        final Set<String> analyzersKeySet,
        final Set<String> tokenizersKeySet,
        final Set<String> tokenFiltersKeySet,
        final Set<String> charFiltersKeySet,
        final Set<String> normalizersKeySet,
        final List<NodeAnalysisComponents.AnalysisPluginComponents> nodeAnalysisPlugins
    ) {
        this.analyzersIds = ensureSorted(analyzersKeySet);
        this.tokenizersIds = ensureSorted(tokenizersKeySet);
        this.tokenFiltersIds = ensureSorted(tokenFiltersKeySet);
        this.charFiltersIds = ensureSorted(charFiltersKeySet);
        this.normalizersIds = ensureSorted(normalizersKeySet);
        this.nodeAnalysisPlugins = ensureNonEmpty(nodeAnalysisPlugins);
    }

    /**
     * This class represents analysis components provided by {@link org.opensearch.plugins.AnalysisPlugin}.
     * There can be several plugins (or modules) installed on each cluster node.
     */
    public static class AnalysisPluginComponents implements Comparable<NodeAnalysisComponents.AnalysisPluginComponents>, Writeable {

        private final String pluginName;
        private final String className;
        private final SortedSet<String> analyzersIds;
        private final SortedSet<String> tokenizersIds;
        private final SortedSet<String> tokenFiltersIds;
        private final SortedSet<String> charFiltersIds;
        private final SortedSet<String> hunspellDictionaries;

        public AnalysisPluginComponents(
            final String pluginName,
            final String className,
            final Set<String> analyzersIds,
            final Set<String> tokenizersIds,
            final Set<String> tokenFiltersIds,
            final Set<String> charFiltersIds,
            final Set<String> hunspellDictionaries
        ) {
            this.pluginName = pluginName;
            this.className = className;
            this.analyzersIds = unmodifiableSortedSet(new TreeSet<>(analyzersIds));
            this.tokenizersIds = unmodifiableSortedSet(new TreeSet<>(tokenizersIds));
            this.tokenFiltersIds = unmodifiableSortedSet(new TreeSet<>(tokenFiltersIds));
            this.charFiltersIds = unmodifiableSortedSet(new TreeSet<>(charFiltersIds));
            this.hunspellDictionaries = unmodifiableSortedSet(new TreeSet<>(hunspellDictionaries));
        }

        public AnalysisPluginComponents(StreamInput in) throws IOException {
            this.pluginName = in.readString();
            this.className = in.readString();
            this.analyzersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            this.tokenizersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            this.tokenFiltersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            this.charFiltersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
            this.hunspellDictionaries = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        }

        public void writeTo(StreamOutput out) throws IOException {
            out.writeString(this.pluginName);
            out.writeString(this.className);
            out.writeStringCollection(this.analyzersIds);
            out.writeStringCollection(this.tokenizersIds);
            out.writeStringCollection(this.tokenFiltersIds);
            out.writeStringCollection(this.charFiltersIds);
            out.writeStringCollection(this.hunspellDictionaries);
        }

        private static final Comparator<String> nullSafeStringComparator = Comparator.nullsFirst(String::compareTo);

        private static String concatenateItems(SortedSet<String> items) {
            return items.stream().collect(Collectors.joining());
        }

        /**
         * This Comparator defines the comparison logic for sorting instances of AnalysisPluginComponents based on
         * their attributes in the following order:
         *
         * 1. Plugin name (as specified in the plugin descriptor)
         * 2. Class name
         * 3. Analyzers IDs
         * 4. Tokenizers IDs
         * 5. TokenFilters IDs
         * 6. CharFilters IDs
         * 7. Hunspell dictionary IDs
         */
        private static final Comparator<NodeAnalysisComponents.AnalysisPluginComponents> pluginComponentsComparator = Comparator.comparing(
            AnalysisPluginComponents::getPluginName,
            nullSafeStringComparator
        )
            .thenComparing(AnalysisPluginComponents::getClassName, nullSafeStringComparator)
            .thenComparing(c -> concatenateItems(c.getAnalyzersIds()), nullSafeStringComparator)
            .thenComparing(c -> concatenateItems(c.getTokenizersIds()), nullSafeStringComparator)
            .thenComparing(c -> concatenateItems(c.getTokenFiltersIds()), nullSafeStringComparator)
            .thenComparing(c -> concatenateItems(c.getCharFiltersIds()), nullSafeStringComparator)
            .thenComparing(c -> concatenateItems(c.getHunspellDictionaries()), nullSafeStringComparator);

        @Override
        public int compareTo(NodeAnalysisComponents.AnalysisPluginComponents o) {
            return pluginComponentsComparator.compare(this, o);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) return true;
            if (o == null || getClass() != o.getClass()) return false;
            AnalysisPluginComponents that = (AnalysisPluginComponents) o;
            return Objects.equals(pluginName, that.pluginName)
                && Objects.equals(className, that.className)
                && Objects.equals(analyzersIds, that.analyzersIds)
                && Objects.equals(tokenizersIds, that.tokenizersIds)
                && Objects.equals(tokenFiltersIds, that.tokenFiltersIds)
                && Objects.equals(charFiltersIds, that.charFiltersIds)
                && Objects.equals(hunspellDictionaries, that.hunspellDictionaries);
        }

        @Override
        public int hashCode() {
            return Objects.hash(pluginName, className, analyzersIds, tokenizersIds, tokenFiltersIds, charFiltersIds, hunspellDictionaries);
        }

        public String getPluginName() {
            return this.pluginName;
        }

        public String getClassName() {
            return this.className;
        }

        public SortedSet<String> getAnalyzersIds() {
            return this.analyzersIds;
        }

        public SortedSet<String> getTokenizersIds() {
            return this.tokenizersIds;
        }

        public SortedSet<String> getTokenFiltersIds() {
            return this.tokenFiltersIds;
        }

        public SortedSet<String> getCharFiltersIds() {
            return this.charFiltersIds;
        }

        public SortedSet<String> getHunspellDictionaries() {
            return this.hunspellDictionaries;
        }
    }

    public NodeAnalysisComponents(StreamInput in) throws IOException {
        this.analyzersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        this.tokenizersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        this.tokenFiltersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        this.charFiltersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        this.normalizersIds = unmodifiableSortedSet(new TreeSet<>(in.readSet(StreamInput::readString)));
        this.nodeAnalysisPlugins = unmodifiableList(in.readList(NodeAnalysisComponents.AnalysisPluginComponents::new));
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeStringCollection(this.analyzersIds);
        out.writeStringCollection(this.tokenizersIds);
        out.writeStringCollection(this.tokenFiltersIds);
        out.writeStringCollection(this.charFiltersIds);
        out.writeStringCollection(this.normalizersIds);
        out.writeList(this.nodeAnalysisPlugins);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("analysis_components");
        builder.field("analyzers").value(this.getAnalyzersIds());
        builder.field("tokenizers").value(this.getTokenizersIds());
        builder.field("tokenFilters").value(this.getTokenFiltersIds());
        builder.field("charFilters").value(this.getCharFiltersIds());
        builder.field("normalizers").value(this.getNormalizersIds());
        builder.startArray("plugins");
        for (NodeAnalysisComponents.AnalysisPluginComponents pluginComponents : this.getNodeAnalysisPlugins()) {
            builder.startObject();
            builder.field("name", pluginComponents.getPluginName());
            builder.field("classname", pluginComponents.getClassName());
            builder.field("analyzers").value(pluginComponents.getAnalyzersIds());
            builder.field("tokenizers").value(pluginComponents.getTokenizersIds());
            builder.field("tokenFilters").value(pluginComponents.getTokenFiltersIds());
            builder.field("charFilters").value(pluginComponents.getCharFiltersIds());
            builder.field("hunspellDictionaries").value(pluginComponents.getHunspellDictionaries());
            builder.endObject();
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    public int hashCode() {
        return Objects.hash(analyzersIds, tokenizersIds, tokenFiltersIds, charFiltersIds, normalizersIds, nodeAnalysisPlugins);
    }

    /**
     * Ensures that a given set of strings is sorted in "natural" order.
     *
     * See: {@link SortedSet}
     */
    private static SortedSet<String> ensureSorted(Set<String> stringSet) {
        return stringSet == null ? Collections.emptySortedSet() : unmodifiableSortedSet(new TreeSet<>(stringSet));
    }

    private static List<NodeAnalysisComponents.AnalysisPluginComponents> ensureNonEmpty(
        List<NodeAnalysisComponents.AnalysisPluginComponents> pluginComponents
    ) {
        return pluginComponents == null ? Collections.emptyList() : unmodifiableList(pluginComponents);
    }
}
