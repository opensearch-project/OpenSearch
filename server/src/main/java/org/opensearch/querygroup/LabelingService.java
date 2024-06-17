/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.querygroup;

import org.opensearch.action.IndicesRequest;
import org.opensearch.common.util.concurrent.ThreadContext;
import org.opensearch.plugins.LabelingPlugin;

import java.util.EnumMap;
import java.util.List;

/**
 * TODO: Don't know the right package to put this class, need suggestions for maintainers on this
 * Main class to hold multiple implementations of {@link org.opensearch.plugins.LabelingPlugin}
 * this class will facilitate access and interactions to different implementations
 * Usage: This class should be used as a member to orchestrate the working of your {@link LabelingPlugin}
 */
public class LabelingService {
    /**
     * Enum to define what all are currently implementing the plugin
     */
    public enum LabelingImplementationType {
        QUERY_GROUP_RESOURCE_MANAGEMENT,
        NOOP
    }

    EnumMap<LabelingImplementationType, LabelingPlugin> implementations;

    public LabelingService(List<LabelingPlugin> loadedPlugins) {
        implementations = new EnumMap<>(LabelingImplementationType.class);
        for (LabelingPlugin plugin : loadedPlugins) {
            if (implementations.containsKey(plugin.getImplementationName())) {
                throw new IllegalArgumentException("There should not be two implementations of a LabelingImplementation type");
            }
            implementations.put(plugin.getImplementationName(), plugin);
        }
    }

    /**
     * populates the threadContext with the labels yielded by the {@param type} against the {@link LabelingHeader} keys
     * @param type
     * @param request
     * @param threadContext
     */
    public void labelRequestFor(final LabelingImplementationType type, final IndicesRequest request, final ThreadContext threadContext) {
        final LabelingPlugin plugin = implementations.get(type);
        if (plugin == null) {
            throw new IllegalArgumentException(type + " implementation is not enabled");
        }
        plugin.labelRequest(request, threadContext);
    }
}
