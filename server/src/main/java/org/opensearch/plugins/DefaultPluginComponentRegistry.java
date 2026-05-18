/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

/**
 * Default implementation of {@link PluginComponentRegistry} backed by an ordered list of
 * components. Components are registered as plugins are initialized (in dependency order),
 * so lookups from later plugins see all components from earlier ones.
 *
 * <p>This registry is only valid during plugin initialization. After all plugins have been
 * initialized, {@link #seal()} should be called to release internal references and prevent
 * further use.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public class DefaultPluginComponentRegistry implements PluginComponentRegistry {

    private List<Object> components = new ArrayList<>();

    /**
     * Registers a component so that subsequent plugins can discover it via {@link #getComponent}.
     */
    public void register(Object component) {
        if (components == null) {
            throw new IllegalStateException("PluginComponentRegistry is sealed; registration is no longer allowed");
        }
        components.add(component);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> Optional<T> getComponent(Class<T> type) {
        if (components == null) {
            throw new IllegalStateException("PluginComponentRegistry is sealed; lookups are no longer allowed");
        }
        for (Object component : components) {
            if (type.isInstance(component)) {
                return Optional.of((T) component);
            }
        }
        return Optional.empty();
    }

    /**
     * Seals the registry, releasing all internal references. After this call, any further
     * {@link #register} or {@link #getComponent} calls will throw {@link IllegalStateException}.
     */
    public void seal() {
        components = null;
    }
}
