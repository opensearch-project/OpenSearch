/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.mapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.action.ActionListener;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.plugins.MapperPlugin;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.util.annotation.NonNull;

/**
 * This class collects all registered mapping transformers and applies them when needed.
 */
public class MappingTransformerRegistry {
    private final List<MappingTransformer> transformers;
    private final NamedXContentRegistry xContentRegistry;
    protected final Logger logger = LogManager.getLogger(getClass());

    public MappingTransformerRegistry(
        @NonNull final List<MapperPlugin> mapperPlugins,
        @NonNull final NamedXContentRegistry xContentRegistry
    ) {
        this.xContentRegistry = xContentRegistry;
        this.transformers = new ArrayList<>();
        // Collect transformers from all MapperPlugins
        for (MapperPlugin plugin : mapperPlugins) {
            transformers.addAll(plugin.getMappingTransformers());
        }
    }

    private void applyNext(
        @NonNull final Map<String, Object> mapping,
        final MappingTransformer.TransformContext context,
        @NonNull final AtomicInteger index,
        @NonNull final ActionListener<String> listener
    ) {
        try {
            // Parse the mapping after each transformer to catch corruption early and identify the faulty transformer.
            final XContentBuilder builder = XContentFactory.jsonBuilder();
            builder.map(mapping);
            final String mappingString = builder.toString();
            if (index.get() == transformers.size()) {
                listener.onResponse(mappingString);
                return;
            }
        } catch (IOException e) {
            if (index.get() == 0) {
                listener.onFailure(
                    new RuntimeException(
                        "Failed to transform the mapping due to the mapping is not a valid json string [" + mapping + "]",
                        e
                    )
                );
            } else {
                listener.onFailure(
                    new RuntimeException(
                        "Failed to parse the mapping ["
                            + mapping
                            + "] transformed by"
                            + " the transformer ["
                            + transformers.get(index.get() - 1).getClass().getName()
                            + "]",
                        e
                    )
                );
            }
        }

        final MappingTransformer transformer = transformers.get(index.getAndIncrement());
        logger.debug("Applying mapping transformer: {}", transformer.getClass().getName());
        transformer.transform(mapping, context, new ActionListener<>() {
            @Override
            public void onResponse(Void unused) {
                logger.debug("Completed transformation: {}", transformer.getClass().getName());
                applyNext(mapping, context, index, listener);
            }

            @Override
            public void onFailure(Exception e) {
                logger.error("Transformer {} failed: {}", transformer.getClass().getName(), e.getMessage());
                listener.onFailure(e);
            }
        });
    }

    /**
     * Applies all registered transformers to the provided mapping which is in string format.
     * @param mappingString The index mapping to transform.
     * @param context Context for mapping transform
     * @param mappingTransformListener A listener for mapping transform
     */
    public void applyTransformers(
        final String mappingString,
        final MappingTransformer.TransformContext context,
        @NonNull final ActionListener<String> mappingTransformListener
    ) {
        if (transformers.isEmpty() || mappingString == null) {
            mappingTransformListener.onResponse(mappingString);
            return;
        }

        Map<String, Object> mappingMap;
        try {
            mappingMap = MapperService.parseMapping(xContentRegistry, mappingString);
        } catch (IOException e) {
            mappingTransformListener.onFailure(
                new IllegalArgumentException(
                    "Failed to transform the mappings because failed to parse the mappings [" + mappingString + "]",
                    e
                )
            );
            return;
        }

        final AtomicInteger index = new AtomicInteger(0);
        applyNext(mappingMap, context, index, mappingTransformListener);
    }
}
