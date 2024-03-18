/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.plugins;

import org.opensearch.index.IndexSettings;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.codec.CodecServiceFactory;
import org.opensearch.index.engine.EngineFactory;
import org.opensearch.index.seqno.RetentionLeases;
import org.opensearch.index.translog.TranslogDeletionPolicy;
import org.opensearch.index.translog.TranslogDeletionPolicyFactory;

import java.util.Optional;
import java.util.function.Supplier;

/**
 * A plugin that provides alternative engine implementations.
 *
 * @opensearch.api
 */
public interface EnginePlugin {

    /**
     * When an index is created this method is invoked for each engine plugin. Engine plugins can inspect the index settings to determine
     * whether or not to provide an engine factory for the given index. A plugin that is not overriding the default engine should return
     * {@link Optional#empty()}. If multiple plugins return an engine factory for a given index the index will not be created and an
     * {@link IllegalStateException} will be thrown during index creation.
     *
     * @return an optional engine factory
     */
    default Optional<EngineFactory> getEngineFactory(IndexSettings indexSettings) {
        return Optional.empty();
    }

    /**
     * EXPERT:
     * When an index is created this method is invoked for each engine plugin. Engine plugins can inspect the index settings
     * to determine if a custom {@link CodecService} should be provided for the given index. A plugin that is not overriding
     * the {@link CodecService} through the plugin can ignore this method and the Codec specified in the {@link IndexSettings}
     * will be used.
     *
     * @deprecated Please use {@code getCustomCodecServiceFactory()} instead as it provides more context for {@link CodecService}
     * instance construction.
     */
    @Deprecated
    default Optional<CodecService> getCustomCodecService(IndexSettings indexSettings) {
        return Optional.empty();
    }

    /**
     * EXPERT:
     * When an index is created this method is invoked for each engine plugin. Engine plugins can inspect the index settings
     * to determine if a custom {@link CodecServiceFactory} should be provided for the given index. A plugin that is not overriding
     * the {@link CodecServiceFactory} through the plugin can ignore this method and the default Codec specified in the
     * {@link IndexSettings} will be used.
     */
    default Optional<CodecServiceFactory> getCustomCodecServiceFactory(IndexSettings indexSettings) {
        return Optional.empty();
    }

    /**
     * When an index is created this method is invoked for each engine plugin. Engine plugins that need to provide a
     * custom {@link TranslogDeletionPolicy} can override this method to return a function that takes the {@link IndexSettings}
     * and a {@link Supplier} for {@link RetentionLeases} and returns a custom {@link TranslogDeletionPolicy}.
     * <p>
     * Only one of the installed Engine plugins can override this otherwise {@link IllegalStateException} will be thrown.
     *
     * @return a function that returns an instance of {@link TranslogDeletionPolicy}
     */
    default Optional<TranslogDeletionPolicyFactory> getCustomTranslogDeletionPolicyFactory() {
        return Optional.empty();
    }
}
