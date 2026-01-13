/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.mapper.MapperService;

import java.util.Map;
import java.util.function.Supplier;

/**
 * The {@link CodecRegistry} is designated as a mechanism to contribute additional {@link Codec}
 * that require non-trivial instantiation logic (could not be registered using service
 * loader).
 *
 * <p>
 *
 * <b>Note</b>: since any plugin could contribute own list of additional {@link Codec}s to be
 * registered, OpenSearch does not provide any ordering / precedence guarantees besides the
 * ability to implement conflict resolution strategy.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface CodecRegistry {
    /**
     * Return an additional {@code Codec} instances to be registered with the default
     * (or custom, if provided) {@link CodecService}. In case there is already registered
     * {@code Codec} with the same name, the implementation calls {@code onConflict} method
     * to resolve the conflict.
     *
     * <b>Note</b>: The default codec (see {@link CodecService#LUCENE_DEFAULT_CODEC}) could not be changed
     * or removed.
     *
     * @param mapperService mapper service (if available)
     * @param indexSettings index settings
     * @param defaultCodec default {@code Codec} supplier
     * @return additional {@code Codec} instances, should not be "null"
     */
    Map<String, Codec> getCodecs(@Nullable MapperService mapperService, IndexSettings indexSettings, Supplier<Codec> defaultCodec);

    /**
     * In case there is already registered {@code Codec} with the same name, allows to resolve
     * the conflict by:
     *   - returning a {@code newCodec}, replaces the {@code oldCodec}
     *   - returning a {@code oldCodec}, effectively ignores the {@code newCodec}
     *   - returning a {@code null}, removes the {@code Codec} altogether
     *
     * By default, the implementation returns a {@code newCodec} that replaces
     * the {@code oldCodec}.
     *
     * @param name codec name
     * @param oldCodec existing codec
     * @param newCodec new codec
     * @return codec to use, or "null" for removal
     */
    default @Nullable Codec onConflict(String name, Codec oldCodec, Codec newCodec) {
        return newCodec;
    }
}
