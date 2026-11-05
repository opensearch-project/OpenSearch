/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Optional;

/**
 * The {@link PerFieldCodecBuilder} is a mechanism for plugins to contribute the encoding /
 * decoding of the fields they own, without replacing the {@link org.apache.lucene.codecs.Codec}
 * the index is configured with. In contrast to {@link CodecServiceFactory}, any number of
 * plugins may contribute a {@link PerFieldCodecBuilder} for the same index: the contributions
 * are collected and injected into the index codec (see also
 * {@link CompositeDelegatingPerFieldCodec}).
 *
 * <p>
 * The formats returned here are only used as per-field resolvers: for a given field name, the
 * {@code getXxxFormatForField} method has to return {@code null} if the plugin does not own the
 * field, in which case the resolution continues with the other plugins and, ultimately, falls
 * back to the format of the index codec.
 *
 * <p>
 * <b>Note</b>: only one plugin may claim a particular field, an attempt of two (or more) plugins
 * to claim the same field leads to an exception.
 *
 * <p>
 * <b>Note</b>: the per-field format name is written into the index (see please
 * {@link PerFieldKnnVectorsFormat}), so every format returned by a {@link PerFieldCodecBuilder}
 * must be discoverable through the service loader in order for the index to be readable.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface PerFieldCodecBuilder {
    /**
     * The per-field {@link org.apache.lucene.codecs.DocValuesFormat} resolver, if any.
     */
    default Optional<PerFieldDocValuesFormat> docValuesFormat() {
        return Optional.empty();
    }

    /**
     * The per-field {@link org.apache.lucene.codecs.KnnVectorsFormat} resolver, if any.
     */
    default Optional<PerFieldKnnVectorsFormat> knnVectorsFormat() {
        return Optional.empty();
    }

    /**
     * The per-field {@link org.apache.lucene.codecs.PostingsFormat} resolver, if any.
     */
    default Optional<PerFieldPostingsFormat> postingsFormat() {
        return Optional.empty();
    }
}
