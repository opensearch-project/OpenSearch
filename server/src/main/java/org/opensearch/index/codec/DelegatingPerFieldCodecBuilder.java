/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.DocValuesFormat;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.function.Function;
import java.util.function.Supplier;

/**
 * Collects the {@link PerFieldCodecBuilder} contributions of all plugins and resolves, for every
 * field, the format to use: the one contributed by the plugin which claims the field or, if no
 * plugin claims it, the format of the index codec.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DelegatingPerFieldCodecBuilder {
    private final List<PerFieldCodecBuilder> builders;

    public DelegatingPerFieldCodecBuilder(Collection<PerFieldCodecBuilder> builders) {
        this.builders = List.copyOf(builders);
    }

    /**
     * Returns {@code true} if no plugin contributed any per-field format, in which case the index
     * codecs are used as-is.
     */
    public boolean isEmpty() {
        return builders.stream()
            .noneMatch(
                builder -> builder.postingsFormat().isPresent()
                    || builder.docValuesFormat().isPresent()
                    || builder.knnVectorsFormat().isPresent()
            );
    }

    /**
     * Wraps the provided {@link Codec} so the per-field contributions of the plugins are taken into
     * account, or returns it unchanged if there are no contributions.
     */
    public Codec build(Codec delegate) {
        return isEmpty() ? delegate : new CompositeDelegatingPerFieldCodec(delegate, this);
    }

    boolean hasPostingsFormat() {
        return builders.stream().anyMatch(builder -> builder.postingsFormat().isPresent());
    }

    boolean hasDocValuesFormat() {
        return builders.stream().anyMatch(builder -> builder.docValuesFormat().isPresent());
    }

    boolean hasKnnVectorsFormat() {
        return builders.stream().anyMatch(builder -> builder.knnVectorsFormat().isPresent());
    }

    /**
     * Resolves the claims of the plugins for the provided fields eagerly, so a field claimed by more than
     * one plugin is rejected (as an invalid mapping) before any document is indexed.
     *
     * @throws IllegalArgumentException if a field is claimed by more than one plugin
     */
    public void validate(Iterable<String> fields) {
        final boolean postings = hasPostingsFormat();
        final boolean docValues = hasDocValuesFormat();
        final boolean knnVectors = hasKnnVectorsFormat();

        try {
            for (final String field : fields) {
                if (postings) {
                    claim(field, "postings format", builder -> builder.postingsFormat().map(f -> f.getPostingsFormatForField(field)));
                }
                if (docValues) {
                    claim(field, "doc values format", builder -> builder.docValuesFormat().map(f -> f.getDocValuesFormatForField(field)));
                }
                if (knnVectors) {
                    claim(
                        field,
                        "knn vectors format",
                        builder -> builder.knnVectorsFormat().map(f -> f.getKnnVectorsFormatForField(field))
                    );
                }
            }
        } catch (final IllegalStateException ex) {
            throw new IllegalArgumentException(ex.getMessage(), ex);
        }
    }

    PostingsFormat postingsFormatForField(String field, PerFieldPostingsFormat delegate) {
        return formatForField(
            field,
            "postings format",
            builder -> builder.postingsFormat().map(format -> format.getPostingsFormatForField(field)).orElse(null),
            () -> delegate.getPostingsFormatForField(field)
        );
    }

    DocValuesFormat docValuesFormatForField(String field, PerFieldDocValuesFormat delegate) {
        return formatForField(
            field,
            "doc values format",
            builder -> builder.docValuesFormat().map(format -> format.getDocValuesFormatForField(field)).orElse(null),
            () -> delegate.getDocValuesFormatForField(field)
        );
    }

    KnnVectorsFormat knnVectorsFormatForField(String field, PerFieldKnnVectorsFormat delegate) {
        return formatForField(
            field,
            "knn vectors format",
            builder -> builder.knnVectorsFormat().map(format -> format.getKnnVectorsFormatForField(field)).orElse(null),
            () -> delegate.getKnnVectorsFormatForField(field)
        );
    }

    private <F> F formatForField(String field, String kind, Function<PerFieldCodecBuilder, F> resolver, Supplier<F> fallback) {
        final F claimed = claim(field, kind, builder -> Optional.ofNullable(resolver.apply(builder)));
        return (claimed != null) ? claimed : fallback.get();
    }

    /**
     * Returns the format claimed for the field, or {@code null} if no plugin claims it.
     *
     * @throws IllegalStateException if more than one plugin claims it
     */
    private <F> F claim(String field, String kind, Function<PerFieldCodecBuilder, Optional<F>> resolver) {
        F claimed = null;
        String claimedBy = null;

        for (final PerFieldCodecBuilder builder : builders) {
            final F format = resolver.apply(builder).orElse(null);
            if (format == null) {
                continue;
            }

            if (claimed != null) {
                throw new IllegalStateException(
                    "The "
                        + kind
                        + " for field ["
                        + field
                        + "] is already provided by ["
                        + claimedBy
                        + "], it could not be provided again by ["
                        + builder.getClass().getName()
                        + "]"
                );
            }

            claimed = format;
            claimedBy = builder.getClass().getName();
        }

        return claimed;
    }
}
