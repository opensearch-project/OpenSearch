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
import org.apache.lucene.codecs.FilterCodec;
import org.apache.lucene.codecs.KnnVectorsFormat;
import org.apache.lucene.codecs.PostingsFormat;
import org.apache.lucene.codecs.perfield.PerFieldDocValuesFormat;
import org.apache.lucene.codecs.perfield.PerFieldKnnVectorsFormat;
import org.apache.lucene.codecs.perfield.PerFieldPostingsFormat;
import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Objects;

/**
 * The {@link Codec} that injects the per-field formats contributed by the plugins (see please
 * {@link PerFieldCodecBuilder}) into the {@link Codec} the index is configured with. The fields
 * which are not claimed by any plugin are encoded / decoded by the delegate exactly the same way
 * as if there were no plugins at all, and so are the formats no plugin contributed at all.
 *
 * <p>
 * The codec deliberately reports the name of the delegate: the per-field formats are resolved from
 * the field attributes (through the service loader) at read time, so the segments written by this
 * codec are readable by the delegate alone. In particular, an index does not become unreadable if
 * the codec is not wrapped anymore (for example, when a plugin stops contributing formats), as long
 * as the formats themselves are still discoverable.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class CompositeDelegatingPerFieldCodec extends FilterCodec {
    private final PostingsFormat postingsFormat;
    private final DocValuesFormat docValuesFormat;
    private final KnnVectorsFormat knnVectorsFormat;

    CompositeDelegatingPerFieldCodec(Codec delegate, DelegatingPerFieldCodecBuilder builder) {
        super(Objects.requireNonNull(delegate, "delegate codec is required").getName(), delegate);

        Objects.requireNonNull(builder, "per-field codec builder is required");

        if (builder.hasPostingsFormat()) {
            final PerFieldPostingsFormat delegateFormat = perFieldFormat(
                delegate,
                delegate.postingsFormat(),
                PerFieldPostingsFormat.class,
                "postings"
            );

            this.postingsFormat = new PerFieldPostingsFormat() {
                @Override
                public PostingsFormat getPostingsFormatForField(String field) {
                    return builder.postingsFormatForField(field, delegateFormat);
                }
            };
        } else {
            this.postingsFormat = delegate.postingsFormat();
        }

        if (builder.hasDocValuesFormat()) {
            final PerFieldDocValuesFormat delegateFormat = perFieldFormat(
                delegate,
                delegate.docValuesFormat(),
                PerFieldDocValuesFormat.class,
                "doc values"
            );

            this.docValuesFormat = new PerFieldDocValuesFormat() {
                @Override
                public DocValuesFormat getDocValuesFormatForField(String field) {
                    return builder.docValuesFormatForField(field, delegateFormat);
                }
            };
        } else {
            this.docValuesFormat = delegate.docValuesFormat();
        }

        if (builder.hasKnnVectorsFormat()) {
            final PerFieldKnnVectorsFormat delegateFormat = perFieldFormat(
                delegate,
                delegate.knnVectorsFormat(),
                PerFieldKnnVectorsFormat.class,
                "knn vectors"
            );

            this.knnVectorsFormat = new PerFieldKnnVectorsFormat() {
                @Override
                public KnnVectorsFormat getKnnVectorsFormatForField(String field) {
                    return builder.knnVectorsFormatForField(field, delegateFormat);
                }
            };
        } else {
            this.knnVectorsFormat = delegate.knnVectorsFormat();
        }
    }

    @Override
    public PostingsFormat postingsFormat() {
        return postingsFormat;
    }

    @Override
    public DocValuesFormat docValuesFormat() {
        return docValuesFormat;
    }

    @Override
    public KnnVectorsFormat knnVectorsFormat() {
        return knnVectorsFormat;
    }

    /**
     * The per-field format of the delegate is used to encode the fields no plugin claims: since the
     * per-field bookkeeping (field attributes, segment suffixes) does not tolerate nesting, the
     * delegate has to be per-field aware to begin with.
     */
    private static <F, P extends F> P perFieldFormat(Codec delegate, F format, Class<P> perFieldType, String kind) {
        if (perFieldType.isInstance(format) == false) {
            throw new IllegalStateException(
                "The codec ["
                    + delegate.getName()
                    + "] does not support per-field "
                    + kind
                    + " customization: its "
                    + kind
                    + " format ["
                    + format.getClass().getName()
                    + "] is not a ["
                    + perFieldType.getName()
                    + "]"
            );
        }

        return perFieldType.cast(format);
    }
}
