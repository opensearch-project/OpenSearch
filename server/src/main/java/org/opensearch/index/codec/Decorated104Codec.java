/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec;

import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene104.Lucene104Codec;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lucene.Lucene;

import java.util.List;

/**
 * The {@link DecoratedCodec} which is compatible with the {@code Lucene104} segment info format.
 *
 * <p>
 * <b>Note</b>: on an Apache Lucene upgrade which changes the segment info format, a new version of the
 * codec has to be introduced (and to become the one {@link DecoratedCodec#wrap} uses) while this one has
 * to be kept, and registered with the service loader, so the existing segments stay readable.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class Decorated104Codec extends DecoratedCodec {
    public static final String NAME = NAME_PREFIX + "104";

    static {
        assert Lucene.LATEST_CODEC.equals("Lucene104") : "a new "
            + DecoratedCodec.class.getSimpleName()
            + " version has to be introduced for the latest Lucene codec: "
            + Lucene.LATEST_CODEC;
    }

    /** Used by the service loader to read the segments written by this codec */
    public Decorated104Codec() {
        super(NAME, new Lucene104Codec());
    }

    Decorated104Codec(Codec delegate, List<CodecDecorator> decorators) {
        super(NAME, new Lucene104Codec(), delegate, decorators);
    }
}
