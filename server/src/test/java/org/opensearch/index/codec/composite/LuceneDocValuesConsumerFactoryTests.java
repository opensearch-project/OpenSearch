/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.lucene912.Lucene912Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentWriteState;
import org.apache.lucene.store.Directory;
import org.apache.lucene.util.InfoStream;
import org.apache.lucene.util.Version;
import org.opensearch.index.codec.composite.composite99.Composite99Codec;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.After;
import org.junit.Before;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.UUID;

public class LuceneDocValuesConsumerFactoryTests extends OpenSearchTestCase {

    private Directory directory;
    private final String dataCodec = "data_codec";
    private final String dataExtension = "data_extension";
    private final String metaCodec = "meta_codec";
    private final String metaExtension = "meta_extension";

    @Before
    public void setup() {
        directory = newDirectory();
    }

    public void testGetDocValuesConsumerForCompositeCodec() throws IOException {
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_12_0,
            "test_segment",
            randomInt(),
            false,
            false,
            new Lucene912Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );
        SegmentWriteState state = new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[0]),
            null,
            newIOContext(random())
        );

        DocValuesConsumer consumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            state,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension
        );

        assertEquals("org.apache.lucene.codecs.lucene90.Lucene90DocValuesConsumer", consumer.getClass().getName());
        assertEquals(CompositeCodecFactory.COMPOSITE_CODEC, Composite99Codec.COMPOSITE_INDEX_CODEC_NAME);
        consumer.close();
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
