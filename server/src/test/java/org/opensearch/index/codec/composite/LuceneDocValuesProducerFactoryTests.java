/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.composite;

import org.apache.lucene.codecs.DocValuesConsumer;
import org.apache.lucene.codecs.lucene99.Lucene99Codec;
import org.apache.lucene.index.FieldInfo;
import org.apache.lucene.index.FieldInfos;
import org.apache.lucene.index.SegmentInfo;
import org.apache.lucene.index.SegmentReadState;
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

import static org.opensearch.index.codec.composite.composite99.Composite99Codec.COMPOSITE_INDEX_CODEC_NAME;
import static org.mockito.Mockito.mock;

public class LuceneDocValuesProducerFactoryTests extends OpenSearchTestCase {

    private Directory directory;
    private final String dataCodec = "data_codec";
    private final String dataExtension = "data_extension";
    private final String metaCodec = "meta_codec";
    private final String metaExtension = "meta_extension";

    @Before
    public void setup() {
        directory = newDirectory();
    }

    public void testGetDocValuesProducerForCompositeCodec99() throws IOException {
        SegmentInfo segmentInfo = new SegmentInfo(
            directory,
            Version.LATEST,
            Version.LUCENE_9_11_0,
            "test_segment",
            randomInt(),
            false,
            false,
            new Lucene99Codec(),
            new HashMap<>(),
            UUID.randomUUID().toString().substring(0, 16).getBytes(StandardCharsets.UTF_8),
            new HashMap<>(),
            null
        );

        // open an consumer first in order for the producer to find the file
        SegmentWriteState state = new SegmentWriteState(
            InfoStream.getDefault(),
            segmentInfo.dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[0]),
            null,
            newIOContext(random())
        );
        DocValuesConsumer consumer = LuceneDocValuesConsumerFactory.getDocValuesConsumerForCompositeCodec(
            COMPOSITE_INDEX_CODEC_NAME,
            state,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension
        );
        consumer.close();

        SegmentReadState segmentReadState = new SegmentReadState(
            segmentInfo.dir,
            segmentInfo,
            new FieldInfos(new FieldInfo[0]),
            newIOContext(random())
        );
        CompositeDocValuesProducer producer = LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
            Composite99Codec.COMPOSITE_INDEX_CODEC_NAME,
            segmentReadState,
            dataCodec,
            dataExtension,
            metaCodec,
            metaExtension
        );

        assertNotNull(producer);
        assertEquals("org.apache.lucene.codecs.lucene90.Lucene90DocValuesProducer", producer.getDocValuesProducer().getClass().getName());
        producer.getDocValuesProducer().close();
    }

    public void testGetDocValuesProducerForCompositeCodec_InvalidCodec() {
        SegmentReadState mockSegmentReadState = mock(SegmentReadState.class);

        IllegalStateException exception = expectThrows(IllegalStateException.class, () -> {
            LuceneDocValuesProducerFactory.getDocValuesProducerForCompositeCodec(
                "invalid_codec",
                mockSegmentReadState,
                dataCodec,
                dataExtension,
                metaCodec,
                metaExtension
            );
        });

        assertNotNull(exception);
        assertTrue(exception.getMessage().contains("Invalid composite codec"));
    }

    @After
    public void teardown() throws Exception {
        super.tearDown();
        directory.close();
    }
}
