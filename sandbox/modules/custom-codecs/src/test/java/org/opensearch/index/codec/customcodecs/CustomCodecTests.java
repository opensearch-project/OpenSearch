/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.codec.customcodec;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.codec.CodecService;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.IndexSettingsModule;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

@LuceneTestCase.SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CustomCodecTests extends OpenSearchTestCase {

    public void testZstdCompression() throws Exception {
        Codec codec = createCodecService().codec("ZSTD");
        assertStoredFieldsCustomCompressionEquals(Lucene92CustomCodec.Mode.ZSTD, codec);
    }

    public void testZstdNoDictCompression() throws Exception {
        Codec codec = createCodecService().codec("ZSTDNODICT");
        assertStoredFieldsCustomCompressionEquals(Lucene92CustomCodec.Mode.ZSTDNODICT, codec);
    }

    private void assertStoredFieldsCustomCompressionEquals(Lucene92CustomCodec.Mode expected, Codec actual) throws Exception {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(actual);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        String v = sr.getSegmentInfo().info.getAttribute(Lucene92CustomStoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene92CustomCodec.Mode.valueOf(v));
        ir.close();
        dir.close();
    }

    private CodecService createCodecService() throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        IndexSettings settings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        SimilarityService similarityService = new SimilarityService(settings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(settings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        MapperService service = new MapperService(
            settings,
            indexAnalyzers,
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> null,
            () -> false,
            null
        );
        return new CodecService(service, LogManager.getLogger("test"));
    }
}
