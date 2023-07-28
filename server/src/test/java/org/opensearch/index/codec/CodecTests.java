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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.index.codec;

import org.apache.logging.log4j.LogManager;
import org.apache.lucene.codecs.Codec;
import org.apache.lucene.codecs.lucene95.Lucene95Codec;
import org.apache.lucene.codecs.lucene90.Lucene90StoredFieldsFormat;
import org.apache.lucene.document.Document;
import org.apache.lucene.index.DirectoryReader;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.SegmentReader;
import org.apache.lucene.store.Directory;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressCodecs;
import org.opensearch.common.settings.IndexScopedSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.analysis.IndexAnalyzers;
import org.opensearch.index.codec.customcodecs.Lucene95CustomCodec;
import org.opensearch.index.codec.customcodecs.Lucene95CustomStoredFieldsFormat;
import org.opensearch.index.engine.EngineConfig;
import org.opensearch.index.mapper.MapperService;
import org.opensearch.index.similarity.SimilarityService;
import org.opensearch.indices.mapper.MapperRegistry;
import org.opensearch.plugins.MapperPlugin;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.IndexSettingsModule;

import java.io.IOException;
import java.util.Collections;

import static org.hamcrest.Matchers.instanceOf;
import static org.opensearch.index.engine.EngineConfig.INDEX_CODEC_COMPRESSION_LEVEL_SETTING;

@SuppressCodecs("*") // we test against default codec so never get a random one here!
public class CodecTests extends OpenSearchTestCase {

    public void testResolveDefaultCodecs() throws Exception {
        CodecService codecService = createCodecService(false);
        assertThat(codecService.codec("default"), instanceOf(PerFieldMappingPostingFormatCodec.class));
        assertThat(codecService.codec("default"), instanceOf(Lucene95Codec.class));
    }

    public void testDefault() throws Exception {
        Codec codec = createCodecService(false).codec("default");
        assertStoredFieldsCompressionEquals(Lucene95Codec.Mode.BEST_SPEED, codec);
    }

    public void testBestCompression() throws Exception {
        Codec codec = createCodecService(false).codec("best_compression");
        assertStoredFieldsCompressionEquals(Lucene95Codec.Mode.BEST_COMPRESSION, codec);
    }

    public void testZstd() throws Exception {
        Codec codec = createCodecService(false).codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(Lucene95CustomCodec.DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDict() throws Exception {
        Codec codec = createCodecService(false).codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(Lucene95CustomCodec.DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdWithCompressionLevel() throws Exception {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "zstd").codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictWithCompressionLevel() throws Exception {
        int randomCompressionLevel = randomIntBetween(1, 6);
        Codec codec = createCodecService(randomCompressionLevel, "zstd_no_dict").codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(randomCompressionLevel, storedFieldsFormat.getCompressionLevel());
    }

    public void testBestCompressionWithCompressionLevel() {
        final Settings zstdSettings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomFrom(CodecService.ZSTD_CODEC, CodecService.ZSTD_NO_DICT_CODEC))
            .build();

        // able to validate zstd
        final IndexScopedSettings zstdIndexScopedSettings = new IndexScopedSettings(
            zstdSettings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
        zstdIndexScopedSettings.validate(zstdSettings, true);

        final Settings settings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), randomFrom(CodecService.DEFAULT_CODEC, CodecService.BEST_COMPRESSION_CODEC))
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        final IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> indexScopedSettings.validate(settings, true));
        assertTrue(e.getMessage().startsWith("Compression level cannot be set"));
    }

    public void testLuceneCodecsWithCompressionLevel() {
        String codecName = randomFrom(Codec.availableCodecs());
        Codec codec = Codec.forName(codecName);

        final Settings customCodecSettings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), "Lucene95CustomCodec")
            .build();

        final IndexScopedSettings customCodecIndexScopedSettings = new IndexScopedSettings(
            customCodecSettings,
            IndexScopedSettings.BUILT_IN_INDEX_SETTINGS
        );
        customCodecIndexScopedSettings.validate(customCodecSettings, true);

        final Settings settings = Settings.builder()
            .put(INDEX_CODEC_COMPRESSION_LEVEL_SETTING.getKey(), randomIntBetween(1, 6))
            .put(EngineConfig.INDEX_CODEC_SETTING.getKey(), codecName)
            .build();
        final IndexScopedSettings indexScopedSettings = new IndexScopedSettings(settings, IndexScopedSettings.BUILT_IN_INDEX_SETTINGS);

        if (!(codec instanceof CodecSettings && ((CodecSettings) codec).supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING))) {
            final IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> indexScopedSettings.validate(settings, true)
            );
            assertTrue(e.getMessage().startsWith("Compression level cannot be set"));
        }
    }

    public void testZstandardCompressionLevelSupport() throws Exception {
        CodecService codecService = createCodecService(false);
        CodecSettings zstdCodec = (CodecSettings) codecService.codec("zstd");
        CodecSettings zstdNoDictCodec = (CodecSettings) codecService.codec("zstd_no_dict");
        assertTrue(zstdCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
        assertTrue(zstdNoDictCodec.supports(INDEX_CODEC_COMPRESSION_LEVEL_SETTING));
    }

    public void testDefaultMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("default");
        assertStoredFieldsCompressionEquals(Lucene95Codec.Mode.BEST_SPEED, codec);
    }

    public void testBestCompressionMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("best_compression");
        assertStoredFieldsCompressionEquals(Lucene95Codec.Mode.BEST_COMPRESSION, codec);
    }

    public void testZstdMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("zstd");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(Lucene95CustomCodec.DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testZstdNoDictMapperServiceNull() throws Exception {
        Codec codec = createCodecService(true).codec("zstd_no_dict");
        assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode.ZSTD_NO_DICT, codec);
        Lucene95CustomStoredFieldsFormat storedFieldsFormat = (Lucene95CustomStoredFieldsFormat) codec.storedFieldsFormat();
        assertEquals(Lucene95CustomCodec.DEFAULT_COMPRESSION_LEVEL, storedFieldsFormat.getCompressionLevel());
    }

    public void testExceptionCodecNull() {
        assertThrows(IllegalArgumentException.class, () -> createCodecService(true).codec(null));
    }

    public void testExceptionIndexSettingsNull() {
        assertThrows(AssertionError.class, () -> new CodecService(null, null, LogManager.getLogger("test")));
    }

    // write some docs with it, inspect .si to see this was the used compression
    private void assertStoredFieldsCompressionEquals(Lucene95Codec.Mode expected, Codec actual) throws Exception {
        SegmentReader sr = getSegmentReader(actual);
        String v = sr.getSegmentInfo().info.getAttribute(Lucene90StoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene95Codec.Mode.valueOf(v));
    }

    private void assertStoredFieldsCompressionEquals(Lucene95CustomCodec.Mode expected, Codec actual) throws Exception {
        SegmentReader sr = getSegmentReader(actual);
        String v = sr.getSegmentInfo().info.getAttribute(Lucene95CustomStoredFieldsFormat.MODE_KEY);
        assertNotNull(v);
        assertEquals(expected, Lucene95CustomCodec.Mode.valueOf(v));
    }

    private CodecService createCodecService(boolean isMapperServiceNull) throws IOException {
        Settings nodeSettings = Settings.builder().put(Environment.PATH_HOME_SETTING.getKey(), createTempDir()).build();
        if (isMapperServiceNull) {
            return new CodecService(null, IndexSettingsModule.newIndexSettings("_na", nodeSettings), LogManager.getLogger("test"));
        }
        return buildCodecService(nodeSettings);
    }

    private CodecService createCodecService(int randomCompressionLevel, String codec) throws IOException {
        Settings nodeSettings = Settings.builder()
            .put(Environment.PATH_HOME_SETTING.getKey(), createTempDir())
            .put("index.codec", codec)
            .put("index.codec.compression_level", randomCompressionLevel)
            .build();
        return buildCodecService(nodeSettings);
    }

    private CodecService buildCodecService(Settings nodeSettings) throws IOException {

        IndexSettings indexSettings = IndexSettingsModule.newIndexSettings("_na", nodeSettings);
        SimilarityService similarityService = new SimilarityService(indexSettings, null, Collections.emptyMap());
        IndexAnalyzers indexAnalyzers = createTestAnalysis(indexSettings, nodeSettings).indexAnalyzers;
        MapperRegistry mapperRegistry = new MapperRegistry(Collections.emptyMap(), Collections.emptyMap(), MapperPlugin.NOOP_FIELD_FILTER);
        MapperService service = new MapperService(
            indexSettings,
            indexAnalyzers,
            xContentRegistry(),
            similarityService,
            mapperRegistry,
            () -> null,
            () -> false,
            null
        );
        return new CodecService(service, indexSettings, LogManager.getLogger("test"));
    }

    private SegmentReader getSegmentReader(Codec codec) throws IOException {
        Directory dir = newDirectory();
        IndexWriterConfig iwc = newIndexWriterConfig(null);
        iwc.setCodec(codec);
        IndexWriter iw = new IndexWriter(dir, iwc);
        iw.addDocument(new Document());
        iw.commit();
        iw.close();
        DirectoryReader ir = DirectoryReader.open(dir);
        SegmentReader sr = (SegmentReader) ir.leaves().get(0).reader();
        ir.close();
        dir.close();
        return sr;
    }

}
