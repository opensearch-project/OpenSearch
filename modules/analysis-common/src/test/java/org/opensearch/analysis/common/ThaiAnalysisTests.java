/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.common;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.CharArraySet;
import org.apache.lucene.analysis.th.ThaiAnalyzer;
import org.apache.lucene.util.BytesRef;
import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.env.Environment;
import org.opensearch.index.analysis.Analysis;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.index.analysis.TokenizerFactory;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.OpenSearchTokenStreamTestCase;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;

public class ThaiAnalysisTests extends OpenSearchTokenStreamTestCase {

    private static OpenSearchTestCase.TestAnalysis analysis(Settings.Builder builder) throws IOException {
        return AnalysisTestsHelper.createTestAnalysisFromSettings(
            builder.put(Environment.PATH_HOME_SETTING.getKey(), createTempDir().toString()).build(),
            new CommonAnalysisModulePlugin()
        );
    }

    private static final String LEGACY_TEXT = "ผลไม้และการเปิดตัวสินค้า";

    public void testAnalyzer() throws IOException {
        Analyzer analyzer = analysis(Settings.builder().put("index.analysis.analyzer.my_thai.type", "thai")).indexAnalyzers.get("my_thai")
            .analyzer();
        assertThat(analyzer, instanceOf(ThaiAnalyzer.class));
        assertAnalyzesTo(analyzer, "เเมววิ่งเร็วๆ", new String[] { "แมว", "วิ่ง", "เร็ว", "เร็ว" });
        // Lucene's curated default stopwords keep content words such as เปิด
        assertAnalyzesTo(analyzer, LEGACY_TEXT, new String[] { "ผลไม้", "เปิด", "ตัว", "สินค้า" });
        assertEquals(new BytesRef("แมว"), analyzer.normalize("field", "เเมว"));
    }

    public void testPreBuiltAnalyzer() throws IOException {
        Analyzer analyzer = analysis(Settings.builder()).indexAnalyzers.get("thai").analyzer();
        assertThat(analyzer, instanceOf(ThaiAnalyzer.class));
        assertAnalyzesTo(analyzer, "เเมววิ่งเร็วๆ", new String[] { "แมว", "วิ่ง", "เร็ว", "เร็ว" });
    }

    public void testAnalyzerStopwords() throws IOException {
        Analyzer analyzer = analysis(
            Settings.builder()
                .put("index.analysis.analyzer.my_thai.type", "thai")
                .putList("index.analysis.analyzer.my_thai.stopwords", "แมว")
        ).indexAnalyzers.get("my_thai").analyzer();
        assertAnalyzesTo(analyzer, "เเมววิ่ง", new String[] { "วิ่ง" });
    }

    /** Indices created before Lucene 10.6 must produce the same terms as before, so that they do not need a reindex. */
    public void testAnalyzerOnOlderIndex() throws IOException {
        OpenSearchTestCase.TestAnalysis analysis = analysis(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_3_9_0)
                .put("index.analysis.analyzer.my_thai.type", "thai")
        );
        for (Analyzer analyzer : List.of(
            analysis.indexAnalyzers.get("my_thai").analyzer(),
            analysis.indexAnalyzers.get("thai").analyzer()
        )) {
            assertThat(analyzer, instanceOf(LegacyThaiAnalyzer.class));
            // no char filter, normalization or repeat expansion
            assertAnalyzesTo(analyzer, "เเมวนอนหลับ", new String[] { "เเมวน", "อ", "น", "หลับ" });
            assertAnalyzesTo(analyzer, "สวัสดีๆ", new String[] { "สวัสดี", "ๆ" });
            // the previous default stopword list removes เปิด
            assertAnalyzesTo(analyzer, LEGACY_TEXT, new String[] { "ผลไม้", "ตัว", "สินค้า" });
        }
    }

    public void testNamedStopWordsAreFrozen() {
        // _thai_ keeps resolving to the list shipped with Lucene up to 10.5, whatever the Lucene version is
        CharArraySet thai = (CharArraySet) Analysis.NAMED_STOP_WORDS.get("_thai_");
        assertThat(thai.size(), equalTo(115));
        assertTrue(thai.contains("เปิด"));
        assertTrue(thai.contains("ผล"));
        assertFalse(thai.contains("สำหรับ"));
    }

    /**
     * Lucene 10.6 treats whitespace as a safe place to cut when the 1024 character buffer fills. A word that straddles the cut
     * after a space is split by the old tokenizer, which indices created before 3.10.0 must keep doing.
     */
    public void testBufferCut() throws IOException {
        String text = "ก".repeat(1021) + " แมว";
        String[] legacy = new String[513];
        Arrays.fill(legacy, 0, 510, "กก");
        legacy[510] = "ก";
        legacy[511] = "แม";
        legacy[512] = "ว";

        for (Version version : List.of(Version.V_3_9_0, Version.V_3_10_0)) {
            OpenSearchTestCase.TestAnalysis analysis = analysis(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                    .put("index.analysis.tokenizer.my_thai.type", "thai")
                    .put("index.analysis.analyzer.thai_only.tokenizer", "my_thai")
                    .put("index.analysis.analyzer.thai_preconfigured.tokenizer", "thai")
            );
            for (String name : List.of("thai_only", "thai_preconfigured")) {
                Analyzer analyzer = analysis.indexAnalyzers.get(name).analyzer();
                if (version.equals(Version.V_3_9_0)) {
                    assertAnalyzesTo(analyzer, text, legacy);
                } else {
                    String[] tokens = new String[legacy.length - 1];
                    System.arraycopy(legacy, 0, tokens, 0, 511);
                    tokens[511] = "แมว";
                    assertAnalyzesTo(analyzer, text, tokens);
                }
            }
        }
    }

    /** {@code _thai_} is frozen: it keeps removing the words Lucene's 10.5 list removed, for every index version. */
    public void testThaiStopwordNameRemovesLegacyStopwords() throws IOException {
        for (Version version : List.of(Version.V_3_9_0, Version.V_3_10_0)) {
            Analyzer analyzer = analysis(
                Settings.builder()
                    .put(IndexMetadata.SETTING_VERSION_CREATED, version)
                    .put("index.analysis.filter.thai_stops.type", "stop")
                    .put("index.analysis.filter.thai_stops.stopwords", "_thai_")
                    .put("index.analysis.analyzer.thai_stops.tokenizer", "thai")
                    .put("index.analysis.analyzer.thai_stops.filter", "thai_stops")
            ).indexAnalyzers.get("thai_stops").analyzer();
            assertAnalyzesTo(analyzer, "ไป", new String[] {});
            assertAnalyzesTo(analyzer, "ที่ผ่านมา", new String[] {});
        }
    }

    public void testLegacyTokenizerSafeEnd() {
        LegacyThaiTokenizer tokenizer = new LegacyThaiTokenizer();
        assertTrue(tokenizer.isSafeEnd('\n'));
        assertTrue(tokenizer.isSafeEnd('\u2029'));
        assertFalse(tokenizer.isSafeEnd(' '));
    }

    public void testTokenizerOnOlderIndex() throws IOException {
        TokenizerFactory factory = analysis(
            Settings.builder()
                .put(IndexMetadata.SETTING_VERSION_CREATED, Version.V_3_9_0)
                .put("index.analysis.tokenizer.my_thai.type", "thai")
        ).tokenizer.get("my_thai");
        assertThat(factory.create(), instanceOf(LegacyThaiTokenizer.class));
    }
}
