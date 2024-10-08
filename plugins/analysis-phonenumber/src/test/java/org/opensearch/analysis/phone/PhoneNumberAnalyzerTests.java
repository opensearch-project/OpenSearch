/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analysis.phone;

import org.apache.lucene.analysis.Analyzer;
import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;
import org.opensearch.index.analysis.AnalysisTestsHelper;
import org.opensearch.test.OpenSearchTokenStreamTestCase;
import org.junit.BeforeClass;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.Matchers.arrayContainingInAnyOrder;
import static org.hamcrest.Matchers.hasItemInArray;

public class PhoneNumberAnalyzerTests extends OpenSearchTokenStreamTestCase {
    private static final String RESOURCE = "/org/opensearch/analysis/phone/phone_analysis.json";

    private static Analyzer phoneAnalyzer;
    private static Analyzer phoneSearchAnalyzer;
    private static Analyzer phoneCHAnalyzer;
    private static Analyzer phoneSearchCHAnalyzer;

    @BeforeClass
    public static void beforeClass() throws IOException {
        final var analysis = AnalysisTestsHelper.createTestAnalysisFromClassPath(
            createTempDir(),
            RESOURCE,
            new PhoneNumberAnalysisPlugin()
        );
        phoneAnalyzer = analysis.indexAnalyzers.get("phone");
        assertNotNull(phoneAnalyzer);
        phoneSearchAnalyzer = analysis.indexAnalyzers.get("phone-search");
        assertNotNull(phoneSearchAnalyzer);
        phoneCHAnalyzer = analysis.indexAnalyzers.get("phone-ch");
        assertNotNull(phoneCHAnalyzer);
        phoneSearchCHAnalyzer = analysis.indexAnalyzers.get("phone-search-ch");
        assertNotNull(phoneSearchCHAnalyzer);
    }

    /**
     * Test for all tokens which are emitted by the "phone" analyzer.
     */
    public void testEuropeDetailled() throws IOException {
        assertTokensAreInAnyOrder(
            phoneAnalyzer,
            "tel:+441344840400",
            Arrays.asList(
                "tel:+441344840400",
                "tel:",
                "441344840400",
                "44",
                "1344840400",
                "1",
                "441",
                "13",
                "4413",
                "134",
                "44134",
                "1344",
                "441344",
                "13448",
                "4413448",
                "134484",
                "44134484",
                "1344840",
                "441344840",
                "13448404",
                "4413448404",
                "134484040",
                "44134484040"
            )
        );
    }

    /**
     * Test for all tokens which are emitted by the "phone" analyzer.
     */
    public void testEuropeDetailledSearch() throws IOException {
        assertTokensAreInAnyOrder(
            phoneSearchAnalyzer,
            "tel:+441344840400",
            Arrays.asList("tel:+441344840400", "tel:", "441344840400", "44", "1344840400")
        );
    }

    public void testEurope() throws IOException {
        assertTokensInclude("tel:+441344840400", Arrays.asList("44", "1344", "1344840400", "441344840400"));
    }

    public void testGermanCastle() throws IOException {
        assertTokensInclude("tel:+498362930830", Arrays.asList("49", "498362930830", "8362930830"));
    }

    public void testBMWofSydney() throws IOException {
        assertTokensInclude("tel:+61293344555", Arrays.asList("61", "293344555", "61293344555"));
    }

    public void testCoffeeShopInIreland() throws IOException {
        assertTokensInclude("tel:+442890319416", Arrays.asList("44", "289", "2890319416", "442890319416"));
    }

    public void testTelWithCountryCode() throws IOException {
        assertTokensInclude("tel:+17177158163", Arrays.asList("1", "717", "7177", "17177158163"));
    }

    public void testTelWithCountryCode2() throws IOException {
        assertTokensInclude("tel:+12177148350", Arrays.asList("1", "217", "2177", "2177148350", "12177148350"));
    }

    public void testNewTollFreeNumber() throws IOException {
        assertTokensInclude("tel:+18337148350", Arrays.asList("1", "833", "8337", "8337148350", "18337148350"));
    }

    public void testMissingCountryCode() throws IOException {
        assertTokensInclude("tel:8177148350", Arrays.asList("817", "8177", "81771", "817714", "8177148350"));
    }

    public void testSipWithNumericUsername() throws IOException {
        assertTokensInclude("sip:222@autosbcpc", Arrays.asList("222"));
    }

    public void testTruncatedNumber() throws IOException {
        assertTokensInclude("tel:5551234", Arrays.asList("5551234"));
    }

    public void testSipWithAlphabeticUsername() throws IOException {
        assertTokensInclude("sip:abc@autosbcpc", Arrays.asList("abc"));
    }

    public void testGarbageInGarbageOut() throws IOException {
        assertTokensInclude("test", Arrays.asList("test"));
    }

    public void testSipWithCountryCode() throws IOException {
        assertTokensInclude("sip:+14177141363@178.97.105.13;isup-oli=0;pstn-params=808481808882", Arrays.asList("417", "4177", "14177"));
    }

    public void testSipWithTelephoneExtension() throws IOException {
        assertTokensInclude("sip:+13169410766;ext=2233@178.17.10.117:8060", Arrays.asList("316", "2233", "1316"));
    }

    public void testSipWithUsername() throws IOException {
        assertTokensInclude("sip:JeffSIP@178.12.220.18", Arrays.asList("JeffSIP"));
    }

    public void testPhoneNumberWithoutPrefix() throws IOException {
        assertTokensInclude("+14177141363", Arrays.asList("14177141363", "417", "4177", "14177"));
    }

    public void testSipWithoutDomainPart() throws IOException {
        assertTokensInclude("sip:+122882", Arrays.asList("122882", "122", "228", "1228", "2288", "12288"));
    }

    public void testTelPrefix() throws IOException {
        assertTokensInclude("tel:+1228", Arrays.asList("1228", "122", "228"));
    }

    public void testNumberPrefix() throws IOException {
        assertTokensInclude("+1228", Arrays.asList("1228", "122", "228"));
    }

    public void testInternationalPrefixWithZZ() throws IOException {
        assertTokensInclude(phoneAnalyzer, "+41583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testInternationalPrefixWithCH() throws IOException {
        assertTokensInclude(phoneCHAnalyzer, "+41583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testNationalPrefixWithCH() throws IOException {
        // + is equivalent to 00 in Switzerland
        assertTokensInclude(phoneCHAnalyzer, "0041583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testLocalNumberWithCH() throws IOException {
        // when omitting the international prefix swiss numbers must start with '0'
        assertTokensInclude(phoneCHAnalyzer, "0583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testSearchInternationalPrefixWithZZ() throws IOException {
        assertTokensInclude(phoneSearchAnalyzer, "+41583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testSearchInternationalPrefixWithCH() throws IOException {
        assertTokensInclude(phoneSearchCHAnalyzer, "+41583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testSearchNationalPrefixWithCH() throws IOException {
        // + is equivalent to 00 in Switzerland
        assertTokensInclude(phoneSearchCHAnalyzer, "0041583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    public void testSearchLocalNumberWithCH() throws IOException {
        // when omitting the international prefix swiss numbers must start with '0'
        assertTokensInclude(phoneSearchCHAnalyzer, "0583161010", Arrays.asList("41", "41583161010", "583161010"));
    }

    /**
     * Unlike {@link #assertTokenStreamContents(TokenStream, String[])} this only asserts whether the generated tokens
     * contain the required ones but does not check for order. Use {@link #assertTokensInclude} if completeness is not needed.
     */
    private void assertTokensAreInAnyOrder(final Analyzer analyzer, final String input, final List<String> expectedTokens)
        throws IOException {
        final var ts = analyzer.tokenStream("test", input);
        final var allTokens = getAllTokens(ts).toArray();
        assertThat(allTokens, arrayContainingInAnyOrder(expectedTokens.toArray()));
    }

    /**
     * Unlike {@link #assertTokenStreamContents(TokenStream, String[])} this only asserts whether the generated tokens
     * contain the required ones but does not check for completeness or order.
     */
    private void assertTokensInclude(final Analyzer analyzer, final String input, final List<String> expectedTokens) throws IOException {
        final var ts = analyzer.tokenStream("test", input);
        final var allTokens = getAllTokens(ts).toArray();
        for (final var expectedToken : expectedTokens) {
            assertThat(allTokens, hasItemInArray(expectedToken));
        }
    }

    /**
     * Unlike {@link #assertTokenStreamContents(TokenStream, String[])} this only asserts whether the generated tokens
     * contain the required ones but does not check for completeness or order.
     * This uses {@link #phoneAnalyzer}.
     */
    private void assertTokensInclude(final String input, final List<String> expectedTokens) throws IOException {
        this.assertTokensInclude(phoneAnalyzer, input, expectedTokens);
    }

    private List<String> getAllTokens(final TokenStream ts) throws IOException {
        final var tokens = new ArrayList<String>();
        final var termAtt = ts.getAttribute(CharTermAttribute.class);
        ts.reset();
        while (ts.incrementToken()) {
            tokens.add(termAtt.toString());
        }
        ts.end();
        ts.close();
        return tokens;
    }

}
