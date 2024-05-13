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
 *    http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.ingest.attachment;

import org.apache.commons.codec.digest.DigestUtils;
import org.apache.lucene.tests.util.LuceneTestCase.SuppressFileSystems;
import org.apache.lucene.tests.util.TestUtil;
import org.apache.tika.metadata.Metadata;
import org.opensearch.common.io.PathUtils;
import org.opensearch.test.OpenSearchTestCase;
import org.junit.Before;

import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Locale;
import java.util.Map;

/**
 * Parse sample tika documents and assert the contents has not changed according to previously recorded checksums.
 * Uncaught changes to tika parsing could potentially pose bwc issues.
 */
@SuppressFileSystems("ExtrasFS") // don't try to parse extraN
public class TikaDocTests extends OpenSearchTestCase {

    /** some test files from tika test suite, zipped up */
    static final String TIKA_FILES = "/org/opensearch/ingest/attachment/test/tika-files/";

    /** tika 2.9.2 expected output in the form of filename:sha1 key value pairs  */
    static final Map<String, String> tika292Checksums = Map.ofEntries(
        Map.entry("testPDFTwoTextBoxes.pdf", "4adf324ce030076b1755fdb3a6cce676ee325ae4"),
        Map.entry("testRTFUnicodeGothic.rtf", "f9932470ff686b0c217ea94ed5d4f2fd85f7998e"),
        Map.entry("headers.mbox", "75ec25789fe870b6d25365e4ea73d731fc274847"),
        Map.entry("testPPT_embeded.ppt", "2b8cebf2e67097b63b683acc5645a99d3ab781ac"),
        Map.entry("testXML3.xml", "804d4812408eb324ae8483d2140b648ec871dd2a"),
        Map.entry("testOptionalHyphen.doc", "10f9ca38cc2985e94967aa2c454bfe40aff76976"),
        Map.entry("testComment.doc", "66e57653d5d08478556ca640408b172b65855cc7"),
        Map.entry("testEXCEL_headers_footers.xls", "18977c66fc8bcb8c44de3063b69b65a3de9c3f25"),
        Map.entry("testWORD_embedded_rtf.doc", "cc2d289acfe3d1068a2649b7fa0c06c50bb6ceda"),
        Map.entry("testEXCEL_custom_props.xlsx", "6b72ae08362a204b37dbba0a30b4134ae3e7918f"),
        Map.entry("testOptionalHyphen.docx", "5b8ffc0df1691a8fed7d63aa9b256e9e02e36d71"),
        Map.entry("testPPT_various.pptx", "d149de9af8071141a6ba6e2cd4ef5f6d9431a826"),
        Map.entry("testWORD_closingSmartQInHyperLink.doc", "9859f378c603b70bf0d44a281169ae5b16a21878"),
        Map.entry("test_embedded_zip.pptx", "d19406edcec09440d066877c451ceba60abc3483"),
        Map.entry("testRTFUmlautSpaces.rtf", "155b39879c5b5fbad22fd650be37ae7f91489eb2"),
        Map.entry("protectedFile.xlsx", "ee08eeaf05c35c960243f831c3a974d9ee07aa28"),
        Map.entry("Doc1_ole.doc", "fb63220506ab666f1fe87b0608e1447fd4fd3489"),
        Map.entry("testEXCEL_embeded.xlsx", "b1e9e6e5bd8702fd84586774a9d5e14ce8d61451"),
        Map.entry("EmbeddedDocument.docx", "3b90c2cd71ccca918bb855580cde96d440225e9f"),
        Map.entry("testODFwithOOo3.odt", "3815d6fb7f5829db882ea8ebd664f252711e6e60"),
        Map.entry("testPagesHeadersFootersRomanUpper.pages", "85b3cd545ba6c33e5d44b844a6afea8cb6eaec0b"),
        Map.entry("testPPT_comment.ppt", "88fd667fd0292785395a8d0d229304aa91110556"),
        Map.entry("testPPT_2imgs.pptx", "66eda11ad472918153100dad8ee5be0f1f8e2e04"),
        Map.entry("testPagesHeadersFootersAlphaUpper.pages", "56bef0d1eaedfd7599aae29031d2eeb0e3fe4688"),
        Map.entry("testWORD_text_box.docx", "e01f7b05c6aac3449b9a699c3e4d2e62ff3368a3"),
        Map.entry("testWORD_missing_text.docx", "3814332884a090b6d1020bff58d0531486710c45"),
        Map.entry("testComment.pdf", "60e181061a00454c2e622bd37a9878234c13231d"),
        Map.entry("testPDF_no_extract_no_accessibility_owner_empty.pdf", "6eb693dac68fece3bf3cd1aa9880ea9b23fc927c"),
        Map.entry("test_embedded_package.rtf", "cd90adb3f777e68aa0288fd23e8f4fbce260a763"),
        Map.entry("testPDF_bom.pdf", "6eb693dac68fece3bf3cd1aa9880ea9b23fc927c"),
        Map.entry("testWORD_tabular_symbol.doc", "c708d7ef841f7e1748436b8ef5670d0b2de1a227"),
        Map.entry("testWORD_1img.docx", "367e2ade13ca3c19bcd8a323e21d51d407e017ac"),
        Map.entry("testMasterFooter.odp", "bcc59df70699c739423a50e362c722b81ae76498"),
        Map.entry("testTXTNonASCIIUTF8.txt", "1ef514431ca8d838f11e99f8e4a0637730b77aa0"),
        Map.entry("EmbeddedOutlook.docx", "c544a6765c19ba11b0bf3edb55c79e1bd8565c6e"),
        Map.entry("testWORD_override_list_numbering.docx", "4e892319b921322916225def763f451e4bbb4e16"),
        Map.entry("testTextBoxes.key", "b01581d5bd2483ce649a1a1406136359f4b93167"),
        Map.entry("testPPT_masterText.pptx", "9fee8337b76dc3e196f4554dcde22b9dd1c3b3e8"),
        Map.entry("testComment.docx", "333b9009686f27265b4729e8172b3e62048ec7ec"),
        Map.entry("testRTFInvalidUnicode.rtf", "32b3e3d8e5c5a1b66cb15fc964b9341bea7048f4"),
        Map.entry("testEXCEL_headers_footers.xlsx", "9e8d2a700fc431fe29030e86e08162fc8ecf2c1a"),
        Map.entry("testWORD6.doc", "1479de589755c7212815445799c44dab69d4587c"),
        Map.entry("testPagesHeadersFootersFootnotes.pages", "99d434be7de4902dc70700aa9c2a31624583c1f1"),
        Map.entry("testPDF_no_extract_yes_accessibility_owner_empty.pdf", "6eb693dac68fece3bf3cd1aa9880ea9b23fc927c"),
        Map.entry("testOpenOffice2.odt", "564b3e1999a53073a04142e01b663757a6e7fb08"),
        Map.entry("testTables.key", "250cff75db7fc3c8b95b2cbd3f37308826e0c93d"),
        Map.entry("testDOCX_Thumbnail.docx", "fce6a43271bc242e2bb8341afa659ed166e08050"),
        Map.entry("testWORD_3imgs.docx", "292ca6fa41d32b462e66061e89adb19423721975"),
        Map.entry("testPDF_acroform3.pdf", "dcf6588cb5e41701b168606ea6bfbadecdcd3bc9"),
        Map.entry("testWORD_missing_ooxml_bean1.docx", "c3058f2513fecc0a6d76d3ecf55676f236b085ff"),
        Map.entry("testOptionalHyphen.ppt", "7e016e42860bd408054bb8653fef39b2756119d9"),
        Map.entry("testHTML_utf8.html", "3ba828044754772e4c9df5f9a2213beaa75842ef"),
        Map.entry("testPPT_comment.pptx", "25fab588194dabd5902fd2ef880ee9542d036776"),
        Map.entry("testRTFWithCurlyBraces.rtf", "019cab63b73ff89d094823cf50c0a721bec08ee2"),
        Map.entry("testFooter.ods", "846e1d0415b23fa27631b536b0cf566abbf8fcc1"),
        Map.entry("testPPT.ppt", "933ee556884b1d9e28b801daa0d77bbaa4f4be62"),
        Map.entry("testEXCEL-formats.xls", "3f3e2e5cd7d6527af8d15e5668dc2cf7c33b25fe"),
        Map.entry("testPPT_masterFooter.pptx", "29bb97006b3608b7db6ff72b94d20157878d94dd"),
        Map.entry("testWORD_header_hyperlink.doc", "914bbec0730c54948ad307ea3e375ef0c100abf1"),
        Map.entry("testRTFHyperlink.rtf", "2b2ffb1997aa495fbab1af490d134051de168c97"),
        Map.entry("testExtraSpaces.pdf", "b5575400309b01c1050a927d8d1ecf8761062abc"),
        Map.entry("testRTFWindowsCodepage1250.rtf", "7ba418843f401634f97d21c844c2c4093b7194fb"),
        Map.entry("testRTFTableCellSeparation2.rtf", "62782ca40ff0ed6c3ba90f8055ee724b44af203f"),
        Map.entry("testPagesHeadersFootersRomanLower.pages", "2410fc803907001eb39c201ad4184b243e271c6d"),
        Map.entry("headerPic.docx", "c704bb648feac7975dff1024a5f762325be7cbc2"),
        Map.entry("testHTMLNoisyMetaEncoding_4.html", "630e14e3495a78580c4e26fa3bbe3123ccf4fd8a"),
        Map.entry("testRTFBoldItalic.rtf", "0475d224078682cf3f9f3f4cbc14a63456c5a0d8"),
        Map.entry("test-outlook.msg", "1f202fc11a873e305d5b4d4607409f3f734065ec"),
        Map.entry("testRTFVarious.rtf", "bf6ea9cf57886e680c5e6743a66a12b950a09083"),
        Map.entry("testXHTML.html", "c6da900f81c1c550518e65d579d3dd62dd7c5c0c"),
        Map.entry("EmbeddedPDF.docx", "454476bdf4a968189a6f53e75c146382bf58a434"),
        Map.entry("testXML.xml", "e1615e9b31be58f7af9ad963e5a112efa5cdaffa"),
        Map.entry("testWORD_no_format.docx", "9a3f5d8a4c8c0f077cc615bcfc554dc87d5926aa"),
        Map.entry("testPPT_masterText.ppt", "f5ff5e2d45ccb180cf371ed99b7dfeb2a93539b3"),
        Map.entry("testPDF_PDFEncodedStringInXMP.pdf", "78fd59d394f72d28a9908739fa562099978dafa1"),
        Map.entry("testPPT_custom_props.pptx", "72152d28afbc23a50cc71fa37d1dce9ef03ca72d"),
        Map.entry("testRTFListOverride.rtf", "f8c61d8a66afdaa07f3740e859497818bfc2ca01"),
        Map.entry("testEXCEL_1img.xls", "c76078abbb5304a20ccfe5e31ef0c12247d6083e"),
        Map.entry("testWORD_1img.doc", "0826d299a7770e93603f5667d89dccb7b74d904c"),
        Map.entry("testNPEOpenDocument.odt", "4210b973c80084c58463ec637fa43e911f77d6fe"),
        Map.entry("testRTFWord2010CzechCharacters.rtf", "9443011aac32434240ab8dbff360c970fc1c7074"),
        Map.entry("testPDF_Version.8.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testPPT.ppsx", "71333ef84f7825d8ad6aba2ba993d04b4bab41c6"),
        Map.entry("testPPT_autodate.pptx", "50467dbb37d1c74b8b37fe93eddf6f9e87d21bf3"),
        Map.entry("testWordArt.pptx", "3566bbee790704b3654fe78319957f9e0cddb6d9"),
        Map.entry("NullHeader.docx", "18430c968ba29173b52610efdaa723424b3c4d79"),
        Map.entry("testRTFWordPadCzechCharacters.rtf", "5dbb58452a3507c384008662f8fce90063f12189"),
        Map.entry("resume.html", "fbfb9d8264f6eebd79847fe7a7f1b81edd4a027d"),
        Map.entry("testPagesLayout.pages", "5db1ab91c93e6183d0af8513f62c7b87964704af"),
        Map.entry("testOptionalHyphen.pptx", "c2977eefe7d2cad8c671f550d7883185ec65591b"),
        Map.entry("testWORD_numbered_list.docx", "07194c58165993468e66bc4eba4f5bd89d5bee09"),
        Map.entry("testEXCEL_1img.xlsx", "17f2a6dde2336cf726285106b96c555b1b60e5e8"),
        Map.entry("testPDFTripleLangTitle.pdf", "6eb693dac68fece3bf3cd1aa9880ea9b23fc927c"),
        Map.entry("protect.xlsx", "ee08eeaf05c35c960243f831c3a974d9ee07aa28"),
        Map.entry("testWORD_bold_character_runs2.docx", "f10e562d8825ec2e17e0d9f58646f8084a658cfa"),
        Map.entry("testXLSX_Thumbnail.xlsx", "020bf155ae157661c11727c54e6694cf9cd2c0d3"),
        Map.entry("testWORD_embedded_pdf.docx", "d8adb797aaaac92afd8dd9b499bd197347f15688"),
        Map.entry("testOptionalHyphen.rtf", "2f77b61bab5b4502b4ddd5018b454be157091d07"),
        Map.entry("testEXCEL-charts.xls", "4011b1d165bf9220a66591c5a161d22d3eb49afd"),
        Map.entry("testWORD_override_list_numbering.doc", "60e47a3e71ba08af20af96131d61740a1f0bafa3"),
        Map.entry("testPDF_twoAuthors.pdf", "c5f0296cc21f9ae99ceb649b561c55f99d7d9452"),
        Map.entry("testPDF_Version.10.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testHTMLNoisyMetaEncoding_2.html", "630e14e3495a78580c4e26fa3bbe3123ccf4fd8a"),
        Map.entry("testFooter.odt", "cd5d0fcbcf48d6f005d087c47d00e84f39bcc321"),
        Map.entry("testPPT.pptm", "71333ef84f7825d8ad6aba2ba993d04b4bab41c6"),
        Map.entry("testPPT_various.ppt", "399e27a9893284f106dc44f15b5e636454db681e"),
        Map.entry("testRTFListMicrosoftWord.rtf", "0303eb3e2f30530621a7a407847b759a3b21467e"),
        Map.entry("testWORD_bold_character_runs2.doc", "f10e562d8825ec2e17e0d9f58646f8084a658cfa"),
        Map.entry("boilerplate-whitespace.html", "a9372bc75d7d84cbcbb0bce68fcaed73ad8ef52c"),
        Map.entry("testEXCEL_95.xls", "20d9b9b0f3aecd28607516b4b837c8bab3524b6c"),
        Map.entry("testPPT_embedded_two_slides.pptx", "0d760dbaf9d9d2f173dd40deecd0de5ecb885301"),
        Map.entry("testPDF_bookmarks.pdf", "5fc486c443511452db4f1aa6530714c6aa49c831"),
        Map.entry("test_recursive_embedded.docx", "afc32b07ce07ad273e5b3d1a43390a9d2b6dd0a9"),
        Map.entry("testEXCEL-formats.xlsx", "801f4850a8e5dca36cd2e3544cb4e74d8f4265f5"),
        Map.entry("testPPT_masterText2.pptx", "2b01eab5d0349e3cfe791b28c70c2dbf4efc884d"),
        Map.entry("test.doc", "774be3106edbb6d80be36dbb548d62401dcfa0fe"),
        Map.entry("test_recursive_embedded_npe.docx", "afc32b07ce07ad273e5b3d1a43390a9d2b6dd0a9"),
        Map.entry("testPPT_embedded2.ppt", "80e106b3fc68107e7f9579cff04e3b15bdfc557a"),
        Map.entry("testWORD_custom_props.docx", "e7a737a5237a6aa9c6b3fc677eb8fa65c30d6dfe"),
        Map.entry("testPDF_Version.4.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testBinControlWord.rtf", "ef858fbb7584ea7f92ffed8d0a08c1cc35ffee07"),
        Map.entry("testWORD_null_style.docx", "0be9dcfb83423c78a06af514ec21e4e7770ec48e"),
        Map.entry("test-outlook2003.msg", "bb3c35eb7e95d657d7977c1d3d52862734f9f329"),
        Map.entry("testPDFVarious.pdf", "c66bbbacb10dd27430f7d0bed9518e75793cedae"),
        Map.entry("testHTMLNoisyMetaEncoding_3.html", "630e14e3495a78580c4e26fa3bbe3123ccf4fd8a"),
        Map.entry("testRTFCorruptListOverride.rtf", "116a782d02a7f25010a15cbbb189bf98e6b89855"),
        Map.entry("testEXCEL_custom_props.xls", "b5584d9b13ab1566ce539238dc75e7eb3449ba7f"),
        Map.entry("testPDF_Version.7.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testPDFEmbeddingAndEmbedded.docx", "e7b648adb15cd16cdd84437c2b9524a8eeb213e4"),
        Map.entry("testHTMLNoisyMetaEncoding_1.html", "630e14e3495a78580c4e26fa3bbe3123ccf4fd8a"),
        Map.entry("testWORD_3imgs.doc", "818aa8c6c44dd78c49100c3c38e95abdf3812981"),
        Map.entry("testRTFEmbeddedLink.rtf", "2720ffb5ff3a6bbb2c5c1cb43fb4922362ed788a"),
        Map.entry("testKeynote.key", "11387b59fc6339bb73653fcbb26d387521b98ec9"),
        Map.entry("testPDF.pdf", "5a377554685367764eaf73d093408ace323fcec7"),
        Map.entry("protectedSheets.xlsx", "91efe165ee6de85fa59e5be9773c379c2c1ea149"),
        Map.entry("testWORD.doc", "cdd41377e699287cbbe17fbb1498cfe5814dde23"),
        Map.entry("testComment.xlsx", "d4be580bb97c1c90be379281179c7932b37a18c0"),
        Map.entry("testPDFPackage.pdf", "75d6fa216b4e2880a65ced55d17ca2b599d2606c"),
        Map.entry("testWORD_embeded.doc", "619670a74684e5aa576e2b2ee55c640842cf927e"),
        Map.entry("testHTML.html", "6548b16c5ea33e907577615ce60ca4876a3936ef"),
        Map.entry("testEXCEL_5.xls", "a174f098333c659d331317641d4d1d9d83055288"),
        Map.entry("pictures.ppt", "95bbfdbf2f60f74371285c337d3445d0acd59a9b"),
        Map.entry("testPPT_masterText2.ppt", "f5ff5e2d45ccb180cf371ed99b7dfeb2a93539b3"),
        Map.entry("testPDF-custommetadata.pdf", "a84b914655db55574e6002b6f37209ecd4c3d462"),
        Map.entry("testWORD_embeded.docx", "0e705de639f6b4fc0285d4e9451c731773ccb7f9"),
        Map.entry("testStyles.odt", "c25dd05633e3aab7132d2f5608126e2b4b03848f"),
        Map.entry("testPDF_multiFormatEmbFiles.pdf", "2103b2c30b44d5bb3aa790ab04a6741a10ea235a"),
        Map.entry("testXML2.xml", "a8c85a327716fad93faa4eb0f993057597d6f471"),
        Map.entry("testPagesComments.pages", "cbb45131cf45b9c454e754a07af3ae927b1a69cc"),
        Map.entry("testEXCEL_4.xls", "8d5e6156222151faaccb079d46ddb5393dd25771"),
        Map.entry("testWORD_no_format.doc", "88feaf03fe58ee5cc667916c6a54cbd5d605cc1c"),
        Map.entry("testPages.pages", "288e6db2f39604e372a2095257509c78dba22cbb"),
        Map.entry("footnotes.docx", "33b01b73a12f9e14efbcc340890b11ee332dca8e"),
        Map.entry("testWORD_bold_character_runs.doc", "f10e562d8825ec2e17e0d9f58646f8084a658cfa"),
        Map.entry("testWORD_custom_props.doc", "e7a737a5237a6aa9c6b3fc677eb8fa65c30d6dfe"),
        Map.entry("testPDF_Version.11.x.PDFA-1b.pdf", "71853c6197a6a7f222db0f1978c7cb232b87c5ee"),
        Map.entry("testAnnotations.pdf", "5f599e7916198540e1b52c3e472a525f50fd45f6"),
        Map.entry("tika434.html", "7d74122631f52f003a48018cc376026ccd8d984e"),
        Map.entry("testPagesHeadersFootersAlphaLower.pages", "fc1d766908134ff4689fa63fa3e91c3e9b08d975"),
        Map.entry("testRTFRegularImages.rtf", "756b1db45cb05357ceaf9c8efcf0b76e3913e190"),
        Map.entry("testRTFUmlautSpaces2.rtf", "1fcd029357062241d74d789e93477c101ff24e3f"),
        Map.entry("testWORD_numbered_list.doc", "e06656dd9b79ac970f3cd065fa8b630a4981556f"),
        Map.entry("testPPT_autodate.ppt", "05b93967ea0248ad263b2f24586e125df353fd3d"),
        Map.entry("testBulletPoints.key", "92242d67c3dbc1b22aac3f98e47061d09e7719f9"),
        Map.entry("testMasterSlideTable.key", "1d61e2fa3c3f3615500c7f72f62971391b9e9a2f"),
        Map.entry("testWORD_various.doc", "8cbdf1a4e0d78471eb90403612c4e92866acf0cb"),
        Map.entry("testEXCEL_textbox.xlsx", "1e81121e91e58a74d838e414ae0fc0055a4b4100"),
        Map.entry("big-preamble.html", "a9d759b46b6c6c1857d0d89c3a75ee2f3ace70c9"),
        Map.entry("testWORD.docx", "f72140bef19475e950e56084d1ab1cb926697b19"),
        Map.entry("testComment.rtf", "f6351d0f1f20c4ee0fff70adca6abbc6e638610e"),
        Map.entry("testRTFUnicodeUCNControlWordCharacterDoubling.rtf", "3e6f2f38682e38ffc96a476ca51bec2291a27fa7"),
        Map.entry("testPDF_Version.5.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testPPTX_Thumbnail.pptx", "6aa019154289317c7b7832fe46556e6d61cd0a9f"),
        Map.entry("testRTFTableCellSeparation.rtf", "5647290a3197c1855fad10201dc7be60ea7b0e42"),
        Map.entry("testRTFControls.rtf", "aee6afb80e8b09cf49f056020c037f70c2757e49"),
        Map.entry("testEXCEL.xls", "b5b3302499974062a7b1abd4ed523e895785b702"),
        Map.entry("testRTFJapanese.rtf", "08976f9a7d6d3a155cad84d7fa23295cb972a17a"),
        Map.entry("testPageNumber.pdf", "96b03d2cc6782eba653af28228045964e68422b5"),
        Map.entry("testOptionalHyphen.pdf", "12edd450ea76ea4e79f80ebd3442999ec2180dbc"),
        Map.entry("testPDFFileEmbInAnnotation.pdf", "97a6e5781bbaa6aea040546d797c4916f9d90c86"),
        Map.entry("testFontAfterBufferedText.rtf", "d1c8757b3ed91f2d7795234405c43005868affa3"),
        Map.entry("testPPT_masterFooter.ppt", "8c9104385820c2631ddda20814231808fac03d4d"),
        Map.entry("testWORD_various.docx", "189df989e80afb09281901aefc458c6630a8530b"),
        Map.entry("testComment.ppt", "21842dd9cb8a7d4af0f102543c192861c9789705"),
        Map.entry("testPopupAnnotation.pdf", "1717b1d16c0a4b9ff5790cac90fc8e0fba170a35"),
        Map.entry("testWORD_bold_character_runs.docx", "f10e562d8825ec2e17e0d9f58646f8084a658cfa"),
        Map.entry("testOverlappingText.pdf", "726da7d6c184512ed8d44af2a5085d65523c4572"),
        Map.entry("testRTF.rtf", "91e830ceba556741116c9e83b0c69a0d6c5c9304"),
        Map.entry("testRTFIgnoredControlWord.rtf", "1eb6a2f2fd32b1bb4227c0c02a35cb6027d9ec8c"),
        Map.entry("testComment.xls", "4de962f16452159ce302fc4a412b06a06cf9a0f6"),
        Map.entry("testPPT.ppsm", "71333ef84f7825d8ad6aba2ba993d04b4bab41c6"),
        Map.entry("boilerplate.html", "b3558f02c3179e4aeeb6057594d87bda79964e7b"),
        Map.entry("testEXCEL_embeded.xls", "110247fc0a3936828c760e40975ff83e4578be76"),
        Map.entry("testEXCEL.xlsx", "b39735e1498ec538615366b48dcfb67b558203b1"),
        Map.entry("testPPT_2imgs.ppt", "9a68072ffcf171389e78cf8bc018c4b568a6202d"),
        Map.entry("testComment.pptx", "6ae6052f469b8f901fd4fd8bc70f8e267255a58e"),
        Map.entry("testPDF_Version.6.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testPPT.pptx", "71333ef84f7825d8ad6aba2ba993d04b4bab41c6"),
        Map.entry("testPPT_custom_props.ppt", "edf196acc12701accc7be5dfe63e053436db45e6"),
        Map.entry("testPPT_embeded.pptx", "4586c710dcc8e8b780766f0e95eddd7d4138fa95"),
        Map.entry("testRTFListLibreOffice.rtf", "4c38d9e2f0a8c9a4c2cc8d2a52db9591ab759abe"),
        Map.entry("testPDF_Version.9.x.pdf", "03b60dfc8c103dbabeedfd682e979f96dd8983a2"),
        Map.entry("testRTFHexEscapeInsideWord.rtf", "6cffda07e774c55b5465d8134a0bdcb8c30f3386"),
        Map.entry("testRTFNewlines.rtf", "2375ca14e2b0d8f7ff6bbda5191544b3ee7c09fb"),
        Map.entry("testRTF-ms932.rtf", "5f9db1b83bf8e9c4c6abb065adaeb151307d33f2"),
        Map.entry("test_TIKA-1251.doc", "5a9394c34274964055fdd9272b4f7dc314b99ecf"),
        Map.entry("test_list_override.rtf", "9fe8b4a36c5222fe7ed2e9b54e2330aec8fa9423")
    );

    @Before
    public void setLocale() {
        Locale.setDefault(Locale.ENGLISH);
    }

    public void testTika292BWC() throws Exception {
        Path tikaUnzip = unzipToTemp(TIKA_FILES);
        DirectoryStream<Path> stream = Files.newDirectoryStream(tikaUnzip);

        for (Path doc : stream) {
            if (!tika292Checksums.containsKey(doc.getFileName().toString())) {
                continue;
            }

            String parsedContent = tryParse(doc);
            assertNotNull(parsedContent);
            assertFalse(parsedContent.isEmpty());
            assertEquals(tika292Checksums.get(doc.getFileName().toString()), DigestUtils.sha1Hex(parsedContent));
        }

        stream.close();
    }

    private Path unzipToTemp(String zipDir) throws Exception {
        Path tmp = createTempDir();
        DirectoryStream<Path> stream = Files.newDirectoryStream(PathUtils.get(getClass().getResource(zipDir).toURI()));

        for (Path doc : stream) {
            String filename = doc.getFileName().toString();
            TestUtil.unzip(getClass().getResourceAsStream(zipDir + filename), tmp);
        }

        stream.close();
        return tmp;
    }

    private String tryParse(Path doc) throws Exception {
        byte bytes[] = Files.readAllBytes(doc);
        return TikaImpl.parse(bytes, new Metadata(), -1);
    }
}
