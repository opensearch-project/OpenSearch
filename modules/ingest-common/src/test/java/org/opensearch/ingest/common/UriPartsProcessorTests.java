/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ingest.common;

import org.opensearch.ingest.CompoundProcessor;
import org.opensearch.ingest.IngestDocument;
import org.opensearch.ingest.Processor;
import org.opensearch.ingest.RandomDocumentPicks;
import org.opensearch.ingest.TestProcessor;
import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.nullValue;
import static org.hamcrest.Matchers.sameInstance;

public class UriPartsProcessorTests extends OpenSearchTestCase {

    private static final String FIELD = "request_url";
    private static final String TARGET = "url";

    // ---- the example from the feature request ----

    public void testFeatureRequestExample() throws Exception {
        String url = "https://alice@shop.example.com:8443/assets/logo.png?size=large#top";
        Map<String, Object> expected = new HashMap<>();
        expected.put("original", url);
        expected.put("scheme", "https");
        expected.put("domain", "shop.example.com");
        expected.put("port", 8443);
        expected.put("path", "/assets/logo.png");
        expected.put("extension", "png");
        expected.put("query", "size=large");
        expected.put("fragment", "top");
        expected.put("user_info", "alice");
        expected.put("username", "alice");

        IngestDocument document = run(defaultProcessor(), document(FIELD, url));

        assertThat(document.getFieldValue(FIELD, String.class), equalTo(url));
        assertThat(parts(document), equalTo(expected));
    }

    public void testPortIsAnInteger() throws Exception {
        Object port = parts("https://example.com:8443/").get("port");
        assertThat(port, instanceOf(Integer.class));
        assertThat(port, equalTo(8443));
    }

    // ---- user info ----

    public void testUserInfoWithPasswordIsSplit() throws Exception {
        Map<String, Object> parts = parts("ftp://alice:s3cret@files.example.com/pub/a.zip");
        assertThat(parts.get("user_info"), equalTo("alice:s3cret"));
        assertThat(parts.get("username"), equalTo("alice"));
        assertThat(parts.get("password"), equalTo("s3cret"));
    }

    public void testPasswordMayContainColons() throws Exception {
        Map<String, Object> parts = parts("http://alice:pa:ss:word@example.com/");
        assertThat(parts.get("user_info"), equalTo("alice:pa:ss:word"));
        assertThat(parts.get("username"), equalTo("alice"));
        assertThat(parts.get("password"), equalTo("pa:ss:word"));
    }

    public void testUserInfoWithoutPasswordHasNoPasswordField() throws Exception {
        Map<String, Object> parts = parts("http://alice@example.com/");
        assertThat(parts.get("username"), equalTo("alice"));
        assertThat(parts.containsKey("password"), equalTo(false));
    }

    public void testEmptyPasswordIsOmitted() throws Exception {
        Map<String, Object> parts = parts("http://alice:@example.com/");
        assertThat(parts.get("user_info"), equalTo("alice:"));
        assertThat(parts.get("username"), equalTo("alice"));
        assertThat(parts.containsKey("password"), equalTo(false));
    }

    public void testEmptyUsernameIsOmitted() throws Exception {
        Map<String, Object> parts = parts("http://:s3cret@example.com/");
        assertThat(parts.get("user_info"), equalTo(":s3cret"));
        assertThat(parts.containsKey("username"), equalTo(false));
        assertThat(parts.get("password"), equalTo("s3cret"));
    }

    public void testEmptyUserInfoIsOmitted() throws Exception {
        Map<String, Object> parts = parts("http://@example.com/");
        assertThat(parts.containsKey("user_info"), equalTo(false));
        assertThat(parts.containsKey("username"), equalTo(false));
        assertThat(parts.containsKey("password"), equalTo(false));
    }

    public void testUserInfoIsNotDecoded() throws Exception {
        Map<String, Object> parts = parts("http://al%40ice:p%3Ass@example.com/");
        assertThat(parts.get("user_info"), equalTo("al%40ice:p%3Ass"));
        assertThat(parts.get("username"), equalTo("al%40ice"));
        assertThat(parts.get("password"), equalTo("p%3Ass"));
    }

    // ---- host and port ----

    public void testPortIsNotInferredFromScheme() throws Exception {
        assertThat(parts("https://example.com/").containsKey("port"), equalTo(false));
        assertThat(parts("http://example.com/").containsKey("port"), equalTo(false));
        assertThat(parts("ftp://example.com/").containsKey("port"), equalTo(false));
    }

    public void testExplicitDefaultPortIsReported() throws Exception {
        assertThat(parts("https://example.com:443/").get("port"), equalTo(443));
        assertThat(parts("http://example.com:80/").get("port"), equalTo(80));
    }

    public void testEmptyPortIsOmitted() throws Exception {
        Map<String, Object> parts = parts("http://example.com:/p");
        assertThat(parts.containsKey("port"), equalTo(false));
        assertThat(parts.get("domain"), equalTo("example.com"));
    }

    public void testPortAtTheUpperBoundIsAccepted() throws Exception {
        assertThat(parts("http://example.com:65535/").get("port"), equalTo(65535));
        assertThat(parts("http://example.com:0/").get("port"), equalTo(0));
    }

    public void testPortAboveTheValidRangeFails() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parts("http://example.com:65536/"));
        assertThat(e.getMessage(), containsString("port [65536] is out of range"));
        assertThat(e.getMessage(), containsString(FIELD));
    }

    public void testIpv4Host() throws Exception {
        Map<String, Object> parts = parts("http://10.0.0.7:9200/_cat/indices");
        assertThat(parts.get("domain"), equalTo("10.0.0.7"));
        assertThat(parts.get("port"), equalTo(9200));
    }

    public void testIpv6HostKeepsItsBrackets() throws Exception {
        Map<String, Object> parts = parts("http://[2001:db8::1]:8080/status");
        assertThat(parts.get("domain"), equalTo("[2001:db8::1]"));
        assertThat(parts.get("port"), equalTo(8080));
        assertThat(parts.get("path"), equalTo("/status"));
    }

    public void testIpv6HostWithoutPort() throws Exception {
        Map<String, Object> parts = parts("https://[::1]/");
        assertThat(parts.get("domain"), equalTo("[::1]"));
        assertThat(parts.containsKey("port"), equalTo(false));
    }

    public void testIpv6HostWithZoneId() throws Exception {
        assertThat(parts("http://[fe80::1%25eth0]/").get("domain"), equalTo("[fe80::1%25eth0]"));
    }

    public void testHostIsNotCaseNormalized() throws Exception {
        Map<String, Object> parts = parts("HTTPS://Example.COM/A.PNG");
        assertThat(parts.get("scheme"), equalTo("HTTPS"));
        assertThat(parts.get("domain"), equalTo("Example.COM"));
        assertThat(parts.get("extension"), equalTo("PNG"));
    }

    // ---- percent-encoding ----

    public void testPathQueryAndFragmentAreNotDecoded() throws Exception {
        Map<String, Object> parts = parts("http://example.com/a%20b/c%2Fd.txt?q=a%26b%3Dc#x%20y");
        assertThat(parts.get("path"), equalTo("/a%20b/c%2Fd.txt"));
        assertThat(parts.get("query"), equalTo("q=a%26b%3Dc"));
        assertThat(parts.get("fragment"), equalTo("x%20y"));
        assertThat(parts.get("extension"), equalTo("txt"));
    }

    public void testFullyEncodedInputIsNotDecoded() throws Exception {
        String encoded = "https%3A%2F%2Fexample.com%2Fa.png";
        Map<String, Object> parts = parts(encoded);
        assertThat(parts.get("original"), equalTo(encoded));
        assertThat(parts.get("path"), equalTo(encoded));
        assertThat(parts.containsKey("scheme"), equalTo(false));
        assertThat(parts.containsKey("domain"), equalTo(false));
    }

    public void testNonAsciiPathIsKeptAsWritten() throws Exception {
        Map<String, Object> parts = parts("http://example.com/café.html?q=ü");
        assertThat(parts.get("path"), equalTo("/café.html"));
        assertThat(parts.get("extension"), equalTo("html"));
        assertThat(parts.get("query"), equalTo("q=ü"));
    }

    // ---- relative and scheme-less input ----

    public void testAbsolutePathReference() throws Exception {
        Map<String, Object> parts = parts("/assets/logo.png?size=large#top");
        Map<String, Object> expected = new HashMap<>();
        expected.put("original", "/assets/logo.png?size=large#top");
        expected.put("path", "/assets/logo.png");
        expected.put("extension", "png");
        expected.put("query", "size=large");
        expected.put("fragment", "top");
        assertThat(parts, equalTo(expected));
    }

    public void testNetworkPathReferenceHasNoScheme() throws Exception {
        Map<String, Object> parts = parts("//example.com:81/x");
        assertThat(parts.containsKey("scheme"), equalTo(false));
        assertThat(parts.get("domain"), equalTo("example.com"));
        assertThat(parts.get("port"), equalTo(81));
        assertThat(parts.get("path"), equalTo("/x"));
    }

    public void testQueryOnlyAndFragmentOnlyReferences() throws Exception {
        assertThat(parts("?a=b").keySet(), containsInAnyOrder("original", "query"));
        assertThat(parts("#top").keySet(), containsInAnyOrder("original", "fragment"));
    }

    public void testSchemeLessHostIsNotGuessedAndIsReadAsPath() throws Exception {
        Map<String, Object> parts = parts("example.com/index.html");
        assertThat(parts.keySet(), containsInAnyOrder("original", "path", "extension"));
        assertThat(parts.get("path"), equalTo("example.com/index.html"));
        assertThat(parts.get("extension"), equalTo("html"));

        // a bare host name is a relative path too, so its last label is reported as the extension
        parts = parts("example.com");
        assertThat(parts.containsKey("domain"), equalTo(false));
        assertThat(parts.get("path"), equalTo("example.com"));
        assertThat(parts.get("extension"), equalTo("com"));
    }

    public void testFileUriWithoutAuthority() throws Exception {
        Map<String, Object> parts = parts("file:///var/log/syslog.1");
        assertThat(parts.get("scheme"), equalTo("file"));
        assertThat(parts.get("path"), equalTo("/var/log/syslog.1"));
        assertThat(parts.get("extension"), equalTo("1"));
        assertThat(parts.containsKey("domain"), equalTo(false));
    }

    public void testOpaqueUrisAreRejected() {
        for (String opaque : new String[] { "mailto:alice@example.com", "urn:isbn:0451450523", "localhost:8080/path", "example.com:443" }) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parts(opaque));
            assertThat(opaque, e.getMessage(), containsString("opaque"));
            assertThat(opaque, e.getMessage(), containsString(FIELD));
        }
    }

    // ---- absent and empty components ----

    public void testUrlWithOnlySchemeAndHost() throws Exception {
        Map<String, Object> parts = parts("http://example.com");
        assertThat(parts.keySet(), containsInAnyOrder("original", "scheme", "domain"));
    }

    public void testRootPathHasNoExtension() throws Exception {
        Map<String, Object> parts = parts("http://example.com/");
        assertThat(parts.get("path"), equalTo("/"));
        assertThat(parts.containsKey("extension"), equalTo(false));
    }

    public void testEmptyQueryAndFragmentAreOmitted() throws Exception {
        Map<String, Object> parts = parts("http://example.com/p?#");
        assertThat(parts.keySet(), containsInAnyOrder("original", "scheme", "domain", "path"));
    }

    // ---- extension ----

    public void testExtensionIsDerivedFromTheLastPathSegmentOnly() throws Exception {
        Map<String, String> expectations = new LinkedHashMap<>();
        expectations.put("/assets/logo.png", "png");
        expectations.put("/a/b.tar.gz", "gz");
        expectations.put("/a/b..txt", "txt");
        expectations.put("/A.PNG", "PNG");
        expectations.put("/a/.hidden.txt", "txt");
        expectations.put("/file.png?other=x.txt", "png");
        expectations.put("/a/b.png/c", null);
        expectations.put("/dir.d/file", null);
        expectations.put("/dir.d/", null);
        expectations.put("/a/b.png/", null);
        expectations.put("/a/.htaccess", null);
        expectations.put("/a/.", null);
        expectations.put("/a/..", null);
        expectations.put("/a/b.", null);
        expectations.put("/file", null);
        expectations.put("/a/b%2Epng", null);
        // the extension is cut from the raw path: it is neither decoded nor normalized
        expectations.put("/a/b.%70ng", "%70ng");
        // path parameters are not part of the file name
        expectations.put("/a.html;jsessionid=ABC123", "html");
        expectations.put("/a.html;jsessionid=AB.C", "html");
        expectations.put("/a.html;", "html");
        expectations.put("/dir;v=1.2/file", null);
        expectations.put("/dir/;x.y", null);
        expectations.put("/a;b.c", null);
        expectations.put("/", null);
        expectations.put("", null);
        for (Map.Entry<String, String> entry : expectations.entrySet()) {
            String url = "http://example.com" + entry.getKey();
            Map<String, Object> parts = parts(url);
            assertThat(url, parts.get("extension"), equalTo(entry.getValue()));
        }
    }

    public void testExtensionOfARelativePath() throws Exception {
        assertThat(parts("logo.png").get("extension"), equalTo("png"));
        assertThat(parts("images/logo").get("extension"), nullValue());
    }

    // ---- invalid input ----

    public void testInvalidUrisFailWithAClearError() {
        String[] invalid = new String[] {
            "http://exa mple.com/",
            "http://example.com/a b",
            "http://example.com/?q=a|b",
            "http://example.com/%zz",
            "http://my_host/",
            "http://münchen.de/",
            "http://example.com:port/",
            "http://example.com:99999999999/",
            "http://[::1/",
            "http://",
            "http://user@/path",
            " http://example.com/ " };
        for (String url : invalid) {
            IngestDocument document = document(FIELD, url);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> defaultProcessor().execute(document));
            assertThat(url, e.getMessage(), containsString("unable to parse"));
            assertThat(url, e.getMessage(), containsString("[" + FIELD + "]"));
            // the message must be self-contained, because on_failure only exposes the message of the root cause, but it must not
            // repeat the input, which may carry credentials
            assertThat(url, e.getMessage(), containsString(" at index "));
            assertThat(url, e.getMessage(), not(containsString(url.trim())));
            assertThat(url, e.getCause(), nullValue());
            assertThat("failed documents are left untouched: " + url, document.hasField(TARGET), equalTo(false));
        }
    }

    public void testErrorMessagesDoNotLeakCredentialsOrTokens() {
        String[] failing = new String[] {
            "http://alice:s3cret@example.com/a b?token=abc123",
            "http://alice:s3cret@exa_mple.com/?token=abc123",
            "http://alice:s3cret@example.com:70000/?token=abc123",
            "mailto:alice:s3cret@example.com?token=abc123" };
        for (String url : failing) {
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parts(url));
            for (String secret : new String[] { "alice", "s3cret", "abc123", "example.com" }) {
                assertThat(url, e.getMessage(), not(containsString(secret)));
            }
            assertThat(url, e.getMessage(), containsString("unable to parse field [" + FIELD + "] as a URI"));
        }
    }

    public void testEmptyStringFails() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> parts(""));
        assertThat(e.getMessage(), containsString("field [" + FIELD + "] is empty"));
    }

    public void testFailureDoesNotRemoveTheSourceField() {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, true, true, false);
        IngestDocument document = document(FIELD, "http://exa mple.com/");
        expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(document.getFieldValue(FIELD, String.class), equalTo("http://exa mple.com/"));
        assertThat(document.hasField(TARGET), equalTo(false));
    }

    public void testNonStringFieldFails() {
        for (Object value : new Object[] { 42, true, List.of("http://example.com/"), Map.of("a", "b") }) {
            IngestDocument document = document(FIELD, value);
            IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> defaultProcessor().execute(document));
            assertThat(e.getMessage(), containsString("cannot be cast to [java.lang.String]"));
        }
    }

    // ---- missing and null fields ----

    public void testMissingFieldFails() {
        IngestDocument document = document("other", "x");
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> defaultProcessor().execute(document));
        assertThat(e.getMessage(), containsString("field [" + FIELD + "] not present as part of path [" + FIELD + "]"));
    }

    public void testNullFieldFails() {
        IngestDocument document = document(FIELD, null);
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> defaultProcessor().execute(document));
        assertThat(e.getMessage(), containsString("field [" + FIELD + "] is null"));
    }

    public void testIgnoreMissingSkipsAMissingField() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, true, true, true);
        IngestDocument document = document("other", "x");
        Map<String, Object> before = new HashMap<>(document.getSourceAndMetadata());
        assertThat(processor.execute(document), sameInstance(document));
        assertThat(document.getSourceAndMetadata(), equalTo(before));
    }

    public void testIgnoreMissingSkipsANullField() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, true, true, true);
        IngestDocument document = document(FIELD, null);
        processor.execute(document);
        assertThat(document.hasField(TARGET), equalTo(false));
        assertThat(document.hasField(FIELD), equalTo(true));
    }

    public void testIgnoreMissingDoesNotHideAnInvalidValue() {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, true, false, true);
        IngestDocument invalid = document(FIELD, "http://exa mple.com/");
        expectThrows(IllegalArgumentException.class, () -> processor.execute(invalid));
        IngestDocument wrongType = document(FIELD, 42);
        expectThrows(IllegalArgumentException.class, () -> processor.execute(wrongType));
    }

    // ---- options ----

    public void testKeepOriginalFalseOmitsOriginal() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, false, false, false);
        IngestDocument document = run(processor, document(FIELD, "http://example.com/a.png"));
        assertThat(parts(document).keySet(), containsInAnyOrder("scheme", "domain", "path", "extension"));
        assertThat(document.getFieldValue(FIELD, String.class), equalTo("http://example.com/a.png"));
    }

    public void testRemoveIfSuccessfulRemovesTheSourceField() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, TARGET, true, true, false);
        IngestDocument document = run(processor, document(FIELD, "http://example.com/a.png"));
        assertThat(document.hasField(FIELD), equalTo(false));
        assertThat(parts(document).get("original"), equalTo("http://example.com/a.png"));
        assertThat(parts(document).get("domain"), equalTo("example.com"));
    }

    public void testSourceFieldIsKeptByDefault() throws Exception {
        IngestDocument document = run(defaultProcessor(), document(FIELD, "http://example.com/a.png"));
        assertThat(document.getFieldValue(FIELD, String.class), equalTo("http://example.com/a.png"));
    }

    public void testCustomTargetField() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, "parsed", true, false, false);
        IngestDocument document = run(processor, document(FIELD, "http://example.com/a.png"));
        assertThat(document.hasField(TARGET), equalTo(false));
        assertThat(document.getFieldValue("parsed.domain", String.class), equalTo("example.com"));
        assertThat(document.getFieldValue("parsed.original", String.class), equalTo("http://example.com/a.png"));
    }

    public void testNestedSourceAndTargetFields() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "http.request.url", "event.url", true, false, false);
        IngestDocument document = new IngestDocument(
            new HashMap<>(Map.of("http", new HashMap<>(Map.of("request", new HashMap<>(Map.of("url", "https://example.com:444/x.js")))))),
            Map.of()
        );
        run(processor, document);
        assertThat(document.getFieldValue("event.url.scheme", String.class), equalTo("https"));
        assertThat(document.getFieldValue("event.url.port", Integer.class), equalTo(444));
        assertThat(document.getFieldValue("event.url.extension", String.class), equalTo("js"));
    }

    // ---- target_field that already exists ----

    public void testExistingObjectAtTargetIsReplaced() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD, "http://example.com/");
        source.put(TARGET, new HashMap<>(Map.of("query", "stale=1", "custom", "value")));
        IngestDocument document = run(defaultProcessor(), RandomDocumentPicks.randomIngestDocument(random(), source));
        assertThat(parts(document).containsKey("query"), equalTo(false));
        assertThat(parts(document).containsKey("custom"), equalTo(false));
        assertThat(parts(document).get("domain"), equalTo("example.com"));
    }

    public void testExistingScalarAtTargetIsReplaced() throws Exception {
        Map<String, Object> source = new HashMap<>();
        source.put(FIELD, "http://example.com/");
        source.put(TARGET, "old");
        IngestDocument document = run(defaultProcessor(), RandomDocumentPicks.randomIngestDocument(random(), source));
        assertThat(parts(document).get("domain"), equalTo("example.com"));
    }

    public void testFieldAndTargetFieldMayBeTheSame() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "url", "url", true, false, false);
        IngestDocument document = run(processor, document("url", "http://example.com/a.png"));
        assertThat(parts(document).get("original"), equalTo("http://example.com/a.png"));
        assertThat(parts(document).get("domain"), equalTo("example.com"));
    }

    public void testFieldMayLiveInsideTheTargetField() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "url.original", "url", true, false, false);
        IngestDocument document = new IngestDocument(
            new HashMap<>(Map.of("url", new HashMap<>(Map.of("original", "http://example.com/a.png", "full", "kept?")))),
            Map.of()
        );
        run(processor, document);
        assertThat(parts(document).get("original"), equalTo("http://example.com/a.png"));
        assertThat(parts(document).get("domain"), equalTo("example.com"));
        assertThat(parts(document).containsKey("full"), equalTo(false));
    }

    public void testParentOfTargetThatIsNotAnObjectFails() {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, FIELD, "other.url", true, false, false);
        IngestDocument document = document(FIELD, "http://example.com/");
        document.setFieldValue("other", "a string");
        expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
    }

    public void testTargetBelowTheSourceFieldFailsWithoutRemovingTheSource() {
        // the source holds the URL string, so it cannot also be the parent object of the target: setting the target fails first and
        // the removal is never reached, which is why only a target at or above the source needs the creation-time check
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "a", "a.b", true, true, false);
        IngestDocument document = document("a", "http://example.com/");
        expectThrows(IllegalArgumentException.class, () -> processor.execute(document));
        assertThat(document.getFieldValue("a", String.class), equalTo("http://example.com/"));
    }

    // ---- constructor validation ----

    public void testRemoveIfSuccessfulIsRejectedWhenItWouldRemoveTheOutput() {
        for (String field : new String[] { "url", "url.original" }) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new UriPartsProcessor(null, null, field, "url", randomBoolean(), true, randomBoolean())
            );
            assertThat(e.getMessage(), containsString("[remove_if_successful]"));
        }
    }

    public void testRemoveIfSuccessfulIsRejectedWhenPathsDifferOnlyByTheSourcePrefix() throws Exception {
        // IngestDocument ignores a leading "_source.", so these pairs address the same field or a field below it
        String[][] overlapping = new String[][] {
            { "_source.url", "url" },
            { "url", "_source.url" },
            { "_source.url", "_source.url" },
            { "_source.url.original", "url" },
            { "url.original", "_source.url" },
            { "_source.url.original", "_source.url" },
            { "_ingest.url", "_ingest.url" },
            { "_ingest.url.original", "_ingest.url" } };
        for (String[] pair : overlapping) {
            IllegalArgumentException e = expectThrows(
                IllegalArgumentException.class,
                () -> new UriPartsProcessor(null, null, pair[0], pair[1], randomBoolean(), true, randomBoolean())
            );
            assertThat(e.getMessage(), containsString("[remove_if_successful]"));
            assertThat(e.getMessage(), containsString("[" + pair[0] + "]"));
            assertThat(e.getMessage(), containsString("[" + pair[1] + "]"));
        }
    }

    public void testSourcePrefixedPathsAreResolvedLikeUnprefixedOnes() throws Exception {
        UriPartsProcessor processor = new UriPartsProcessor(null, null, "_source." + FIELD, "_source.parsed", true, true, false);
        IngestDocument document = run(processor, document(FIELD, "http://example.com/a.png"));
        assertThat(document.hasField(FIELD), equalTo(false));
        assertThat(document.getFieldValue("parsed.domain", String.class), equalTo("example.com"));
    }

    public void testRemoveIfSuccessfulIsAllowedForDifferentRoots() {
        // "_ingest." addresses the ingest metadata, which is a different root than the source, so nothing can overlap
        new UriPartsProcessor(null, null, "_ingest.url", "url", true, true, false);
        new UriPartsProcessor(null, null, "url", "_ingest.url", true, true, false);
        new UriPartsProcessor(null, null, "_source._ingest.url", "_ingest.url", true, true, false);
        new UriPartsProcessor(null, null, "_ingest.url", "_source._ingest.url", true, true, false);
        new UriPartsProcessor(null, null, "_source.urls", "url", true, true, false);
        new UriPartsProcessor(null, null, "_source.url2", "_source.url", true, true, false);
    }

    public void testRemoveIfSuccessfulIsAllowedForUnrelatedFields() {
        new UriPartsProcessor(null, null, "urls", "url", true, true, false);
        new UriPartsProcessor(null, null, "url", "url2", true, true, false);
        new UriPartsProcessor(null, null, "a.b", "a.c", true, true, false);
    }

    // ---- common processor options ----

    public void testFailureIsHandedToOnFailureProcessors() throws Exception {
        Processor onFailure = new TestProcessor(
            document -> document.setFieldValue("error", document.getIngestMetadata().get(CompoundProcessor.ON_FAILURE_MESSAGE_FIELD))
        );
        CompoundProcessor pipeline = new CompoundProcessor(false, List.of(defaultProcessor()), List.of(onFailure));
        IngestDocument document = executeSuccessfully(pipeline, document(FIELD, "http://exa mple.com/"));
        assertThat(document.getFieldValue("error", String.class), containsString("unable to parse field [" + FIELD + "] as a URI"));
        assertThat(document.getFieldValue("error", String.class), containsString(" at index "));
        assertThat(document.hasField(TARGET), equalTo(false));
    }

    public void testIgnoreFailureLetsTheDocumentThrough() throws Exception {
        CompoundProcessor pipeline = new CompoundProcessor(true, List.of(defaultProcessor()), List.of());
        IngestDocument document = executeSuccessfully(pipeline, document(FIELD, "http://exa mple.com/"));
        assertThat(document.getFieldValue(FIELD, String.class), equalTo("http://exa mple.com/"));
        assertThat(document.hasField(TARGET), equalTo(false));
    }

    public void testTagAndDescription() {
        UriPartsProcessor processor = new UriPartsProcessor("my-tag", "my description", FIELD, TARGET, true, false, false);
        assertThat(processor.getType(), equalTo("uri_parts"));
        assertThat(processor.getTag(), equalTo("my-tag"));
        assertThat(processor.getDescription(), equalTo("my description"));
    }

    // ---- helpers ----

    private static UriPartsProcessor defaultProcessor() {
        return new UriPartsProcessor(randomAlphaOfLength(5), null, FIELD, TARGET, true, false, false);
    }

    private static IngestDocument document(String field, Object value) {
        Map<String, Object> source = new HashMap<>();
        source.put(field, value);
        return RandomDocumentPicks.randomIngestDocument(random(), source);
    }

    private static IngestDocument executeSuccessfully(CompoundProcessor pipeline, IngestDocument document) {
        AtomicReference<IngestDocument> result = new AtomicReference<>();
        AtomicReference<Exception> failure = new AtomicReference<>();
        pipeline.execute(document, (resultDocument, exception) -> {
            result.set(resultDocument);
            failure.set(exception);
        });
        assertThat(failure.get(), nullValue());
        return result.get();
    }

    private static IngestDocument run(UriPartsProcessor processor, IngestDocument document) throws Exception {
        processor.execute(document);
        return document;
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> parts(IngestDocument document) {
        return (Map<String, Object>) document.getFieldValue(TARGET, Map.class);
    }

    private static Map<String, Object> parts(String url) throws Exception {
        return parts(run(defaultProcessor(), document(FIELD, url)));
    }
}
