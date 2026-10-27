/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.test.OpenSearchTestCase;

import java.nio.charset.StandardCharsets;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;

/**
 * Unit tests for {@link ManifestParser}, covering the valid case and one failing case per hard-reject rule.
 */
public class ManifestParserTests extends OpenSearchTestCase {

    public void testParsesValidManifest() {
        ObjectStoreManifest manifest = ManifestParser.parse(
            "[{\"key\":\"b.ndjson\",\"size\":20},{\"key\":\"a.ndjson\",\"size\":10}]",
            100L
        );
        assertThat(manifest.objectCount(), is(2));
        assertThat(manifest.keys(), contains("a.ndjson", "b.ndjson"));
        assertThat(manifest.sizeOf("a.ndjson"), is(10L));
        assertThat(manifest.sizeOf("b.ndjson"), is(20L));
    }

    public void testParsesEmptyArray() {
        ObjectStoreManifest manifest = ManifestParser.parse("[]", 100L);
        assertThat(manifest.objectCount(), is(0));
    }

    public void testFractionalSizeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":1.5}]", 100L)
        );
        assertThat(e.getMessage(), containsString("non-negative integer"));
    }

    public void testEmptyKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"\",\"size\":10}]", 100L)
        );
        assertThat(e.getMessage(), containsString("must not be empty or blank"));
    }

    public void testBlankKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"   \",\"size\":10}]", 100L)
        );
        assertThat(e.getMessage(), containsString("must not be empty or blank"));
    }

    public void testNonStringKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":123,\"size\":10}]", 100L)
        );
        assertThat(e.getMessage(), containsString("must be a JSON string"));
    }

    public void testOutOfLongRangeSizeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":99999999999999999999999999}]", 100L)
        );
        assertThat(e.getMessage(), containsString("non-negative integer"));
    }

    public void testNullManifestRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse(null, 100L));
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testBlankManifestRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse("   ", 100L));
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testMalformedJsonRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse("[{\"key\":", 100L));
        assertThat(e.getMessage(), containsString("manifest"));
    }

    public void testNonArrayRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("{\"key\":\"a\",\"size\":1}", 100L)
        );
        assertThat(e.getMessage(), containsString("JSON array"));
    }

    public void testNonObjectEntryRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse("[\"a\"]", 100L));
        assertThat(e.getMessage(), containsString("JSON objects"));
    }

    public void testNegativeSizeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":-1}]", 100L)
        );
        assertThat(e.getMessage(), containsString("must be >= 0"));
    }

    public void testDuplicateKeyRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":1},{\"key\":\"a\",\"size\":2}]", 100L)
        );
        assertThat(e.getMessage(), containsString("duplicate key"));
    }

    public void testEntryMissingSizeRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse("[{\"key\":\"a\"}]", 100L));
        assertThat(e.getMessage(), containsString("size"));
    }

    public void testEntryMissingKeyRejected() {
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse("[{\"size\":5}]", 100L));
        assertThat(e.getMessage(), containsString("key"));
    }

    public void testCountExactlyAtMaxObjectsAllowed() {
        ObjectStoreManifest manifest = ManifestParser.parse("[{\"key\":\"a\",\"size\":1},{\"key\":\"b\",\"size\":2}]", 2L);
        assertThat(manifest.objectCount(), is(2));
    }

    public void testCardinalityExceedingMaxObjectsRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":1},{\"key\":\"b\",\"size\":2}]", 1L)
        );
        assertThat(e.getMessage(), containsString("cardinality"));
    }

    public void testUnknownEntryFieldsSkipped() {
        ObjectStoreManifest manifest = ManifestParser.parse("[{\"etag\":\"x\",\"key\":\"a\",\"size\":5,\"nested\":{\"z\":1}}]", 100L);
        assertThat(manifest.sizeOf("a"), is(5L));
    }

    public void testIntSizeAccepted() {
        assertThat(ManifestParser.parse("[{\"key\":\"a\",\"size\":5}]", 100L).sizeOf("a"), is(5L));
    }

    public void testLongSizeBeyondIntRangeAccepted() {
        long size = 5_000_000_000L;
        assertThat(ManifestParser.parse("[{\"key\":\"a\",\"size\":" + size + "}]", 100L).sizeOf("a"), is(size));
    }

    public void testStringSizeRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":\"5\"}]", 100L)
        );
        assertThat(e.getMessage(), containsString("must be a non-negative integer"));
    }

    public void testTrailingContentAfterTheArrayRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":1}] [{\"key\":\"b\",\"size\":2}]", 100L)
        );
        assertThat(e.getMessage(), containsString("single JSON array and nothing after it"));
    }

    public void testTrailingGarbageAfterTheArrayRejected() {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse("[{\"key\":\"a\",\"size\":1}]}}}", 100L)
        );
        assertThat(e.getMessage(), containsString(ObjectStoreSourceConfig.MANIFEST));
    }

    public void testByteCapCountsUnpairedSurrogatesExactlyAsUtf8Does() {
        StringBuilder manifest = new StringBuilder("[");
        int entry = 0;
        while (manifest.toString().getBytes(StandardCharsets.UTF_8).length <= ObjectStoreSourceConfig.MAX_MANIFEST_BYTES) {
            if (entry > 0) {
                manifest.append(',');
            }
            manifest.append("{\"key\":\"\uD83Dpart-").append(entry++).append("-\uDE00.ndjson\",\"size\":10}");
        }
        manifest.append(']');
        String json = manifest.toString();
        long expected = json.getBytes(StandardCharsets.UTF_8).length;
        IllegalArgumentException e = expectThrows(IllegalArgumentException.class, () -> ManifestParser.parse(json, 1000000L));
        assertThat(e.getMessage(), containsString("is [" + expected + "] bytes"));
    }

    public void testMultiByteKeysAreMeasuredAgainstTheByteCap() {
        String key = "dump/caf\u00e9/\u4e2d\u6587/\uD83D\uDE00.ndjson";
        ObjectStoreManifest manifest = ManifestParser.parse("[{\"key\":\"" + key + "\",\"size\":10}]", 100L);
        assertThat(manifest.sizeOf(key), is(10L));
    }

    public void testMultiByteManifestOverTheByteCapRejected() {
        StringBuilder manifest = new StringBuilder("[");
        int entry = 0;
        while (manifest.length() <= ObjectStoreSourceConfig.MAX_MANIFEST_BYTES) {
            if (entry > 0) {
                manifest.append(',');
            }
            manifest.append("{\"key\":\"\u4e2d\u6587-").append(entry++).append(".ndjson\",\"size\":10}");
        }
        manifest.append("]\uD83D");
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> ManifestParser.parse(manifest.toString(), 1000000L)
        );
        assertThat(e.getMessage(), containsString("byte cap"));
    }

}
