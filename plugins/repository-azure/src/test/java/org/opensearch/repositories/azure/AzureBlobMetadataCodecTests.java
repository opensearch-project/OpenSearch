/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.repositories.azure;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.Map;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.empty;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.everyItem;
import static org.hamcrest.Matchers.lessThanOrEqualTo;
import static org.hamcrest.Matchers.matchesPattern;

public class AzureBlobMetadataCodecTests extends OpenSearchTestCase {

    public void testRoundTripAsciiAndUnicode() throws Exception {
        final Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("ckp-data", "AQIDBA==");
        metadata.put("ключ-🔑", "checkpoint-値-🙂");
        metadata.put("", "");

        assertThat(AzureBlobMetadataCodec.decode(AzureBlobMetadataCodec.encode(metadata)), equalTo(metadata));
    }

    public void testWireKeysAndValuesAreAzureSafeAscii() {
        final Map<String, String> encoded = AzureBlobMetadataCodec.encode(Map.of("ckp-data", "checkpoint-値-🙂"));

        assertThat(encoded.keySet(), everyItem(matchesPattern("[a-z_][a-z0-9_]*")));
        assertThat(encoded.values(), everyItem(matchesPattern("[\\x00-\\x7f]*")));
        assertTrue(encoded.keySet().iterator().next().startsWith(AzureBlobMetadataCodec.KEY_PREFIX));
    }

    public void testCheckpointDataRoundTrip() throws Exception {
        final byte[] checkpoint = randomByteArrayOfLength(1024);
        final Map<String, String> metadata = Map.of("ckp-data", Base64.getEncoder().encodeToString(checkpoint));

        final Map<String, String> encoded = AzureBlobMetadataCodec.encode(metadata);
        assertThat(AzureBlobMetadataCodec.decode(encoded), equalTo(metadata));
        assertThat(AzureBlobMetadataCodec.encodedMetadataSize(encoded), lessThanOrEqualTo(AzureBlobMetadataCodec.MAX_METADATA_SIZE));
    }

    public void testMalformedOwnedKeyPrefixAndVersionAreRejected() {
        assertDecodeFails(Map.of("opensearch_metadata_v1", "v1_"), "key codec");
        assertDecodeFails(Map.of("opensearch_metadata_v2_61", "v1_YQ=="), "key codec");
        assertDecodeFails(Map.of("OPENSEARCH_METADATA_future", "v1_YQ=="), "key codec");
    }

    public void testOddAndNonLowercaseHexSuffixesAreRejected() {
        assertDecodeFails(Map.of(AzureBlobMetadataCodec.KEY_PREFIX + "6", "v1_YQ=="), "odd-length");
        assertDecodeFails(Map.of(AzureBlobMetadataCodec.KEY_PREFIX + "gg", "v1_YQ=="), "non-lowercase-hex");
        assertDecodeFails(Map.of(AzureBlobMetadataCodec.KEY_PREFIX + "6A", "v1_YQ=="), "non-lowercase-hex");
    }

    public void testInvalidUtf8IsRejected() {
        assertDecodeFails(Map.of(AzureBlobMetadataCodec.KEY_PREFIX + "ff", "v1_YQ=="), "valid UTF-8");
        assertDecodeFails(Map.of(AzureBlobMetadataCodec.KEY_PREFIX + "61", "v1_/w=="), "valid UTF-8");
    }

    public void testMalformedAndUnknownValueCodecsAreRejected() {
        final String key = AzureBlobMetadataCodec.KEY_PREFIX + "61";
        assertDecodeFails(Map.of(key, "v2_YQ=="), "value codec");
        assertDecodeFails(Map.of(key, "v1_%%%"), "Malformed Base64");
        assertDecodeFails(Map.of(key, "v1_YQ"), "Non-canonical Base64");
    }

    public void testDuplicateDecodedKeysAreRejectedCaseInsensitively() {
        final Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put(AzureBlobMetadataCodec.KEY_PREFIX + "61", "v1_eA==");
        metadata.put(AzureBlobMetadataCodec.KEY_PREFIX.toUpperCase(java.util.Locale.ROOT) + "61", "v1_eQ==");

        assertDecodeFails(metadata, "Duplicate decoded");
    }

    public void testInvalidLogicalStringsAreRejected() {
        expectThrows(IllegalArgumentException.class, () -> AzureBlobMetadataCodec.encode(Map.of("\ud800", "value")));
        expectThrows(IllegalArgumentException.class, () -> AzureBlobMetadataCodec.encode(Map.of("key", "\udfff")));
    }

    public void testExternalMetadataIsFiltered() throws Exception {
        final Map<String, String> metadata = new LinkedHashMap<>();
        metadata.put("external-owner", "someone");
        metadata.put("other", "value");

        assertThat(AzureBlobMetadataCodec.decode(metadata).entrySet(), empty());
    }

    public void testAggregateMetadataSizeBoundary() {
        final String wireKey = AzureBlobMetadataCodec.KEY_PREFIX + "6b";
        final int fixedSize = AzureBlobMetadataCodec.encodedMetadataSize(Map.of(wireKey, "v1_"));
        final int maximumEncodedPayload = ((AzureBlobMetadataCodec.MAX_METADATA_SIZE - fixedSize) / 4) * 4;
        final int maximumRawValueSize = maximumEncodedPayload / 4 * 3;

        final Map<String, String> accepted = AzureBlobMetadataCodec.encode(Map.of("k", "a".repeat(maximumRawValueSize)));
        assertThat(AzureBlobMetadataCodec.encodedMetadataSize(accepted), lessThanOrEqualTo(AzureBlobMetadataCodec.MAX_METADATA_SIZE));

        final IllegalArgumentException exception = expectThrows(
            IllegalArgumentException.class,
            () -> AzureBlobMetadataCodec.encode(Map.of("k", "a".repeat(maximumRawValueSize + 1)))
        );
        assertThat(exception.getMessage(), containsString("exceeds"));
    }

    public void testAggregateLimitAppliesAcrossEntries() {
        assertThat(
            AzureBlobMetadataCodec.encodedMetadataSize(AzureBlobMetadataCodec.encode(Map.of("a", "a".repeat(3050)))),
            lessThanOrEqualTo(AzureBlobMetadataCodec.MAX_METADATA_SIZE)
        );
        assertThat(
            AzureBlobMetadataCodec.encodedMetadataSize(AzureBlobMetadataCodec.encode(Map.of("b", "b".repeat(3050)))),
            lessThanOrEqualTo(AzureBlobMetadataCodec.MAX_METADATA_SIZE)
        );
        expectThrows(
            IllegalArgumentException.class,
            () -> AzureBlobMetadataCodec.encode(Map.of("a", "a".repeat(3050), "b", "b".repeat(3050)))
        );
    }

    public void testV1WireFormatAndCaseInsensitivePrefix() throws Exception {
        final Map<String, String> metadata = Map.of("ключ", "value");
        assertEquals(Map.of("opensearch_metadata_v1_d0bad0bbd18ed187", "v1_dmFsdWU="), AzureBlobMetadataCodec.encode(metadata));
        assertEquals(metadata, AzureBlobMetadataCodec.decode(Map.of("OPENSEARCH_METADATA_V1_d0bad0bbd18ed187", "v1_dmFsdWU=")));
    }

    private static void assertDecodeFails(Map<String, String> metadata, String message) {
        final IOException exception = expectThrows(IOException.class, () -> AzureBlobMetadataCodec.decode(metadata));
        assertThat(exception.getMessage(), containsString(message));
    }
}
