/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobMetadata;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.index.store.RemoteSegmentStoreDirectory;
import org.opensearch.index.translog.transfer.TranslogTransferMetadata;
import org.opensearch.test.OpenSearchTestCase;

import java.math.BigInteger;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.opensearch.index.remote.RemoteStoreUtils.URL_BASE64_CHARSET;
import static org.opensearch.index.remote.RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding;
import static org.opensearch.index.remote.RemoteStoreUtils.longToUrlBase64;
import static org.opensearch.index.remote.RemoteStoreUtils.urlBase64ToLong;
import static org.opensearch.index.remote.RemoteStoreUtils.verifyNoMultipleWriters;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils.METADATA_PREFIX;
import static org.opensearch.index.store.RemoteSegmentStoreDirectory.MetadataFilenameUtils.SEPARATOR;
import static org.opensearch.index.translog.transfer.TranslogTransferMetadata.METADATA_SEPARATOR;

public class RemoteStoreUtilsTests extends OpenSearchTestCase {

    private static Map<Character, Integer> BASE64_CHARSET_IDX_MAP;

    static {
        Map<Character, Integer> charToIndexMap = new HashMap<>();
        for (int i = 0; i < URL_BASE64_CHARSET.length; i++) {
            charToIndexMap.put(URL_BASE64_CHARSET[i], i);
        }
        BASE64_CHARSET_IDX_MAP = Collections.unmodifiableMap(charToIndexMap);
    }

    private final String metadataFilename = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        1,
        1,
        "node-1"
    );

    private final String metadataFilenameDup = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        23,
        34,
        2,
        1,
        "node-2"
    );
    private final String metadataFilename2 = RemoteSegmentStoreDirectory.MetadataFilenameUtils.getMetadataFilename(
        12,
        13,
        34,
        1,
        1,
        "node-1"
    );

    private final String oldMetadataFilename = getOldSegmentMetadataFilename(12, 23, 34, 1, 1);

    /*
    Gives segment metadata filename for <2.11 version
     */
    public static String getOldSegmentMetadataFilename(
        long primaryTerm,
        long generation,
        long translogGeneration,
        long uploadCounter,
        int metadataVersion
    ) {
        return String.join(
            SEPARATOR,
            METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(translogGeneration),
            RemoteStoreUtils.invertLong(uploadCounter),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(metadataVersion)
        );
    }

    public static String getOldTranslogMetadataFilename(long primaryTerm, long generation, int metadataVersion) {
        return String.join(
            METADATA_SEPARATOR,
            METADATA_PREFIX,
            RemoteStoreUtils.invertLong(primaryTerm),
            RemoteStoreUtils.invertLong(generation),
            RemoteStoreUtils.invertLong(System.currentTimeMillis()),
            String.valueOf(metadataVersion)
        );
    }

    public void testInvertToStrInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong(-1));
    }

    public void testInvertToStrValid() {
        assertEquals("9223372036854774573", RemoteStoreUtils.invertLong(1234));
        assertEquals("0000000000000001234", RemoteStoreUtils.invertLong(9223372036854774573L));
    }

    public void testInvertToLongInvalid() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.invertLong("-5"));
    }

    public void testInvertToLongValid() {
        assertEquals(1234, RemoteStoreUtils.invertLong("9223372036854774573"));
        assertEquals(9223372036854774573L, RemoteStoreUtils.invertLong("0000000000000001234"));
    }

    public void testinvert() {
        assertEquals(0, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(0)));
        assertEquals(Long.MAX_VALUE, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(Long.MAX_VALUE)));
        for (int i = 0; i < 10; i++) {
            long num = randomLongBetween(1, Long.MAX_VALUE);
            assertEquals(num, RemoteStoreUtils.invertLong(RemoteStoreUtils.invertLong(num)));
        }
    }

    public void testGetSegmentNameForCfeFile() {
        assertEquals("_foo", RemoteStoreUtils.getSegmentName("_foo.cfe"));
    }

    public void testGetSegmentNameForDvmFile() {
        assertEquals("_bar", RemoteStoreUtils.getSegmentName("_bar_1_Lucene90_0.dvm"));
    }

    public void testGetSegmentNameWeirdSegmentNameOnlyUnderscore() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("_.dvm"));
    }

    public void testGetSegmentNameUnderscoreDelimiterOverrides() {
        // Validate behaviour when segment name contains delimiters only
        assertEquals("_", RemoteStoreUtils.getSegmentName("___.dvm"));
    }

    public void testGetSegmentNameException() {
        assertThrows(IllegalArgumentException.class, () -> RemoteStoreUtils.getSegmentName("dvd"));
    }

    public void testVerifyMultipleWriters_Segment() {
        List<String> mdFiles = new ArrayList<>();
        mdFiles.add(metadataFilename);
        mdFiles.add(metadataFilename2);
        mdFiles.add(oldMetadataFilename);
        verifyNoMultipleWriters(mdFiles, RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen);

        mdFiles.add(metadataFilenameDup);
        assertThrows(
            IllegalStateException.class,
            () -> verifyNoMultipleWriters(mdFiles, RemoteSegmentStoreDirectory.MetadataFilenameUtils::getNodeIdByPrimaryTermAndGen)
        );
    }

    public void testVerifyMultipleWriters_Translog() throws InterruptedException {
        TranslogTransferMetadata tm = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename = tm.getFileName();
        Thread.sleep(1);
        TranslogTransferMetadata tm2 = new TranslogTransferMetadata(1, 1, 1, 2, "node--1");
        String mdFilename2 = tm2.getFileName();
        List<BlobMetadata> bmList = new LinkedList<>();
        bmList.add(new PlainBlobMetadata(mdFilename, 1));
        bmList.add(new PlainBlobMetadata(mdFilename2, 1));
        bmList.add(new PlainBlobMetadata(getOldTranslogMetadataFilename(1, 1, 1), 1));
        RemoteStoreUtils.verifyNoMultipleWriters(
            bmList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
            TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
        );

        bmList = new LinkedList<>();
        bmList.add(new PlainBlobMetadata(mdFilename, 1));
        TranslogTransferMetadata tm3 = new TranslogTransferMetadata(1, 1, 1, 2, "node--2");
        bmList.add(new PlainBlobMetadata(tm3.getFileName(), 1));
        List<BlobMetadata> finalBmList = bmList;
        assertThrows(
            IllegalStateException.class,
            () -> RemoteStoreUtils.verifyNoMultipleWriters(
                finalBmList.stream().map(BlobMetadata::name).collect(Collectors.toList()),
                TranslogTransferMetadata::getNodeIdByPrimaryTermAndGen
            )
        );
    }

    public void testLongToBase64() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "syVHd0gGq0w",
            -5878421770170594047L,
            "rmumi5UPDQE",
            -5147010836697060622L,
            "uJIk6f-V6vI",
            937096430362711837L,
            "DQE8PQwOVx0",
            8422273604115462710L,
            "dOHtOEZzejY",
            -2528761975013221124L,
            "3OgIYbXSXPw",
            -5512387536280560513L,
            "s4AQvdu03H8",
            -5749656451579835857L,
            "sDUd65cNCi8",
            5569654857969679538L,
            "TUtjlYLPvLI",
            -1563884000447039930L,
            "6kv3yZNv9kY"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            String base64Str = longToUrlBase64(entry.getKey());
            assertEquals(entry.getValue(), base64Str);
            assertEquals(11, entry.getValue().length());
            assertEquals((long) entry.getKey(), urlBase64ToLong(base64Str));
        }

        int iters = randomInt(100);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            String base64Str = longToUrlBase64(value);
            assertEquals(value, urlBase64ToLong(base64Str));
        }
    }

    public void testLongToCompositeUrlBase64AndBinaryEncodingUsing20Bits() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "s11001001010100",
            -5878421770170594047L,
            "r10011010111010",
            -5147010836697060622L,
            "u00100100100010",
            937096430362711837L,
            "D01000000010011",
            8422273604115462710L,
            "d00111000011110",
            -2528761975013221124L,
            "300111010000000",
            -5512387536280560513L,
            "s11100000000001",
            -5749656451579835857L,
            "s00001101010001",
            5569654857969679538L,
            "T01010010110110",
            -1563884000447039930L,
            "610010010111111"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            String base64Str = RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(entry.getKey(), 20);
            assertEquals(entry.getValue(), base64Str);
            assertEquals(15, entry.getValue().length());
            assertEquals(longToUrlBase64(entry.getKey()).charAt(0), base64Str.charAt(0));
        }

        int iters = randomInt(1000);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            assertEquals(RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(value, 20).charAt(0), longToUrlBase64(value).charAt(0));
        }
    }

    public void testLongToCompositeUrlBase64AndBinaryEncoding() {
        Map<Long, String> longToExpectedBase64String = Map.of(
            -5537941589147079860L,
            "s1100100101010001110111011101001000000001101010101101001100",
            -5878421770170594047L,
            "r1001101011101001101000101110010101000011110000110100000001",
            -5147010836697060622L,
            "u0010010010001001001110100111111111100101011110101011110010",
            937096430362711837L,
            "D0100000001001111000011110100001100000011100101011100011101",
            8422273604115462710L,
            "d0011100001111011010011100001000110011100110111101000110110",
            -2528761975013221124L,
            "30011101000000010000110000110110101110100100101110011111100",
            -5512387536280560513L,
            "s1110000000000100001011110111011011101101001101110001111111",
            -5749656451579835857L,
            "s0000110101000111011110101110010111000011010000101000101111",
            5569654857969679538L,
            "T0101001011011000111001010110000010110011111011110010110010",
            -1563884000447039930L,
            "61001001011111101111100100110010011011011111111011001000110"
        );
        for (Map.Entry<Long, String> entry : longToExpectedBase64String.entrySet()) {
            Long hashValue = entry.getKey();
            String expectedCompositeEncoding = entry.getValue();
            String actualCompositeEncoding = longToCompositeBase64AndBinaryEncoding(hashValue, 64);
            assertEquals(expectedCompositeEncoding, actualCompositeEncoding);
            assertEquals(59, expectedCompositeEncoding.length());
            assertEquals(longToUrlBase64(entry.getKey()).charAt(0), actualCompositeEncoding.charAt(0));
            assertEquals(RemoteStoreUtils.longToCompositeBase64AndBinaryEncoding(hashValue, 20), actualCompositeEncoding.substring(0, 15));

            Long computedHashValue = compositeUrlBase64BinaryEncodingToLong(actualCompositeEncoding);
            assertEquals(hashValue, computedHashValue);
        }

        int iters = randomInt(1000);
        for (int i = 0; i < iters; i++) {
            long value = randomLong();
            String compositeEncoding = longToCompositeBase64AndBinaryEncoding(value, 64);
            assertEquals(value, compositeUrlBase64BinaryEncodingToLong(compositeEncoding));
        }
    }

    static long compositeUrlBase64BinaryEncodingToLong(String encodedValue) {
        char ch = encodedValue.charAt(0);
        int base64BitsIntValue = BASE64_CHARSET_IDX_MAP.get(ch);
        String base64PartBinary = Integer.toBinaryString(base64BitsIntValue);
        String binaryString = base64PartBinary + encodedValue.substring(1);
        return new BigInteger(binaryString, 2).longValue();
    }
}
