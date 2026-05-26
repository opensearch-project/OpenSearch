/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.apache.commons.codec.binary.Hex;
import org.apache.commons.codec.digest.DigestUtils;
import org.apache.commons.codec.digest.MessageDigestAlgorithms;
import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32;

/**
 * End-to-end coverage for PPL cryptographic hash functions executed through the
 * DataFusion analytics backend:
 *
 * <ul>
 *   <li>{@code md5(x)} — Sig mapping {@code SqlLibraryOperators.MD5 → "md5"} resolves
 *       to DataFusion's built-in {@code md5} (Utf8 hex string output).</li>
 *   <li>{@code sha1(x)} — Sig mapping {@code SqlLibraryOperators.SHA1 → "sha1"} resolves
 *       to the opensearch-datafusion Rust UDF (wraps the {@code sha1} crate; emits a
 *       lowercase 40-character hex string).</li>
 *   <li>{@code sha2(x, bitLen)} — {@code Sha2FunctionAdapter} rewrites the call into
 *       {@code encode(digest(x, 'shaN'), 'hex')} using DataFusion built-ins for
 *       {@code bitLen ∈ {224, 256, 384, 512}}.</li>
 *   <li>{@code crc32(x)} — Sig mapping {@code SqlLibraryOperators.CRC32 → "crc32"}
 *       resolves to the opensearch-datafusion Rust UDF (wraps {@code crc32fast},
 *       IEEE 802.3 polynomial); returns BIGINT zero-extended from the u32 result so
 *       the value is always non-negative, matching PPL's contract.</li>
 * </ul>
 *
 * <p>Each test pins a single row of the {@code calcs} dataset via {@code where key='keyNN'}.
 * Field references prevent Calcite's {@code ReduceExpressionsRule} from constant-folding the
 * crypto call on the coordinator, so the hash is actually computed by the DataFusion backend.
 *
 * <p>Fixture row values (from {@code datasets/calcs/bulk.json}):
 * <ul>
 *   <li>{@code key00}: str0="FURNITURE"</li>
 *   <li>{@code key04}: str0="OFFICE SUPPLIES"</li>
 * </ul>
 */
public class CryptoFunctionsIT extends AnalyticsRestTestCase {

    private static final Dataset DATASET = new Dataset("calcs", "calcs");

    private static boolean dataProvisioned = false;

    private void ensureDataProvisioned() throws IOException {
        if (dataProvisioned == false) {
            DatasetProvisioner.provision(client(), DATASET);
            dataProvisioned = true;
        }
    }

    /** Base query template: filter to exactly one row (cardinality 1) keyed by {@code key}. */
    private String oneRow(String key) {
        return "source=" + DATASET.indexName + " | where key='" + key + "' | head 1 ";
    }

    // ── md5 ─────────────────────────────────────────────────────────────────

    /** {@code md5(str0)} on row 0 (str0="FURNITURE"). */
    public void testMd5OnColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = md5(str0) | fields v",
            DigestUtils.md5Hex("FURNITURE")
        );
    }

    /** {@code md5(str0)} on a different row, proving the call wasn't constant-folded into
     *  row-0's value at plan time. */
    public void testMd5DifferentRow() throws IOException {
        assertFirstRowString(
            oneRow("key04") + "| eval v = md5(str0) | fields v",
            DigestUtils.md5Hex("OFFICE SUPPLIES")
        );
    }

    /** Canonical RFC 1321 vector: {@code md5("abc")} → 5d41402abc4b2a76b9719d911017c592. */
    public void testMd5Hello() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = md5('hello') | fields v",
            "5d41402abc4b2a76b9719d911017c592"
        );
    }

    // ── sha1 ────────────────────────────────────────────────────────────────

    /** {@code sha1(str0)} on row 0. Returns 40-character lowercase hex string. */
    public void testSha1OnColumn() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha1(str0) | fields v",
            DigestUtils.sha1Hex("FURNITURE")
        );
    }

    /** RFC 3174 appendix A vector: {@code sha1("hello")} → aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d. */
    public void testSha1Hello() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha1('hello') | fields v",
            "aaf4c61ddcc5e8a2dabede0f3b482cd9aea9434d"
        );
    }

    /** {@code sha1(str0)} on a different row to verify field-referenced evaluation. */
    public void testSha1DifferentRow() throws IOException {
        assertFirstRowString(
            oneRow("key04") + "| eval v = sha1(str0) | fields v",
            DigestUtils.sha1Hex("OFFICE SUPPLIES")
        );
    }

    // ── sha2 (adapter-driven: encode(digest(x, 'shaN'), 'hex')) ─────────────

    /** {@code sha2(str0, 224)} — rewrites to {@code encode(digest(str0, 'sha224'), 'hex')}
     *  on the DataFusion side. */
    public void testSha2_224() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(str0, 224) | fields v",
            Hex.encodeHexString(
                DigestUtils.getDigest(MessageDigestAlgorithms.SHA_224).digest("FURNITURE".getBytes(StandardCharsets.UTF_8)))
        );
    }

    /** {@code sha2(str0, 256)}. Reference: {@link DigestUtils#sha256Hex(String)}. */
    public void testSha2_256() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(str0, 256) | fields v",
            DigestUtils.sha256Hex("FURNITURE")
        );
    }

    /** {@code sha2("hello", 256)} → 2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824 */
    public void testSha2_256_hello() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(\"hello\", 256) | fields v",
            "2cf24dba5fb0a30e26e83b2ac5b9e29e1b161e5c1fa7425e73043362938b9824"
        );
    }

    /** {@code sha2(str0, 384)}. Reference: {@link DigestUtils#sha384Hex(String)}. */
    public void testSha2_384() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(str0, 384) | fields v",
            DigestUtils.sha384Hex("FURNITURE")
        );
    }

    /** {@code sha2(str0, 512)}. Reference: {@link DigestUtils#sha512Hex(String)}. */
    public void testSha2_512() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(str0, 512) | fields v",
            DigestUtils.sha512Hex("FURNITURE")
        );
    }

    /** {@code sha2(str0, 512)} → 9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043 */
    public void testSha2_512_hello() throws IOException {
        assertFirstRowString(
            oneRow("key00") + "| eval v = sha2(\"hello\", 512) | fields v",
            "9b71d224bd62f3785d96d46ad3ea3d73319bfbc2890caadae2dff72519673ca72323c3d99ba5c11d7c7acc6e14b8c5da0c4663475c2e5c3adef46f73bcdec043"
        );
    }

    /** {@code sha2(str0, 256)} on a different row — guards against constant-folding. */
    public void testSha2_256DifferentRow() throws IOException {
        assertFirstRowString(
            oneRow("key04") + "| eval v = sha2(str0, 256) | fields v",
            DigestUtils.sha256Hex("OFFICE SUPPLIES")
        );
    }

    // ── crc32 ───────────────────────────────────────────────────────────────

    /** {@code crc32(str0)} matches {@link java.util.zip.CRC32}. */
    public void testCrc32OnColumn() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = crc32(str0) | fields v",
            crc32Reference("FURNITURE")
        );
    }

    /** {@code crc32('')} → 0. Canonical IEEE 802.3 empty-message checksum. */
    public void testCrc32EmptyInput() throws IOException {
        assertFirstRowLong(oneRow("key00") + "| eval v = crc32('') | fields v", 0L);
    }

    /** {@code crc32('123456789')} → 0xCBF43926 — canonical IEEE 802.3 reference vector. */
    public void testCrc32StandardReferenceVector() throws IOException {
        assertFirstRowLong(
            oneRow("key00") + "| eval v = crc32('123456789') | fields v",
            0xCBF43926L
        );
    }

    /** {@code crc32} must be non-negative (zero-extended u32 → i64), even for inputs whose
     *  u32 checksum has the high bit set. {@code "a"} → 0xE8B7BE43 qualifies. */
    public void testCrc32AlwaysNonNegative() throws IOException {
        Object cell = firstRowFirstCell(oneRow("key00") + "| eval v = crc32('a') | fields v");
        assertTrue("crc32 must be numeric, got: " + cell, cell instanceof Number);
        long value = ((Number) cell).longValue();
        assertEquals(0xE8B7BE43L, value);
        assertTrue("crc32 must be non-negative (zero-extended u32), got: " + value, value >= 0);
    }

    // ── helpers ─────────────────────────────────────────────────────────────

    /** Java-side CRC32 reference that matches the Rust UDF's zero-extended {@code u32 → i64} contract.
     *  {@link java.util.zip.CRC32#getValue()} already returns the unsigned 32-bit value widened to long. */
    private static long crc32Reference(String input) {
        CRC32 crc = new CRC32();
        crc.update(input.getBytes(StandardCharsets.UTF_8));
        return crc.getValue();
    }

    private void assertFirstRowString(String ppl, String expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertNotNull("Expected non-null result for query [" + ppl + "]", cell);
        assertEquals("Value mismatch for query: " + ppl, expected, cell);
    }

    private void assertFirstRowLong(String ppl, long expected) throws IOException {
        Object cell = firstRowFirstCell(ppl);
        assertTrue("Expected numeric result for query [" + ppl + "] but got: " + cell, cell instanceof Number);
        assertEquals("Value mismatch for query: " + ppl, expected, ((Number) cell).longValue());
    }

    private Object firstRowFirstCell(String ppl) throws IOException {
        Map<String, Object> response = executePpl(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) response.get("rows");
        assertNotNull("Response missing 'rows' for query: " + ppl, rows);
        assertTrue("Expected at least one row for query: " + ppl, rows.size() >= 1);
        return rows.get(0).get(0);
    }

    private Map<String, Object> executePpl(String ppl) throws IOException {
        ensureDataProvisioned();
        Request request = new Request("POST", "/_analytics/ppl");
        request.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(request);
        return assertOkAndParse(response, "PPL: " + ppl);
    }
}
