/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugin.ingestion.objectstore;

import org.opensearch.plugin.ingestion.objectstore.NdjsonFramer.FramingResult;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

/**
 * Unit tests for {@link NdjsonFramer} exercising the byte-boundary ownership rule and all framing edge cases.
 */
public class NdjsonFramerTests extends OpenSearchTestCase {

    private static InputStream stream(String content) {
        return new ByteArrayInputStream(content.getBytes(StandardCharsets.UTF_8));
    }

    private static String payload(NdjsonFramer.FramedRecord record) {
        return new String(record.getPayload(), StandardCharsets.UTF_8);
    }

    public void testFramesRecordsAcrossTheReadChunkBoundary() throws Exception {
        StringBuilder content = new StringBuilder();
        int lines = 0;
        while (content.length() < 40_000) {
            content.append("line-").append(lines++).append('\n');
        }
        FramingResult result = NdjsonFramer.frame(
            stream(content.toString()),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1)
        );
        List<NdjsonFramer.FramedRecord> records = result.records();
        assertThat(records.size(), is(lines));
        assertThat(payload(records.get(0)), is("line-0"));
        assertThat(payload(records.get(lines - 1)), is("line-" + (lines - 1)));
        assertThat(records.get(lines - 1).getNextOffset(), is((long) content.length()));
        long offset = 0;
        for (int i = 0; i < lines; i++) {
            assertThat(records.get(i).getStartOffset(), is(offset));
            offset = records.get(i).getNextOffset();
        }
    }

    public void testFramesSingleRecordLongerThanTheReadChunk() throws Exception {
        String longLine = "x".repeat(30_000);
        FramingResult result = NdjsonFramer.frame(
            stream(longLine + "\nshort\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 40_000, -1)
        );
        List<NdjsonFramer.FramedRecord> records = result.records();
        assertThat(records.size(), is(2));
        assertThat(payload(records.get(0)), is(longLine));
        assertThat(records.get(0).getNextOffset(), is(30_001L));
        assertThat(payload(records.get(1)), is("short"));
        assertThat(records.get(1).getStartOffset(), is(30_001L));
    }

    public void testRecordLongerThanTheReadChunkOverCapRejected() {
        String longLine = "x".repeat(30_000);
        IOException e = expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream(longLine + "\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1))
        );
        assertThat(e.getMessage(), containsString("max_record_bytes"));
    }

    public void testFramesLinesWithCorrectOffsets() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("aa\nbbb\nc\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1)
        );
        List<NdjsonFramer.FramedRecord> records = result.records();
        assertThat(records.size(), is(3));
        assertThat(payload(records.get(0)), is("aa"));
        assertThat(records.get(0).getStartOffset(), is(0L));
        assertThat(records.get(0).getNextOffset(), is(3L));
        assertThat(payload(records.get(1)), is("bbb"));
        assertThat(records.get(1).getStartOffset(), is(3L));
        assertThat(records.get(1).getNextOffset(), is(7L));
        assertThat(payload(records.get(2)), is("c"));
        assertThat(records.get(2).getStartOffset(), is(7L));
        assertThat(records.get(2).getNextOffset(), is(9L));
    }

    public void testStripsTrailingCarriageReturn() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("a\r\nb\r\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1)
        );
        assertThat(result.records().size(), is(2));
        assertThat(payload(result.records().get(0)), is("a"));
        assertThat(payload(result.records().get(1)), is("b"));
    }

    public void testCrlfRecordsSplitAcrossSliceBoundaryNoLossNoDuplicate() throws Exception {
        FramingResult shard0 = NdjsonFramer.frame(stream("aaa\r\n"), new NdjsonFramer.FramingOptions(0, 5, true, 1024, -1));
        FramingResult shard1 = NdjsonFramer.frame(stream("bbb\r\n"), new NdjsonFramer.FramingOptions(5, Long.MAX_VALUE, false, 1024, -1));
        assertThat(shard0.records().size(), is(1));
        assertThat(payload(shard0.records().get(0)), is("aaa"));
        assertThat(shard0.records().get(0).getNextOffset(), is(5L));
        assertThat(shard1.records().size(), is(1));
        assertThat(payload(shard1.records().get(0)), is("bbb"));
        assertThat(shard1.records().get(0).getStartOffset(), is(5L));
    }

    public void testNoTrailingNewlineEmitsLastRecord() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("a\nb"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(result.records().size(), is(2));
        assertThat(payload(result.records().get(1)), is("b"));
        assertThat(result.records().get(1).getNextOffset(), is(3L));
    }

    public void testStripsBomOnlyWhenOwningOffsetZero() throws Exception {
        String withBom = "﻿a\nb\n";
        FramingResult owning = NdjsonFramer.frame(stream(withBom), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1));
        assertThat(payload(owning.records().get(0)), is("a"));

        FramingResult notOwning = NdjsonFramer.frame(stream(withBom), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(payload(notOwning.records().get(0)), is("﻿a"));
    }

    public void testBlankLinesAreSkippedAndCounted() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("a\n\n\nb\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1)
        );
        assertThat(result.records().size(), is(2));
        assertThat(result.skippedBlankLines(), is(2L));
        assertThat(payload(result.records().get(0)), is("a"));
        assertThat(payload(result.records().get(1)), is("b"));
    }

    public void testOwnershipStopsAtSliceEndButFinishesStraddlingLine() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("aaaa\nbbbb\ncccc\n"), new NdjsonFramer.FramingOptions(0, 5, false, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testLineStartingBeyondSliceEndNotOwned() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("aaaa\nbbbb\n"), new NdjsonFramer.FramingOptions(0, 4, false, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testMaxRecordsLimit() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("a\nb\nc\nd\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, 2)
        );
        assertThat(result.records().size(), is(2));
        assertThat(payload(result.records().get(1)), is("b"));
        assertThat(result.limitReached(), is(true));
    }

    public void testLimitReachedFalseWhenStreamExhausted() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("a\nb\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, 10));
        assertThat(result.records().size(), is(2));
        assertThat(result.limitReached(), is(false));
    }

    public void testMaxRecordsZeroEmitsNothing() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("a\nb\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, 0));
        assertThat(result.records().size(), is(0));
    }

    public void testEmptyStreamEmitsNothing() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream(""), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(result.records().size(), is(0));
        assertThat(result.skippedBlankLines(), is(0L));
    }

    public void testMaxSizeRecordWithCrlfIsNotRejected() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("aaaa\r\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testUnlimitedMaxRecordBytesDoesNotRejectRecords() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("aaaa\n"),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, Long.MAX_VALUE, -1)
        );
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testRecordExceedingMaxRecordBytesThrows() {
        expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream("aaaaaaaaaa\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 3, -1))
        );
    }

    public void testPayloadExactlyAtCapIsAcceptedWithLf() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("aaaa\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testPayloadJustBelowCapIsAccepted() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("aaa\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1));
        assertThat(payload(result.records().get(0)), is("aaa"));
    }

    public void testPayloadOneOverCapIsRejectedWithLf() {
        IOException e = expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream("aaaaa\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1))
        );
        assertThat(e.getMessage(), containsString("exceeds max_record_bytes [4]"));
    }

    public void testPayloadOneOverCapIsRejectedWithCrlf() {
        expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream("aaaaa\r\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1))
        );
    }

    public void testUnterminatedPayloadOverCapIsRejected() {
        expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream("aaaaa"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 4, -1))
        );
    }

    public void testBomIsNotCountedTowardTheCap() throws Exception {
        String withBom = "﻿aaaa\n";
        FramingResult result = NdjsonFramer.frame(stream(withBom), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 4, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("aaaa"));
    }

    public void testBomFollowedByOverCapPayloadIsRejected() {
        String withBom = "﻿aaaaa\n";
        expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream(withBom), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 4, -1))
        );
    }

    public void testBomStrippingAllowanceDoesNotApplyToLaterLines() {
        expectThrows(
            IOException.class,
            () -> NdjsonFramer.frame(stream("a\naaaaaaa\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 4, -1))
        );
    }

    public void testStartOffsetIsHonoredInEmittedOffsets() throws Exception {
        FramingResult result = NdjsonFramer.frame(
            stream("bbb\nccc\n"),
            new NdjsonFramer.FramingOptions(100, Long.MAX_VALUE, false, 1024, -1)
        );
        assertThat(result.records().get(0).getStartOffset(), is(100L));
        assertThat(result.records().get(0).getNextOffset(), is(104L));
        assertThat(result.records().get(1).getStartOffset(), is(104L));
    }

    public void testTrailingBlankLineWithoutNewlineIsNotEmitted() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("a\n\r"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(result.skippedBlankLines(), is(1L));
        assertThat(payload(result.records().get(0)), is("a"));
    }

    public void testAllRecordsSharePayloadOrder() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("x\ny\nz\n"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(result.records().stream().map(NdjsonFramerTests::payload).toList(), contains("x", "y", "z"));
    }

    public void testFramedRecordNullPayloadReturnsNull() {
        NdjsonFramer.FramedRecord record = new NdjsonFramer.FramedRecord(null, 0, 0);
        assertThat(record.getPayload(), is(nullValue()));
    }

    public void testFramedRecordPayloadIsCloned() {
        byte[] source = new byte[] { 1, 2, 3 };
        NdjsonFramer.FramedRecord record = new NdjsonFramer.FramedRecord(source, 0, 3);
        byte[] first = record.getPayload();
        first[0] = 9;
        assertThat(record.getPayload()[0], is((byte) 1));
    }

    public void testShortTrailingLineNotMistakenForBom() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("ab"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("ab"));
    }

    public void testBomStrippedFromTrailingRecordWithoutNewline() throws Exception {
        String withBom = "﻿abc";
        FramingResult result = NdjsonFramer.frame(stream(withBom), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("abc"));
    }

    public void testTrailingBlankLineOwningOffsetZeroIsBomOnly() throws Exception {
        byte[] bomOnly = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0xBF };
        FramingResult result = NdjsonFramer.frame(
            new ByteArrayInputStream(bomOnly),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1)
        );
        assertThat(result.records().size(), is(0));
        assertThat(result.skippedBlankLines(), is(1L));
    }

    public void testTrailingLineWithNonBomLeadingBytesNotStripped() throws Exception {
        byte[] notBom = new byte[] { (byte) 0xEF, (byte) 0xBB, (byte) 0x41 };
        FramingResult result = NdjsonFramer.frame(
            new ByteArrayInputStream(notBom),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1)
        );
        assertThat(result.records().size(), is(1));
        assertThat(result.records().get(0).getPayload().length, is(3));
    }

    public void testTrailingSingleLineNotOwningOffsetZeroKeepsBytes() throws Exception {
        FramingResult result = NdjsonFramer.frame(stream("xyz"), new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, false, 1024, -1));
        assertThat(result.records().size(), is(1));
        assertThat(payload(result.records().get(0)), is("xyz"));
    }

    public void testTrailingLineWithBomFirstByteOnlyNotStripped() throws Exception {
        byte[] partialBom = new byte[] { (byte) 0xEF, (byte) 0x41, (byte) 0x42 };
        FramingResult result = NdjsonFramer.frame(
            new ByteArrayInputStream(partialBom),
            new NdjsonFramer.FramingOptions(0, Long.MAX_VALUE, true, 1024, -1)
        );
        assertThat(result.records().size(), is(1));
        assertThat(result.records().get(0).getPayload().length, is(3));
    }
}
