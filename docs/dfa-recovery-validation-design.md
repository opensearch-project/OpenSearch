# Design: Format-Aware File Validation

Minimal-change design to close validation gaps G1–G5 for non-Lucene DataFormat-Aware files.

Companion to `dfa-recovery-validation-analysis.md`.

## Goals

1. Reuse existing recovery code paths. No new recovery protocol, no new transfer handler.
2. Route validation through a single per-format SPI. One dispatch point per gap.
3. Lucene behavior unchanged (Lucene path goes through the same SPI with an adapter).
4. Formats that don't implement structural validation degrade gracefully to checksum-only.

## Non-goals

- Replacing `CheckIndex` for Lucene.
- Adding a new remote-transfer protocol.
- Verifying bytes in flight for remote-store downloads (remains the `check_on_startup` knob's job).

## 1. Core SPI — two-tier validation mirroring Lucene

Lucene has two validation tiers, triggered at different moments:

| Lucene tier | Method | When | Cost |
|---|---|---|---|
| Quick | `CodecUtil.retrieveChecksum` | Dedupe, metadata comparison, upload | O(1) — reads footer only |
| Full-body | `CodecUtil.checksumEntireFile` | Inline during write/receive, `check_on_startup=checksum`, error recovery | O(n) — reads every byte |

Existing on-disk files are never re-validated in steady state. Full-body validation runs only at **entry events** (write, receive, restore) and at the opt-in startup check. DFA must match this model exactly — no stronger guarantee, no weaker one.

Accordingly, the SPI exposes two operations:

```java
public interface FormatIntegrityStrategy extends FormatChecksumStrategy {

    /**
     * Quick checksum — Lucene's CodecUtil.retrieveChecksum analogue.
     * Lucene: reads footer (8 bytes).
     * Parquet etc.: reads PrecomputedChecksumStrategy cache, falls back to full scan on miss.
     * Used by: dedupe (localDirectoryContains), upload, metadata comparison.
     */
    long quickChecksum(Directory dir, String fileName) throws IOException;

    /**
     * Full-body verification — Lucene's CodecUtil.checksumEntireFile analogue.
     * Always re-reads every byte on disk; compares computed CRC against expected.checksum().
     * Used by: Store.checkIntegrity, peer-recovery error path, streaming verifier's verify().
     */
    void verifyIntegrity(Directory dir, String fileName, StoreFileMetadata expected) throws IOException;

    /**
     * Streaming verifier for inline validation during file receipt.
     * Returned VerifyingIndexOutput validates bytes as they are written and calls
     * verify() at close time.
     */
    VerifyingIndexOutput newVerifyingOutput(IndexOutput out, StoreFileMetadata expected);
}
```

Two implementations:

- **`LuceneIntegrityStrategy`**
  - `quickChecksum` → `CodecUtil.retrieveChecksum` (existing fast path).
  - `verifyIntegrity` → `CodecUtil.checksumEntireFile` + length + footer check (moves the current body of `Store.checkIntegrity` here).
  - `newVerifyingOutput` → existing `LuceneVerifyingIndexOutput`.

- **`PrecomputedIntegrityStrategy`** (supersedes `PrecomputedChecksumStrategy` + `GenericCRC32ChecksumHandler`)
  - `quickChecksum` → look up pre-computed cache; on miss, fall back to full CRC32 scan.
  - `verifyIntegrity` → always full CRC32 scan; compare against `expected.checksum()`.
  - `newVerifyingOutput` → `CRC32VerifyingIndexOutput` (running CRC32 over every write, verify at close).

The pre-computed cache is authoritative for the `quickChecksum` tier only — exactly like Lucene's footer is authoritative for its quick tier. Both can be stale relative to unread disk regions; neither is used on the full-body path. This is the same guarantee Lucene provides today.

## 2. Strategy ownership — `DataFormatDescriptor` carries it

Each format's `DataFormatDescriptor` already carries a `FormatChecksumStrategy`. Widen that field to `FormatIntegrityStrategy` so the descriptor becomes the single source of truth for a format's validation behavior:

```java
public class DataFormatDescriptor {
    private final String formatName;
    private final FormatIntegrityStrategy integrityStrategy;   // was FormatChecksumStrategy

    public FormatIntegrityStrategy getIntegrityStrategy() { return integrityStrategy; }
}
```

Format plugins return their own descriptor with their own strategy. Each format fully owns its quick/full/streaming behavior — Parquet can plug in a Parquet-aware streaming verifier, Arrow a different one, without touching `Store` or `MultiFileWriter`.

## 3. Dispatcher — directory encapsulates lookup

`DataFormatAwareStoreDirectory` exposes validation operations directly. Callers never see `FormatIntegrityStrategy`. Mirrors the existing `calculateUploadChecksum` pattern.

```java
public class DataFormatAwareStoreDirectory extends FilterDirectory {

    public long quickChecksum(String name) throws IOException {
        return resolveStrategy(name).quickChecksum(this, name);
    }

    public void verifyIntegrity(String name, StoreFileMetadata expected) throws IOException {
        resolveStrategy(name).verifyIntegrity(this, name, expected);
    }

    public VerifyingIndexOutput newVerifyingOutput(String name, IndexOutput raw, StoreFileMetadata md) {
        return resolveStrategy(name).newVerifyingOutput(raw, md);
    }

    private FormatIntegrityStrategy resolveStrategy(String fileName) {
        FileMetadata fm = toFileMetadata(fileName);
        DataFormatDescriptor desc = formatRegistry.get(fm.dataFormat());
        return desc != null ? desc.getIntegrityStrategy() : DEFAULT_LUCENE_STRATEGY;
    }
}
```

Non-DFA indices: `DataFormatAwareStoreDirectory.unwrap(dir)` returns null, and call sites fall back to the current hardcoded Lucene path. Zero behavior change.

Call-site shape (applies to `Store.checkIntegrity`, `Store.createVerifyingOutput`, etc.):

```java
DataFormatAwareStoreDirectory dfasd = DataFormatAwareStoreDirectory.unwrap(directory);
if (dfasd != null) {
    dfasd.verifyIntegrity(md.name(), md);
} else {
    // existing Lucene path
}
```

## 4. Change list (minimal edits)

| Gap | File | Change |
|---|---|---|
| G1 | `Store.java` | In `checkIntegrity(md, directory)` — if `DataFormatAwareStoreDirectory.unwrap(directory) != null`, delegate to `dfasd.verifyIntegrity(md.name(), md)`. Else keep existing code. ~6 lines. |
| G2 | Deferred | Structural validation (Parquet magic / per-chunk CRC) requires a separate hook on `DataFormatDescriptor` (`verifyStructure`). Not required for this PR — deferred until a format needs it. `Store.checkIndex` remains Lucene-only. |
| G3 | `Store.java` | In `createVerifyingOutput(fileName, metadata, ctx)` — if `unwrap() != null`, return `dfasd.newVerifyingOutput(fileName, rawOutput, metadata)` instead of hardcoding `LuceneVerifyingIndexOutput`. ~5 lines. |
| G4 | No code change | `SegmentFileTransferHandler` already calls `Store.checkIntegrityNoException(md)` which now dispatches correctly via G1. |
| G5 | `DataFormatAwareStoreDirectory.java` | Add `markReadFailureAsCorruption(String fileName, IOException cause)` helper that reader plugins can call — delegates to `store.markStoreCorrupted`. Per-format readers call it in their catch blocks. ~8 lines + per-format reader wiring (one line per reader). |

Total: ~30 lines of server changes + two new strategy classes (~50 lines each, mostly boilerplate).

## 5. Why this is minimal

- `FormatChecksumStrategy` already exists and is already registered per format — we only extend it. No new registry, no new plugin API.
- Every call site (`checkIntegrity`, `checkIndex`, `createVerifyingOutput`) already exists. We add a single `if (dfasd != null)` branch at each, keeping the Lucene fast path as the `else`.
- `MultiFileWriter`, `BlobStoreRepository.restoreFile`, `SegmentFileTransferHandler` — **no changes needed**. They already call `Store.createVerifyingOutput` / `checkIntegrityNoException`, which now dispatch correctly.
- No new network messages, no new on-disk format, no new commit data.

## 6. Streaming CRC32 verifier (new class, ~40 lines)

```java
final class CRC32VerifyingIndexOutput extends VerifyingIndexOutput {
    private final StoreFileMetadata metadata;
    private final CRC32 crc = new CRC32();
    private long writtenBytes;

    CRC32VerifyingIndexOutput(IndexOutput out, StoreFileMetadata metadata) {
        super(out);
        this.metadata = metadata;
    }

    @Override public void writeByte(byte b) throws IOException {
        super.writeByte(b);
        crc.update(b & 0xFF);
        writtenBytes++;
    }

    @Override public void writeBytes(byte[] b, int off, int len) throws IOException {
        super.writeBytes(b, off, len);
        crc.update(b, off, len);
        writtenBytes += len;
    }

    @Override public void verify() throws IOException {
        String actual = Long.toString(crc.getValue());
        if (writtenBytes != metadata.length() || !actual.equals(metadata.checksum())) {
            throw new CorruptIndexException(
                "DFA file verification failed: expected=" + metadata.checksum()
                + " actual=" + actual + " length=" + writtenBytes + "/" + metadata.length(),
                metadata.name());
        }
    }
}
```

Same contract as `LuceneVerifyingIndexOutput` but driven by running CRC32 over every byte rather than comparing footer bytes. Works for any format whose upload-side strategy is `GenericCRC32ChecksumHandler`. Validation happens in `verify()`, called by `Store.verify(IndexOutput)` at the end of chunk transfer (existing contract).

## 7. Lucene-path adapter (new class, ~30 lines)

```java
public class LuceneIntegrityStrategy implements FormatIntegrityStrategy {
    @Override public long quickChecksum(Directory dir, String f) throws IOException {
        try (IndexInput in = dir.openInput(f, IOContext.READONCE)) {
            return CodecUtil.retrieveChecksum(in);
        }
    }
    @Override public void verifyIntegrity(Directory dir, String f, StoreFileMetadata md) throws IOException {
        // existing body of Store.checkIntegrity, verbatim
    }
    @Override public VerifyingIndexOutput newVerifyingOutput(IndexOutput out, StoreFileMetadata md) {
        return new Store.LuceneVerifyingIndexOutput(md, out);
    }
}
```

Registered as the default fallback in `DataFormatAwareStoreDirectory`, so bare (un-prefixed) filenames route through the same SPI as DFA files but keep the current Lucene-codec semantics.

## 8. Test plan

| Test | What it asserts |
|---|---|
| `DataFormatAwareCheckIntegrityTests` | `Store.checkIntegrity` on a Parquet file with correct CRC32 passes; with one flipped byte fails with `CorruptIndexException`. |
| `DataFormatAwarePeerRecoveryIT` | Peer recovery of a DFA index with a corrupted file in flight triggers `CorruptIndexException` and shard failure (not silent success). |
| `CheckOnStartupChecksumIT` | Shard with `index.shard.check_on_startup=checksum` on a DFA index passes when healthy, fails-and-marks-corrupt when a DFA file is tampered with after commit. |
| `LuceneIntegrityRegressionTests` | Existing `StoreTests.testCheckIntegrity` and `LuceneVerifyingIndexOutputTests` continue to pass — verifies zero regression via the adapter. |
| `CRC32VerifyingIndexOutputTests` | Unit: correct bytes pass; truncation / byte-flip / length-mismatch fail. |

## 9. Rollout and compatibility

- Feature-gated behind `isPluggableDataFormatEnabled()` at each dispatch site — if the index isn't DFA, zero behavior change.
- No commit data changes, no network wire changes, no on-disk layout changes.
- Can be shipped in one PR per gap; G1 and G3 are the minimum set for correct recovery. G2 and G5 are quality-of-life.

## 10. Open questions (deferred)

- **Per-format structural checker plugin point**: today `verifyStructure` default is no-op. A future enhancement could let format plugins register a deeper checker (e.g. Parquet magic bytes + column-chunk CRCs). Doesn't block this design.
- **Inline validation during remote-store download**: today no validation during download. Would need a new `VerifyingIndexInput` at the `Directory.copyFrom` layer. Out of scope — `check_on_startup=checksum` provides the escape hatch.

## 11. Summary

Three ~5-line edits in `Store.java` + one new SPI method + two strategy classes close the DFA validation gaps while preserving every existing recovery code path. Lucene behavior is identical (adapter pattern). DFA files gain the same correctness guarantees that Lucene files get today.
