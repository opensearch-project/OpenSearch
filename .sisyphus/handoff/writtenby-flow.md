# StoreFileMetadata.writtenBy — Complete Data-Flow Report

## 1. DEFINITION

**File:** `server/src/main/java/org/opensearch/index/store/StoreFileMetadata.java`  
**Line 63:** `private final Version writtenBy;` (type: `org.apache.lucene.util.Version`)  
**Accessor (line 168):** `public Version writtenBy() { return writtenBy; }`  
**Constructor (line 71):** `StoreFileMetadata(String name, long length, String checksum, Version writtenBy, BytesRef hash)`  
**Invariant (line 75):** `Objects.requireNonNull(writtenBy, "writtenBy must not be null")`

---

## 2. PRODUCERS (where writtenBy is SET)

### 2.1 Store.java — loadMetadata (SegmentInfos path)
**File:** `server/src/main/java/org/opensearch/index/store/Store.java`  
- **Line 1404:** `builder.put(file, new StoreFileMetadata(file, length, checksum, version, fileHash.get()));`  
- **Line 1449:** `builder.put(file, new StoreFileMetadata(file, length, checksum, version, fileHash.get()));`  
- **Line 1469:** `builder.put(file, new StoreFileMetadata(file, length, checksum, version));`  
- **Source expression:** `version` comes from `info.info.getVersion()` (per-segment Lucene version from SegmentInfo)

### 2.2 Store.java — loadMetadata (CatalogSnapshot path)
**File:** `server/src/main/java/org/opensearch/index/store/Store.java`  
- **Line 1348:** calls `checksumFromFile(directory, file, builder, logger, version, isSiFile)`  
- **Source expression:** `version = catalogSnapshot.getFormatVersionForFile(file)` (line 1342)  
- **For DataformatAwareCatalogSnapshot:** returns `Version.LATEST` (line 241 of DataformatAwareCatalogSnapshot.java)  
- **For SegmentInfosCatalogSnapshot:** returns actual per-segment Lucene version from `segmentFileVersionMap`

### 2.3 Store.java — directory listing fallback
**File:** `server/src/main/java/org/opensearch/index/store/Store.java`  
- **Line 483:** `result.put(file, new StoreFileMetadata(file, length, checksum, org.opensearch.Version.CURRENT.luceneVersion));`  
- **Source:** hardcoded to current OpenSearch's Lucene version

### 2.4 RemoteStoreReplicationSource.java
**File:** `server/src/main/java/org/opensearch/indices/replication/RemoteStoreReplicationSource.java`  
- **Line 110:** `new StoreFileMetadata(e.getValue().getOriginalFilename(), ..., version, null)`  
- **Source (line 75):** `version = catalogSnapshotRef.get().getCommitDataFormatVersion()`  
  - DataformatAwareCatalogSnapshot returns `Version.LATEST` (line 252)
  - SegmentInfosCatalogSnapshot returns `segmentInfos.getCommitLuceneVersion()` (line 182)

### 2.5 RemoteSegmentMetadata.java
**File:** `server/src/main/java/org/opensearch/index/store/remote/metadata/RemoteSegmentMetadata.java`  
- **Line 183:** `new StoreFileMetadata(entry.getKey(), ..., Version.LATEST)`  
- **Source:** hardcoded `Version.LATEST`

### 2.6 SwitchableIndexInput.java
**File:** `server/src/main/java/org/opensearch/storage/indexinput/SwitchableIndexInput.java`  
- **Line 430:** `new StoreFileMetadata(fileName, ..., Version.LATEST)`  
- **Source:** hardcoded `Version.LATEST`

### 2.7 CompositeDirectory.java
**File:** `server/src/main/java/org/opensearch/index/store/CompositeDirectory.java`  
- **Line 345:** `new StoreFileMetadata(name, ..., Version.LATEST)`  
- **Source:** hardcoded `Version.LATEST`

### 2.8 BlobStoreIndexShardSnapshot.java (snapshot restore)
**File:** `server/src/main/java/org/opensearch/index/snapshots/blobstore/BlobStoreIndexShardSnapshot.java`  
- **Line 352:** `new StoreFileMetadata(physicalName, length, checksum, writtenBy, metaHash)`  
- **Source:** parsed from JSON field `"written_by"` in snapshot metadata (line 321-323)

### 2.9 FileChunkRequest.java (recovery transport)
**File:** `server/src/main/java/org/opensearch/indices/recovery/FileChunkRequest.java`  
- **Line 72:** `metadata = new StoreFileMetadata(name, length, checksum, writtenBy);`  
- **Source (line 70):** `Lucene.parseVersionLenient(in.readString(), null)` — deserialized from wire

### 2.10 RemoteSegmentStoreDirectory — setWrittenByMajor
**File:** `server/src/main/java/org/opensearch/index/store/RemoteSegmentStoreDirectory.java`  
- **Line 877:** `metadata.setWrittenByMajor(catalogSnapshot.getFormatVersionForFile(metadata.originalFilename).major)`  
- **Line 400:** Validates: `writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR`  
- **CRITICAL:** This is a range check that would REJECT any version outside [MIN_SUPPORTED_MAJOR, LATEST.major]

---

## 3. CONSUMERS (where writtenBy is READ)

### Category (a) — Logging/Display/Serialization
| File | Line | Usage |
|------|------|-------|
| StoreFileMetadata.java | 162 | `toString()` |
| BlobStoreIndexShardSnapshot.java | 278-279 | JSON serialization: `builder.field(WRITTEN_BY, file.metadata.writtenBy())` |
| FileChunkRequest.java | 142 | Wire serialization: `out.writeString(metadata.writtenBy().toString())` |
| Store.java | 725, 748 | `assert metadata.writtenBy() != null` (assertions only) |

### Category (b) — Equality/Diff Comparison
**NONE.** The `isSame()` method (StoreFileMetadata.java:152-157) does NOT compare `writtenBy`:
```java
public boolean isSame(StoreFileMetadata other) {
    if (checksum == null || other.checksum == null) {
        return false;
    }
    return length == other.length && checksum.equals(other.checksum) && hash.equals(other.hash);
}
```
There is no `equals()` or `hashCode()` override in StoreFileMetadata.

### Category (c) — Version-Specific Branching
**RemoteSegmentStoreDirectory.java:400** — range validation on `writtenByMajor`:
```java
public void setWrittenByMajor(int writtenByMajor) {
    if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR) {
        this.writtenByMajor = writtenByMajor;
    } else {
        throw new IllegalArgumentException("Lucene major version supplied (" + writtenByMajor + ") is incorrect...");
    }
}
```
This is the ONLY version-branching site in production code.

---

## 4. DIFF PROTOCOL

### recoveryDiff (Store.java:1557-1594)
Uses `storeFileMetadata.isSame(meta)` which compares **length + checksum + hash only**.  
**writtenBy is NOT compared.**

### segmentReplicationDiff (Store.java:497-517)
Uses `fileMetadata.checksum().equals(value.checksum())` — **checksum only**.  
**writtenBy is NOT compared.**

**Conclusion:** Neither diff protocol uses `writtenBy` in its comparison logic.

---

## 5. CROSS-VERSION BRANCHING

Only ONE site in production code:

| File | Line | Code |
|------|------|------|
| RemoteSegmentStoreDirectory.java | 400 | `if (writtenByMajor <= Version.LATEST.major && writtenByMajor >= Version.MIN_SUPPORTED_MAJOR)` |

No `onOrBefore()`, `onOrAfter()`, or `major < N` checks exist on `StoreFileMetadata.writtenBy()` in production code.

The `loadMetadata` path uses `version.onOrAfter(maxVersion)` (Store.java:1306) but this is for computing `maxVersion` to assign to the segments file — it's not branching on an existing `writtenBy` value.

---

## 6. IMPACT IF writtenBy IS 'WRONG' (Parquet stamped with Lucene major=10)

### What happens with Version.LATEST (currently Lucene 10.x):

1. **Diff/Recovery:** NO IMPACT. Both `recoveryDiff` and `segmentReplicationDiff` ignore `writtenBy`. Files are matched by checksum+length only.

2. **Remote Store Upload (setWrittenByMajor):** The range check at RemoteSegmentStoreDirectory:400 validates `writtenByMajor <= Version.LATEST.major && >= Version.MIN_SUPPORTED_MAJOR`. Since Lucene 10 IS `Version.LATEST.major`, this passes. **If a future Lucene bump makes LATEST=11 but parquet stays at 10, it still passes** (within range). Only fails if the value falls outside [MIN_SUPPORTED_MAJOR, LATEST.major].

3. **Snapshot Serialization:** Written as a string to JSON (`"written_by": "10.0.0"`). On restore, parsed back via `Lucene.parseVersionLenient`. No branching on the value.

4. **Wire Protocol (FileChunkRequest):** Serialized/deserialized as string. No branching.

5. **Assertions:** `assert metadata.writtenBy() != null` — passes as long as non-null.

### Summary of Risk:
- **LOW RISK for diffs/replication** — writtenBy is invisible to comparison logic
- **MEDIUM RISK for remote store upload** — the `setWrittenByMajor` range check could reject a non-Lucene version if it falls outside [MIN_SUPPORTED_MAJOR, LATEST.major]. For parquet with its own versioning (e.g., major=2), this WOULD throw IllegalArgumentException.
- **NO RISK for snapshot/recovery** — purely serialization metadata

### Critical Finding for DFA Redesign:
If `DataformatAwareCatalogSnapshot.getFormatVersionForFile()` returns `Version.LATEST` (Lucene 10), the `setWrittenByMajor` validation passes. But if redesigned to return a parquet-native version (e.g., major=2), the validation at RemoteSegmentStoreDirectory:400 will **throw IllegalArgumentException** because 2 < MIN_SUPPORTED_MAJOR (currently 8 or 9). This is the single point where "Lucene-ness" of writtenBy leaks into downstream logic.

---

## Key Takeaway

`writtenBy` is primarily a **metadata tag for serialization** — it does NOT participate in any diff, equality, or replication decision. The only place it "leaks" Lucene semantics is the `setWrittenByMajor` range validation in `RemoteSegmentStoreDirectory`, which enforces Lucene version bounds. Any DFA redesign that assigns non-Lucene versions must either:
1. Keep returning a Lucene-compatible version (current approach with `Version.LATEST`), or
2. Relax/bypass the `setWrittenByMajor` validation for non-Lucene formats.
