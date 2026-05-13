# API Consumer Flow Map

## 1. CatalogSnapshot.getFormatVersionForFile(String file)

**Definition:** `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshot.java:276` (abstract)

**Implementations:**
- `SegmentInfosCatalogSnapshot.java:160` — looks up `segmentFileVersionMap`, falls back to commit version or `.si` version
- `DataformatAwareCatalogSnapshot.java:241` — returns `Version.LATEST` (TODO: per-file tracking)

**Call Sites:**

| # | File:Line | Downstream Effect |
|---|-----------|-------------------|
| 1 | `RemoteSegmentStoreDirectory.java:877` | `.major` extracted, passed to `metadata.setWrittenByMajor(...)`. Value serialized into remote metadata file as 5th field of `UploadedSegmentMetadata.toString()`. |
| 2 | `Store.java:1342` | Full `Version` object used to compute `maxVersion` (via `version.onOrAfter(maxVersion)`) and passed to `checksumFromFile(...)` which stores it in `StoreFileMetadata.writtenBy`. |

**Terminal Effects:**
- **Path A (RemoteSegmentStoreDirectory):** `Version.major` → `UploadedSegmentMetadata.writtenByMajor` → serialized as `SEPARATOR`-joined string in remote metadata file → deserialized back via `fromString()` on replica.
- **Path B (Store.MetadataSnapshot):** Full `Version` → `StoreFileMetadata.writtenBy` → serialized in `BlobStoreIndexShardSnapshot` (snapshot XContent) and `FileChunkRequest` (recovery wire protocol).

---

## 2. CatalogSnapshot.getMinSegmentFormatVersion()

**Definition:** `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshot.java:284` (abstract)

**Implementations:**
- `SegmentInfosCatalogSnapshot.java:177` — returns `segmentInfos.getMinSegmentLuceneVersion()`
- `DataformatAwareCatalogSnapshot.java:247` — returns `null`

**Call Sites:**

| # | File:Line | Downstream Effect |
|---|-----------|-------------------|
| 1 | `Store.java:1340` | Used as initial seed for `maxVersion`. If all per-file versions are older, this is the floor. If null (DFA), maxVersion starts null and is computed purely from per-file versions. Final maxVersion is used for the segments file's `StoreFileMetadata.writtenBy`. |

**Terminal Effect:** Seeds `maxVersion` which becomes `StoreFileMetadata.writtenBy` for the segments file → serialized in snapshots and recovery protocol.

---

## 3. CatalogSnapshot.getCommitDataFormatVersion()

**Definition:** `server/src/main/java/org/opensearch/index/engine/exec/coord/CatalogSnapshot.java:299` (abstract)

**Implementations:**
- `SegmentInfosCatalogSnapshot.java:182` — returns `segmentInfos.getCommitLuceneVersion()`
- `DataformatAwareCatalogSnapshot.java:252` — returns `Version.LATEST` (TODO)

**Call Sites:**

| # | File:Line | Downstream Effect |
|---|-----------|-------------------|
| 1 | `RemoteStoreReplicationSource.java:75` | Returned `Version` used as the `writtenBy` for ALL `StoreFileMetadata` entries built from remote metadata during replication. Applied uniformly to every file in the checkpoint response. |

**Terminal Effect:** `Version` → `StoreFileMetadata(name, length, checksum, version, null)` for every file in `CheckpointInfoResponse.metadataMap` → consumed by replica during segment replication to decide compatibility.

```java
// RemoteStoreReplicationSource.java:105-113
metadataMap = mdFile.getMetadata().entrySet().stream().collect(
    Collectors.toMap(
        e -> e.getKey(),
        e -> new StoreFileMetadata(
            e.getValue().getOriginalFilename(),
            e.getValue().getLength(),
            Store.digestToString(Long.valueOf(e.getValue().getChecksum())),
            version,  // <-- getCommitDataFormatVersion() result
            null)));
```

---

## 4. UploadedSegmentMetadata.setWrittenByMajor(int)

**Definition:** `RemoteSegmentStoreDirectory.java:399`

**Call Sites:**

| # | File:Line | Source of Value |
|---|-----------|----------------|
| 1 | `RemoteSegmentStoreDirectory.java:386` | Deserialized from remote metadata string (`values[4]`) |
| 2 | `RemoteSegmentStoreDirectory.java:807` | `segmentToLuceneVersion.get(metadata.originalFilename)` — deprecated path |
| 3 | `RemoteSegmentStoreDirectory.java:877` | `catalogSnapshot.getFormatVersionForFile(metadata.originalFilename).major` — new path |

**Terminal Effect:** Stored in `UploadedSegmentMetadata.writtenByMajor` field → serialized via `toString()` into remote metadata file → persisted to remote store → read back on any node via `fromString()`.

---

## 5. UploadedSegmentMetadata.getWrittenByMajor()

**Definition:** Not found — field `writtenByMajor` (line 350) is accessed directly within the class via `toString()` (line 367: `String.valueOf(writtenByMajor)`). No public getter method exists in the codebase.

**Effective read path:** `toString()` serializes it; `fromString()` deserializes it. The value is never read programmatically outside serialization.

---

## 6. Investigation: segmentToLuceneVersion Map (deprecated path, line ~807)

The map comes from `getSegmentToLuceneVersion(segmentFiles, segmentInfosSnapshot)` (line 920):

```java
// RemoteSegmentStoreDirectory.java:920-941
private Map<String, Integer> getSegmentToLuceneVersion(...) {
    Map<String, Integer> segmentToLuceneVersion = new HashMap<>();
    for (SegmentCommitInfo sci : segmentInfosSnapshot) {
        for (String file : sci.info.files()) {
            segmentToLuceneVersion.put(file, sci.info.getVersion().major);
        }
    }
    // Fallback: segments file → commitLuceneVersion.major
    // Other files → look up their segment's .si version
    return segmentToLuceneVersion;
}
```

This is the **same logic** now encapsulated in `SegmentInfosCatalogSnapshot.buildSegmentToLuceneVersionMap()` (line 214), except the old one stores `int` (major only) while the new one stores full `Version` objects.

---

## 7. Investigation: Is `.major` the only component used?

At `RemoteSegmentStoreDirectory.java:877`:
```java
metadata.setWrittenByMajor(catalogSnapshot.getFormatVersionForFile(metadata.originalFilename).major);
```

**Answer: Only `.major` matters here.** The `setWrittenByMajor(int)` API accepts only an int. The full Version is never stored in UploadedSegmentMetadata.

However, at `Store.java:1342`, the **full Version object** is used:
- `version.onOrAfter(maxVersion)` — compares major.minor.bugfix
- Passed to `StoreFileMetadata` constructor which stores the full `Version`

**Conclusion:** Two consumption patterns exist:
1. **Remote metadata path** — only `.major` matters (int serialization)
2. **Store.MetadataSnapshot path** — full `Version` (major.minor.bugfix) matters for ordering and is preserved in StoreFileMetadata

---

## Summary: Terminal Destinations

| API | Terminal Storage |
|-----|----------------|
| `getFormatVersionForFile` | (a) `UploadedSegmentMetadata.writtenByMajor` → remote metadata file (major only) <br> (b) `StoreFileMetadata.writtenBy` → snapshots + recovery wire (full Version) |
| `getMinSegmentFormatVersion` | Seeds `maxVersion` → `StoreFileMetadata.writtenBy` for segments file |
| `getCommitDataFormatVersion` | `StoreFileMetadata.writtenBy` for ALL files in replication checkpoint response |
| `setWrittenByMajor` | `UploadedSegmentMetadata.writtenByMajor` → serialized to remote metadata |
| `getWrittenByMajor` | No public getter; accessed only via `toString()` serialization |
