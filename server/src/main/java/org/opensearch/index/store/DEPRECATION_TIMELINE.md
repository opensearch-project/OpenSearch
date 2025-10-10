# Deprecation Timeline for CompositeStoreDirectory APIs

## Overview

This document outlines the deprecation timeline for legacy string-based APIs in favor of FileMetadata-based APIs. The migration provides better format awareness and eliminates reliance on file extension detection.

## Deprecation Schedule

### Phase 1: Current Release (Deprecation Warnings)
**Status:** ✅ Implemented  
**Timeline:** Current release

#### Deprecated APIs
The following methods are marked as `@Deprecated` with warnings:

##### CompositeStoreDirectory
- `fileLength(String name)` → Use `fileLength(FileMetadata)`
- `deleteFile(String name)` → Use `deleteFile(FileMetadata)`
- `openInput(String name, IOContext context)` → Use `openInput(FileMetadata, IOContext)`
- `createOutput(String name, IOContext context)` → Use `createOutput(FileMetadata, IOContext)`
- `getDirectoryForFile(String fileName)` → Use `getDirectoryForFormat(DataFormat)`
- `createOutput(String name)` → Use FileMetadata-based methods
- `openInput(String name)` → Use FileMetadata-based methods

##### RemoteSegmentStoreDirectory
- `copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload)` → Use `copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener<Void>, boolean)`
- `copyFrom(CompositeStoreDirectory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload)` → Use `copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener<Void>, boolean)`
- `copyFrom(Directory from, String src, String dest, IOContext context)` → Use `copyFrom(FileMetadata, CompositeStoreDirectory, IOContext, ActionListener<Void>, boolean)`

##### RemoteStoreUploaderService
- `uploadSegmentsLegacy(Collection<String> localSegments, ...)` → Use `uploadSegments(Collection<FileMetadata>, ...)`

##### FormatStoreDirectory Interface
- `acceptsFile(String fileName)` → Use `supportsFormat(DataFormat)` and FileMetadata-based routing

#### Actions Required
- **Developers:** Update code to use FileMetadata-based APIs
- **Plugin Authors:** Implement `getDataFormat()` properly and avoid file extension logic in `acceptsFile()`
- **Users:** No immediate action required (backward compatibility maintained)

### Phase 2: Next Major Release (Removal Warnings)
**Timeline:** Next major release (e.g., 3.1.0)

#### Enhanced Deprecation Warnings
- Add more prominent deprecation warnings in logs
- Include migration suggestions in error messages
- Update documentation to emphasize new APIs

#### New Features
- Enhanced FileMetadata-based APIs
- Improved format-specific error handling
- Better performance optimizations for format routing

#### Actions Required
- **Developers:** Complete migration to FileMetadata-based APIs
- **Plugin Authors:** Remove file extension logic from `acceptsFile()` implementations
- **Users:** Plan for eventual removal of deprecated APIs

### Phase 3: Major Release + 1 (Final Warnings)
**Timeline:** Major release + 1 (e.g., 3.2.0)

#### Final Deprecation Phase
- Add "will be removed in next version" warnings
- Provide automated migration tools where possible
- Comprehensive documentation updates

#### Breaking Change Preparation
- Announce exact removal timeline
- Provide migration scripts for common use cases
- Update all examples and documentation

#### Actions Required
- **Developers:** Must complete migration (deprecated APIs will be removed soon)
- **Plugin Authors:** Must update to new interface requirements
- **Users:** Test applications with new APIs

### Phase 4: Major Release + 2 (Removal)
**Timeline:** Major release + 2 (e.g., 4.0.0)

#### API Removal
Complete removal of deprecated APIs:

##### Removed Methods
```java
// These methods will be completely removed:

// CompositeStoreDirectory
public long fileLength(String name) // REMOVED
public void deleteFile(String name) // REMOVED
public IndexInput openInput(String name, IOContext context) // REMOVED
public IndexOutput createOutput(String name, IOContext context) // REMOVED
public FormatStoreDirectory getDirectoryForFile(String fileName) // REMOVED

// RemoteSegmentStoreDirectory  
public void copyFrom(Directory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) // REMOVED
public void copyFrom(CompositeStoreDirectory from, String src, IOContext context, ActionListener<Void> listener, boolean lowPriorityUpload) // REMOVED

// FormatStoreDirectory
boolean acceptsFile(String fileName) // REMOVED (interface method)
```

#### Breaking Changes
- Code using deprecated APIs will fail to compile
- Plugins using old interface methods must be updated
- File extension-based routing completely removed

#### Actions Required
- **Developers:** Must use FileMetadata-based APIs exclusively
- **Plugin Authors:** Must implement new interface requirements
- **Users:** Must update to compatible plugin versions

## Migration Examples

### Before (Deprecated)
```java
// String-based operations (deprecated)
long length = compositeDirectory.fileLength("segments_1");
compositeDirectory.deleteFile("data.parquet");
IndexInput input = compositeDirectory.openInput("_0.cfe", IOContext.READ);

// Extension-based routing (deprecated)
FormatStoreDirectory dir = compositeDirectory.getDirectoryForFile("data.parquet");

// String-based upload (deprecated)
remoteDirectory.copyFrom(storeDirectory, "segments_1", IOContext.DEFAULT, listener, false);
```

### After (Current)
```java
// FileMetadata-based operations (current)
FileMetadata segmentsMetadata = new FileMetadata(DataFormat.LUCENE, "segments_1");
long length = compositeDirectory.fileLength(segmentsMetadata);

FileMetadata parquetMetadata = new FileMetadata(DataFormat.PARQUET, "data.parquet");
compositeDirectory.deleteFile(parquetMetadata);

FileMetadata luceneMetadata = new FileMetadata(DataFormat.LUCENE, "_0.cfe");
IndexInput input = compositeDirectory.openInput(luceneMetadata, IOContext.READ);

// Format-based routing (current)
FormatStoreDirectory dir = compositeDirectory.getDirectoryForFormat(DataFormat.PARQUET);

// FileMetadata-based upload (current)
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, "segments_1");
remoteDirectory.copyFrom(metadata, storeDirectory, IOContext.DEFAULT, listener, false);
```

## Compatibility Matrix

| OpenSearch Version | Deprecated APIs | FileMetadata APIs | Status |
|-------------------|----------------|-------------------|---------|
| 3.0.0 | ✅ Available (with warnings) | ✅ Available | Current |
| 3.1.0 | ⚠️ Available (enhanced warnings) | ✅ Available | Next Release |
| 3.2.0 | ⚠️ Available (final warnings) | ✅ Available | Major + 1 |
| 4.0.0 | ❌ Removed | ✅ Available | Major + 2 |

## Plugin Compatibility

### FormatStoreDirectory Interface Changes

#### Current (3.0.0)
```java
public interface FormatStoreDirectory<T extends DataFormat> {
    T getDataFormat(); // Required
    
    default boolean supportsFormat(DataFormat format) { // New, optional
        return getDataFormat().equals(format);
    }
    
    @Deprecated
    boolean acceptsFile(String fileName); // Deprecated but required
    
    // Other methods...
}
```

#### Future (4.0.0)
```java
public interface FormatStoreDirectory<T extends DataFormat> {
    T getDataFormat(); // Required
    
    default boolean supportsFormat(DataFormat format) { // Required
        return getDataFormat().equals(format);
    }
    
    // acceptsFile() method completely removed
    
    // Other methods...
}
```

## Migration Tools

### Automated Detection
Use the following script to find deprecated API usage:

```bash
# Find deprecated method calls
grep -r "\.fileLength(" --include="*.java" src/
grep -r "\.deleteFile(" --include="*.java" src/
grep -r "\.openInput(" --include="*.java" src/
grep -r "\.getDirectoryForFile(" --include="*.java" src/
grep -r "\.copyFrom.*String" --include="*.java" src/
```

### IDE Support
Most IDEs will highlight deprecated method usage. Configure your IDE to show deprecation warnings prominently.

### Compilation Warnings
Enable deprecation warnings in your build:

```xml
<!-- Maven -->
<plugin>
    <groupId>org.apache.maven.plugins</groupId>
    <artifactId>maven-compiler-plugin</artifactId>
    <configuration>
        <compilerArgs>
            <arg>-Xlint:deprecation</arg>
        </compilerArgs>
    </configuration>
</plugin>
```

```gradle
// Gradle
compileJava {
    options.compilerArgs << "-Xlint:deprecation"
}
```

## Support and Resources

### Documentation
- [CompositeStoreDirectory API Documentation](COMPOSITE_STORE_DIRECTORY_API.md)
- [FormatStoreDirectory Migration Guide](FORMAT_STORE_DIRECTORY_MIGRATION_GUIDE.md)

### Community Support
- OpenSearch GitHub Issues: Report migration problems
- OpenSearch Forums: Ask migration questions
- OpenSearch Slack: Real-time migration help

### Professional Support
Contact OpenSearch support for enterprise migration assistance.

## FAQ

### Q: Can I continue using deprecated APIs?
**A:** Yes, until version 4.0.0. However, we strongly recommend migrating to FileMetadata-based APIs as soon as possible.

### Q: Will my existing plugins break?
**A:** Not immediately. Deprecated APIs will continue to work until version 4.0.0. However, you should update your plugins to use the new APIs.

### Q: How do I know if my code uses deprecated APIs?
**A:** Enable deprecation warnings in your compiler and IDE. Also check the OpenSearch logs for deprecation warnings at runtime.

### Q: Is there a performance difference between old and new APIs?
**A:** The new FileMetadata-based APIs are more efficient as they avoid file extension parsing and provide direct format routing.

### Q: What if I can't migrate before the removal deadline?
**A:** You'll need to stay on an older OpenSearch version until you can complete the migration. We recommend planning the migration well in advance.

### Q: Are there any breaking changes in the new APIs?
**A:** The new APIs are additive and don't break existing functionality. However, they do require FileMetadata objects instead of plain strings.

### Q: How do I create FileMetadata objects?
**A:** Use the constructor: `new FileMetadata(DataFormat.LUCENE, "filename")`. The DataFormat should match the actual format of your file.

### Q: What happens if I use the wrong DataFormat in FileMetadata?
**A:** The system will route the file to the wrong format directory, which may cause errors. Always ensure the DataFormat matches the actual file format.