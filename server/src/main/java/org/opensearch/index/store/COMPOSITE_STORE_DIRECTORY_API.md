# CompositeStoreDirectory API Documentation

## Overview

The CompositeStoreDirectory provides format-aware file operations by routing files to appropriate format-specific directories based on FileMetadata. This enables OpenSearch to handle multiple data formats (Lucene, Parquet, Text, etc.) through a unified interface.

## Key Concepts

### FileMetadata-Based Routing
Instead of relying on file extensions, the API uses `FileMetadata(DataFormat df, String fileName)` to determine the correct format directory for each operation.

### Format-Agnostic Operations
All file operations are format-agnostic at the CompositeStoreDirectory level, with format-specific logic handled by individual FormatStoreDirectory implementations.

## Primary API Methods

### Format Routing

#### `getDirectoryForFormat(DataFormat format)`
Returns the FormatStoreDirectory that handles the specified format.

```java
FormatStoreDirectory luceneDir = compositeDirectory.getDirectoryForFormat(DataFormat.LUCENE);
FormatStoreDirectory parquetDir = compositeDirectory.getDirectoryForFormat(DataFormat.PARQUET);
```

**Throws:** `FormatNotSupportedException` if the format is not supported.

### FileMetadata-Based File Operations

#### `fileLength(FileMetadata fileMetadata)`
Returns the byte length of a file using format-aware routing.

```java
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, "segments_1");
long length = compositeDirectory.fileLength(metadata);
```

#### `deleteFile(FileMetadata fileMetadata)`
Deletes a file using format-aware routing.

```java
FileMetadata metadata = new FileMetadata(DataFormat.PARQUET, "data.parquet");
compositeDirectory.deleteFile(metadata);
```

#### `openInput(FileMetadata fileMetadata, IOContext context)`
Opens an IndexInput for reading using format-aware routing.

```java
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, "_0.cfe");
try (IndexInput input = compositeDirectory.openInput(metadata, IOContext.READ)) {
    // Read file content
}
```

#### `createOutput(FileMetadata fileMetadata, IOContext context)`
Creates an IndexOutput for writing using format-aware routing.

```java
FileMetadata metadata = new FileMetadata(DataFormat.TEXT, "output.txt");
try (IndexOutput output = compositeDirectory.createOutput(metadata, IOContext.DEFAULT)) {
    // Write file content
}
```

#### `copyFrom(FileMetadata fileMetadata, Directory source, IOContext context)`
Copies a file from another directory using format-aware routing.

```java
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, "segments_1");
compositeDirectory.copyFrom(metadata, sourceDirectory, IOContext.DEFAULT);
```

### Upload-Specific Operations

#### `createUploadInputStream(FileMetadata fileMetadata)`
Creates an input stream for uploading file content.

```java
FileMetadata metadata = new FileMetadata(DataFormat.PARQUET, "data.parquet");
try (InputStream stream = compositeDirectory.createUploadInputStream(metadata)) {
    // Upload stream content
}
```

#### `calculateUploadChecksum(FileMetadata fileMetadata)`
Calculates format-specific checksum for upload verification.

```java
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, "_0.cfs");
String checksum = compositeDirectory.calculateUploadChecksum(metadata);
```

## Legacy API Methods (Deprecated)

The following methods are deprecated and will be removed in future versions. Use FileMetadata-based alternatives instead.

### Deprecated Methods
- `fileLength(String name)` → Use `fileLength(FileMetadata)`
- `deleteFile(String name)` → Use `deleteFile(FileMetadata)`
- `openInput(String name, IOContext context)` → Use `openInput(FileMetadata, IOContext)`
- `createOutput(String name, IOContext context)` → Use `createOutput(FileMetadata, IOContext)`
- `getDirectoryForFile(String fileName)` → Use `getDirectoryForFormat(DataFormat)`

## Error Handling

### FormatNotSupportedException
Thrown when a requested format is not supported by any registered FormatStoreDirectory.

```java
try {
    FormatStoreDirectory dir = compositeDirectory.getDirectoryForFormat(unknownFormat);
} catch (FormatNotSupportedException e) {
    logger.error("Format not supported: {}, available: {}", 
                e.getRequestedFormat(), e.getAvailableFormats());
}
```

### Fallback Mechanisms
The API provides graceful fallback when FileMetadata is not available:

```java
// Automatic fallback to extension-based detection
FormatStoreDirectory dir = compositeDirectory.getDirectoryWithFallback(fileName, fileMetadata);
```

## Best Practices

### 1. Always Use FileMetadata When Available
```java
// Preferred
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, fileName);
long length = compositeDirectory.fileLength(metadata);

// Avoid (deprecated)
long length = compositeDirectory.fileLength(fileName);
```

### 2. Handle Format Exceptions Gracefully
```java
try {
    FormatStoreDirectory dir = compositeDirectory.getDirectoryForFormat(format);
    // Perform operations
} catch (FormatNotSupportedException e) {
    // Log error and handle gracefully
    logger.warn("Format {} not supported, using fallback", format.name());
    // Implement fallback logic
}
```

### 3. Use Appropriate IOContext
```java
// For reading existing files
IndexInput input = compositeDirectory.openInput(metadata, IOContext.READ);

// For creating new files
IndexOutput output = compositeDirectory.createOutput(metadata, IOContext.DEFAULT);

// For read-once operations
IndexInput input = compositeDirectory.openInput(metadata, IOContext.READONCE);
```

### 4. Proper Resource Management
```java
FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, fileName);
try (IndexInput input = compositeDirectory.openInput(metadata, IOContext.READ)) {
    // Use input stream
} // Automatically closed
```

## Migration Guide

### From String-Based to FileMetadata-Based APIs

#### Before (Deprecated)
```java
// Old approach - extension-based routing
long length = compositeDirectory.fileLength("segments_1");
compositeDirectory.deleteFile("data.parquet");
IndexInput input = compositeDirectory.openInput("_0.cfe", IOContext.READ);
```

#### After (Recommended)
```java
// New approach - metadata-based routing
FileMetadata segmentsMetadata = new FileMetadata(DataFormat.LUCENE, "segments_1");
long length = compositeDirectory.fileLength(segmentsMetadata);

FileMetadata parquetMetadata = new FileMetadata(DataFormat.PARQUET, "data.parquet");
compositeDirectory.deleteFile(parquetMetadata);

FileMetadata luceneMetadata = new FileMetadata(DataFormat.LUCENE, "_0.cfe");
IndexInput input = compositeDirectory.openInput(luceneMetadata, IOContext.READ);
```

### Custom FormatStoreDirectory Implementation

When implementing a custom FormatStoreDirectory:

```java
public class CustomFormatDirectory implements FormatStoreDirectory<CustomDataFormat> {
    
    @Override
    public CustomDataFormat getDataFormat() {
        return new CustomDataFormat(); // Return your format instance
    }
    
    @Override
    @Deprecated
    public boolean acceptsFile(String fileName) {
        // Deprecated - avoid file extension logic
        return true; // Or implement for backward compatibility
    }
    
    // Implement other required methods...
}
```

## Performance Considerations

### Format Directory Caching
Format directories are cached internally, so repeated calls to `getDirectoryForFormat()` are efficient.

### Batch Operations
For multiple files of the same format, cache the FormatStoreDirectory:

```java
FormatStoreDirectory luceneDir = compositeDirectory.getDirectoryForFormat(DataFormat.LUCENE);
for (String fileName : luceneFiles) {
    FileMetadata metadata = new FileMetadata(DataFormat.LUCENE, fileName);
    long length = compositeDirectory.fileLength(metadata); // Uses cached directory
}
```

## Monitoring and Debugging

### Format Statistics
Access format-specific statistics through RemoteSegmentTransferTracker:

```java
// Get upload statistics per format
Map<String, Long> formatCounts = tracker.getFormatUploadCounts();
Map<String, Long> formatBytes = tracker.getFormatUploadBytesMap();
String statistics = tracker.getFormatUploadStatistics();
```

### Logging
Enable debug logging for format routing decisions:

```yaml
logger.org.opensearch.index.store.CompositeStoreDirectory: DEBUG
```

This will log detailed information about format routing, fallback mechanisms, and error handling.