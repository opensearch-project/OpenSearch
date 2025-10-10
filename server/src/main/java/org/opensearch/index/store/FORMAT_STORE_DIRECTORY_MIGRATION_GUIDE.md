# FormatStoreDirectory Migration Guide

## Overview

This guide helps developers migrate custom FormatStoreDirectory implementations to support the new FileMetadata-based format routing system. The migration ensures compatibility with the enhanced CompositeStoreDirectory while maintaining backward compatibility.

## Key Changes

### 1. Enhanced Interface Methods

The FormatStoreDirectory interface now includes:
- `supportsFormat(DataFormat format)` - New method for format matching
- `acceptsFile(String fileName)` - Now deprecated, but maintained for backward compatibility

### 2. Format Declaration Requirement

All FormatStoreDirectory implementations must properly declare their supported DataFormat through the `getDataFormat()` method.

## Migration Steps

### Step 1: Update Format Declaration

Ensure your FormatStoreDirectory properly implements `getDataFormat()`:

#### Before
```java
public class MyCustomDirectory implements FormatStoreDirectory<MyDataFormat> {
    
    @Override
    public MyDataFormat getDataFormat() {
        // May have been incomplete or incorrect
        return null; // or incorrect implementation
    }
}
```

#### After
```java
public class MyCustomDirectory implements FormatStoreDirectory<MyDataFormat> {
    
    private final MyDataFormat dataFormat;
    
    public MyCustomDirectory(MyDataFormat dataFormat, /* other params */) {
        this.dataFormat = dataFormat;
        // Initialize other fields
    }
    
    @Override
    public MyDataFormat getDataFormat() {
        return dataFormat; // Always return the correct format instance
    }
}
```

### Step 2: Implement supportsFormat() Method

The new `supportsFormat()` method has a default implementation, but you can override it if needed:

#### Default Implementation (Usually Sufficient)
```java
// No action needed - default implementation works for most cases
// default boolean supportsFormat(DataFormat format) {
//     return getDataFormat().equals(format);
// }
```

#### Custom Implementation (If Needed)
```java
@Override
public boolean supportsFormat(DataFormat format) {
    // Custom logic if your directory supports multiple related formats
    return getDataFormat().equals(format) || 
           isCompatibleFormat(format);
}

private boolean isCompatibleFormat(DataFormat format) {
    // Your custom compatibility logic
    return false;
}
```

### Step 3: Update acceptsFile() Method

Mark the `acceptsFile()` method as deprecated and remove file extension logic:

#### Before
```java
@Override
public boolean acceptsFile(String fileName) {
    // File extension-based logic
    return fileName.endsWith(".myext") || 
           fileName.endsWith(".custom") ||
           fileName.startsWith("myprefix_");
}
```

#### After
```java
@Override
@Deprecated
public boolean acceptsFile(String fileName) {
    // Deprecated - format determination should be done through FileMetadata
    // Keep simple logic for backward compatibility, or return true
    return true; // Let format routing handle this
}
```

### Step 4: Update DataFormat Implementation

Ensure your DataFormat implementation is complete:

#### Before
```java
public class MyDataFormat implements DataFormat {
    @Override
    public String name() {
        return "mycustom";
    }
    
    // Other methods may be incomplete
}
```

#### After
```java
public class MyDataFormat implements DataFormat {
    
    @Override
    public Setting<Settings> dataFormatSettings() {
        return null; // Or return your custom settings
    }

    @Override
    public Setting<Settings> clusterLeveldataFormatSettings() {
        return null; // Or return your cluster-level settings
    }

    @Override
    public String name() {
        return "mycustom"; // Consistent format name
    }

    @Override
    public void configureStore() {
        // Any store configuration logic
    }

    @Override
    public String getDirectoryName() {
        return "mycustom"; // Directory name under shard path
    }
    
    @Override
    public boolean equals(Object obj) {
        return obj instanceof MyDataFormat;
    }
    
    @Override
    public int hashCode() {
        return name().hashCode();
    }
}
```

### Step 5: Update Plugin Registration

Ensure your DataSourcePlugin properly registers the format:

#### Before
```java
public class MyDataFormatPlugin extends Plugin implements DataSourcePlugin {
    
    @Override
    public DataFormat getDataFormat() {
        return new MyDataFormat();
    }
    
    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        // May not properly pass DataFormat instance
        return new MyCustomDirectory(/* params */);
    }
}
```

#### After
```java
public class MyDataFormatPlugin extends Plugin implements DataSourcePlugin {
    
    private static final MyDataFormat DATA_FORMAT = new MyDataFormat();
    
    @Override
    public DataFormat getDataFormat() {
        return DATA_FORMAT;
    }
    
    @Override
    public FormatStoreDirectory<?> createFormatStoreDirectory(
        IndexSettings indexSettings,
        ShardPath shardPath
    ) throws IOException {
        Logger logger = LogManager.getLogger("index.store.mycustom." + shardPath.getShardId());
        
        // Pass the DataFormat instance to the directory
        return new MyCustomDirectory(
            DATA_FORMAT,
            shardPath,
            logger
        );
    }
}
```

## Testing Your Migration

### 1. Format Registration Test
```java
@Test
public void testFormatRegistration() {
    MyDataFormat format = new MyDataFormat();
    MyCustomDirectory directory = new MyCustomDirectory(format, /* params */);
    
    // Verify format declaration
    assertEquals(format, directory.getDataFormat());
    assertTrue(directory.supportsFormat(format));
}
```

### 2. CompositeStoreDirectory Integration Test
```java
@Test
public void testCompositeIntegration() {
    // Create composite directory with your format
    List<DataFormat> formats = List.of(DataFormat.LUCENE, new MyDataFormat());
    Any dataFormats = new Any(formats);
    
    CompositeStoreDirectory composite = new CompositeStoreDirectory(
        indexSettings, pluginsService, dataFormats, shardPath, logger
    );
    
    // Test format routing
    MyDataFormat myFormat = new MyDataFormat();
    FormatStoreDirectory directory = composite.getDirectoryForFormat(myFormat);
    assertNotNull(directory);
    assertTrue(directory instanceof MyCustomDirectory);
}
```

### 3. FileMetadata-Based Operations Test
```java
@Test
public void testFileMetadataOperations() throws IOException {
    MyDataFormat format = new MyDataFormat();
    FileMetadata metadata = new FileMetadata(format, "test.myext");
    
    // Test FileMetadata-based operations
    long length = compositeDirectory.fileLength(metadata);
    boolean exists = compositeDirectory.fileExists(metadata);
    
    // Verify operations are routed to your directory
    assertTrue(length >= 0);
}
```

## Common Migration Issues

### Issue 1: Format Equality Problems
**Problem:** Format routing fails because DataFormat.equals() is not properly implemented.

**Solution:**
```java
@Override
public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    MyDataFormat that = (MyDataFormat) obj;
    return Objects.equals(name(), that.name());
}

@Override
public int hashCode() {
    return Objects.hash(name());
}
```

### Issue 2: Plugin Discovery Failures
**Problem:** CompositeStoreDirectory cannot find your format plugin.

**Solution:** Ensure your plugin is properly registered in `plugin-descriptor.properties`:
```properties
name=my-data-format-plugin
classname=com.example.MyDataFormatPlugin
description=My custom data format plugin
version=1.0.0
opensearch.version=2.0.0
java.version=11
```

### Issue 3: Directory Creation Failures
**Problem:** FormatStoreDirectory creation fails during CompositeStoreDirectory initialization.

**Solution:** Add proper error handling and logging:
```java
@Override
public FormatStoreDirectory<?> createFormatStoreDirectory(
    IndexSettings indexSettings,
    ShardPath shardPath
) throws IOException {
    try {
        return new MyCustomDirectory(DATA_FORMAT, shardPath, logger);
    } catch (Exception e) {
        logger.error("Failed to create MyCustomDirectory: {}", e.getMessage(), e);
        throw new IOException("Failed to create format store directory", e);
    }
}
```

## Backward Compatibility

### Maintaining Legacy Support
If you need to support both old and new APIs:

```java
public class MyCustomDirectory implements FormatStoreDirectory<MyDataFormat> {
    
    @Override
    public MyDataFormat getDataFormat() {
        return dataFormat;
    }
    
    @Override
    @Deprecated
    public boolean acceptsFile(String fileName) {
        // Maintain backward compatibility for legacy callers
        return fileName.endsWith(".myext");
    }
    
    // Support both old and new file operations
    public long fileLength(String name) throws IOException {
        // Legacy implementation
        return Files.size(directoryPath.resolve(name));
    }
    
    // New FileMetadata-based operations will be routed through
    // CompositeStoreDirectory to the legacy methods
}
```

## Performance Considerations

### 1. Format Instance Reuse
Reuse DataFormat instances to avoid unnecessary object creation:

```java
public class MyDataFormatPlugin extends Plugin implements DataSourcePlugin {
    private static final MyDataFormat INSTANCE = new MyDataFormat();
    
    @Override
    public DataFormat getDataFormat() {
        return INSTANCE; // Reuse the same instance
    }
}
```

### 2. Directory Caching
CompositeStoreDirectory caches format directories, so avoid expensive initialization in constructors:

```java
public class MyCustomDirectory implements FormatStoreDirectory<MyDataFormat> {
    
    public MyCustomDirectory(MyDataFormat format, ShardPath shardPath, Logger logger) {
        // Keep constructor lightweight
        this.format = format;
        this.shardPath = shardPath;
        this.logger = logger;
        // Defer expensive initialization to initialize() method
    }
    
    @Override
    public void initialize() throws IOException {
        // Perform expensive initialization here
        createDirectoryStructure();
        initializeResources();
    }
}
```

## Getting Help

If you encounter issues during migration:

1. **Enable Debug Logging:**
   ```yaml
   logger.org.opensearch.index.store.CompositeStoreDirectory: DEBUG
   logger.org.opensearch.index.store.FormatStoreDirectoryFactory: DEBUG
   ```

2. **Check Plugin Loading:**
   Look for plugin registration messages in OpenSearch logs.

3. **Verify Format Registration:**
   Use the CompositeStoreDirectory.getAllDirectories() method to verify your format is registered.

4. **Test Format Routing:**
   Create unit tests to verify format routing works correctly with your implementation.