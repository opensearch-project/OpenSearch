/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FSDirectory;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.file.Path;

public class DistributedDirectoryFactoryTests extends OpenSearchTestCase {

    private Path tempDir;
    private Directory baseDirectory;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir();
        baseDirectory = FSDirectory.open(tempDir);
    }

    @Override
    public void tearDown() throws Exception {
        if (baseDirectory != null) {
            baseDirectory.close();
        }
        super.tearDown();
    }

    public void testFactoryWithDefaultSettings() throws IOException {
        Settings settings = Settings.EMPTY;
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        
        assertFalse("Distributed should be disabled by default", factory.isDistributedEnabled());
        assertEquals("Should use default subdirectories", 5, factory.getNumSubdirectories());
        assertEquals("Should use default hash algorithm", "default", factory.getHashAlgorithm());
    }

    public void testFactoryWithDistributedDisabled() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, false)
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        ShardId shardId = new ShardId("test", "test-uuid", 0);
        
        Directory result = factory.createDirectory(baseDirectory, tempDir, shardId);
        
        assertSame("Should return delegate directory when disabled", baseDirectory, result);
        assertFalse("Should report as disabled", factory.isDistributedEnabled());
    }

    public void testFactoryWithDistributedEnabled() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        ShardId shardId = new ShardId("test", "test-uuid", 0);
        
        Directory result = factory.createDirectory(baseDirectory, tempDir, shardId);
        
        assertNotSame("Should return distributed directory when enabled", baseDirectory, result);
        assertTrue("Should be instance of DistributedSegmentDirectory", 
                  result instanceof DistributedSegmentDirectory);
        assertTrue("Should report as enabled", factory.isDistributedEnabled());
        
        result.close();
    }

    public void testFactoryWithCustomSettings() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .put(DistributedDirectoryFactory.DISTRIBUTED_SUBDIRECTORIES_SETTING, 3)
            .put(DistributedDirectoryFactory.DISTRIBUTED_HASH_ALGORITHM_SETTING, "custom")
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        
        assertTrue("Should be enabled", factory.isDistributedEnabled());
        assertEquals("Should use custom subdirectories", 3, factory.getNumSubdirectories());
        assertEquals("Should use custom hash algorithm", "custom", factory.getHashAlgorithm());
    }

    public void testFactoryWithoutShardId() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        
        Directory result = factory.createDirectory(baseDirectory, tempDir);
        
        assertNotSame("Should return distributed directory", baseDirectory, result);
        assertTrue("Should be instance of DistributedSegmentDirectory", 
                  result instanceof DistributedSegmentDirectory);
        
        result.close();
    }

    public void testFactoryWithInvalidHashAlgorithm() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .put(DistributedDirectoryFactory.DISTRIBUTED_HASH_ALGORITHM_SETTING, "invalid")
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        ShardId shardId = new ShardId("test", "test-uuid", 0);
        
        // Should still create directory with default hasher
        Directory result = factory.createDirectory(baseDirectory, tempDir, shardId);
        
        assertNotSame("Should return distributed directory", baseDirectory, result);
        assertTrue("Should be instance of DistributedSegmentDirectory", 
                  result instanceof DistributedSegmentDirectory);
        
        result.close();
    }

    public void testFactoryWithSettings() throws IOException {
        Settings originalSettings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, false)
            .build();
        
        Settings newSettings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .build();
        
        DistributedDirectoryFactory originalFactory = new DistributedDirectoryFactory(originalSettings);
        DistributedDirectoryFactory newFactory = originalFactory.withSettings(newSettings);
        
        assertFalse("Original factory should be disabled", originalFactory.isDistributedEnabled());
        assertTrue("New factory should be enabled", newFactory.isDistributedEnabled());
    }

    public void testFactoryFallbackOnError() throws IOException {
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        ShardId shardId = new ShardId("test", "test-uuid", 0);
        
        // Use a path that will cause directory creation to fail
        Path invalidPath = tempDir.resolve("nonexistent/invalid/path");
        
        Directory result = factory.createDirectory(baseDirectory, invalidPath, shardId);
        
        // Should fall back to delegate directory on error
        assertSame("Should fall back to delegate on error", baseDirectory, result);
    }

    public void testFactorySettingsValidation() throws IOException {
        // Test with various settings combinations
        Settings settings1 = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, "true")
            .build();
        
        Settings settings2 = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, "false")
            .build();
        
        DistributedDirectoryFactory factory1 = new DistributedDirectoryFactory(settings1);
        DistributedDirectoryFactory factory2 = new DistributedDirectoryFactory(settings2);
        
        assertTrue("String 'true' should be parsed as enabled", factory1.isDistributedEnabled());
        assertFalse("String 'false' should be parsed as disabled", factory2.isDistributedEnabled());
    }
} 
   public void testSettingsIntegration() throws IOException {
        // Test all configuration options
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .put(DistributedDirectoryFactory.DISTRIBUTED_SUBDIRECTORIES_SETTING, 3)
            .put(DistributedDirectoryFactory.DISTRIBUTED_HASH_ALGORITHM_SETTING, "default")
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        
        assertTrue("Should be enabled", factory.isDistributedEnabled());
        assertEquals("Should use configured subdirectories", 3, factory.getNumSubdirectories());
        assertEquals("Should use configured hash algorithm", "default", factory.getHashAlgorithm());
    }

    public void testSettingsValidation() throws IOException {
        // Test with invalid subdirectory count (should still work, just use the value)
        Settings settings = Settings.builder()
            .put(DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING, true)
            .put(DistributedDirectoryFactory.DISTRIBUTED_SUBDIRECTORIES_SETTING, 10)
            .build();
        
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(settings);
        assertEquals("Should accept configured value", 10, factory.getNumSubdirectories());
    }

    public void testDefaultSettingsValues() throws IOException {
        DistributedDirectoryFactory factory = new DistributedDirectoryFactory(Settings.EMPTY);
        
        assertEquals("Default enabled should be false", 
                    DistributedDirectoryFactory.DEFAULT_DISTRIBUTED_ENABLED, factory.isDistributedEnabled());
        assertEquals("Default subdirectories should be 5", 
                    DistributedDirectoryFactory.DEFAULT_SUBDIRECTORIES, factory.getNumSubdirectories());
        assertEquals("Default hash algorithm should be 'default'", 
                    DistributedDirectoryFactory.DEFAULT_HASH_ALGORITHM, factory.getHashAlgorithm());
    }

    public void testSettingsConstants() {
        // Verify setting key constants are correct
        assertEquals("Enabled setting key", "index.store.distributed.enabled", 
                    DistributedDirectoryFactory.DISTRIBUTED_ENABLED_SETTING);
        assertEquals("Subdirectories setting key", "index.store.distributed.subdirectories", 
                    DistributedDirectoryFactory.DISTRIBUTED_SUBDIRECTORIES_SETTING);
        assertEquals("Hash algorithm setting key", "index.store.distributed.hash_algorithm", 
                    DistributedDirectoryFactory.DISTRIBUTED_HASH_ALGORITHM_SETTING);
    }