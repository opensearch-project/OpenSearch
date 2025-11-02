/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.store.distributed;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.store.Directory;
import org.apache.lucene.store.FilterDirectory;
import org.apache.lucene.store.IOContext;
import org.apache.lucene.store.IndexInput;
import org.apache.lucene.store.IndexOutput;
import org.opensearch.index.shard.IndexShard;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Collection;
import java.util.concurrent.atomic.AtomicReference;

/**
 * A wrapper directory that can be initialized without IndexShard and later
 * configured to use primary term routing when the IndexShard becomes available.
 * This allows the DirectoryFactory to create the directory before IndexShard
 * is created, and then the IndexShard can configure primary term routing later.
 *
 * @opensearch.internal
 */
public class PrimaryTermAwareDirectoryWrapper extends FilterDirectory {

    private static final Logger logger = LogManager.getLogger(PrimaryTermAwareDirectoryWrapper.class);
    
    private final Path basePath;
    private final AtomicReference<DistributedSegmentDirectory> distributedDirectory;
    private volatile boolean primaryTermRoutingEnabled = false;

    /**
     * Creates a new PrimaryTermAwareDirectoryWrapper.
     *
     * @param delegate the base Directory instance
     * @param basePath the base filesystem path for creating subdirectories
     */
    public PrimaryTermAwareDirectoryWrapper(Directory delegate, Path basePath) {
        super(delegate);
        this.basePath = basePath;
        this.distributedDirectory = new AtomicReference<>();
        
        logger.debug("Created PrimaryTermAwareDirectoryWrapper at path: {}", basePath);
    }

    /**
     * Enables primary term routing by setting the IndexShard reference.
     * This method should be called after the IndexShard is created.
     *
     * @param indexShard the IndexShard instance for primary term access
     * @throws IOException if primary term routing setup fails
     */
    public void enablePrimaryTermRouting(IndexShard indexShard) throws IOException {
        if (primaryTermRoutingEnabled) {
            logger.debug("Primary term routing already enabled");
            return;
        }
        
        try {
            DistributedSegmentDirectory newDistributedDirectory = new DistributedSegmentDirectory(in, basePath, indexShard);
            distributedDirectory.set(newDistributedDirectory);
            primaryTermRoutingEnabled = true;
            
            logger.info("Enabled primary term routing for directory at path: {}", basePath);
        } catch (IOException e) {
            logger.error("Failed to enable primary term routing", e);
            throw e;
        }
    }

    /**
     * Gets the underlying distributed directory if primary term routing is enabled.
     *
     * @return the DistributedSegmentDirectory, or null if not enabled
     */
    private DistributedSegmentDirectory getDistributedDirectory() {
        return distributedDirectory.get();
    }

    /**
     * Checks if primary term routing is enabled.
     *
     * @return true if primary term routing is enabled, false otherwise
     */
    public boolean isPrimaryTermRoutingEnabled() {
        return primaryTermRoutingEnabled;
    }

    @Override
    public String[] listAll() throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            return distributed.listAll();
        }
        return super.listAll();
    }

    @Override
    public IndexInput openInput(String name, IOContext context) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            return distributed.openInput(name, context);
        }
        return super.openInput(name, context);
    }

    @Override
    public IndexOutput createOutput(String name, IOContext context) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            return distributed.createOutput(name, context);
        }
        return super.createOutput(name, context);
    }

    @Override
    public void deleteFile(String name) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            distributed.deleteFile(name);
        } else {
            super.deleteFile(name);
        }
    }

    @Override
    public long fileLength(String name) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            return distributed.fileLength(name);
        }
        return super.fileLength(name);
    }

    @Override
    public void sync(Collection<String> names) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            distributed.sync(names);
        } else {
            super.sync(names);
        }
    }

    @Override
    public void rename(String source, String dest) throws IOException {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            distributed.rename(source, dest);
        } else {
            super.rename(source, dest);
        }
    }

    @Override
    public void close() throws IOException {
        IOException exception = null;
        
        // Close the distributed directory if it exists
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            try {
                distributed.close();
            } catch (IOException e) {
                exception = e;
            }
        }
        
        // Close the base directory
        try {
            super.close();
        } catch (IOException e) {
            if (exception == null) {
                exception = e;
            } else {
                exception.addSuppressed(e);
            }
        }
        
        if (exception != null) {
            throw exception;
        }
        
        logger.debug("Closed PrimaryTermAwareDirectoryWrapper");
    }

    /**
     * Gets routing information for debugging purposes.
     *
     * @param filename the filename to analyze
     * @return routing information string
     */
    public String getRoutingInfo(String filename) {
        DistributedSegmentDirectory distributed = getDistributedDirectory();
        if (distributed != null) {
            return distributed.getRoutingInfo(filename);
        }
        return "Primary term routing not enabled, using base directory";
    }

    /**
     * Gets the base path.
     *
     * @return the base filesystem path
     */
    public Path getBasePath() {
        return basePath;
    }
}