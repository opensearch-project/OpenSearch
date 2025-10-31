/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.datafusion;

import org.opensearch.index.engine.exec.FileMetadata;

import java.util.Collection;

/**
 * JNI wrapper for DataFusion operations
 */
public class DataFusionQueryJNI {

    private static boolean libraryLoaded = false;

    static {
        loadNativeLibrary();
    }

    /**
     * Private constructor to prevent instantiation of utility class.
     */
    private DataFusionQueryJNI() {
        // Utility class
    }

    /**
     * Load the native library from resources
     */
    private static synchronized void loadNativeLibrary() {
        if (libraryLoaded) {
            return;
        }

        try {
            // Try to load the library directly
            System.loadLibrary("opensearch_datafusion_jni");
            libraryLoaded = true;
        } catch (UnsatisfiedLinkError e) {
            // Try loading from resources
            try {
                String osName = System.getProperty("os.name").toLowerCase();
                String libExtension = osName.contains("windows") ? ".dll" : (osName.contains("mac") ? ".dylib" : ".so");
                String libName = "libopensearch_datafusion_jni" + libExtension;

                java.io.InputStream is = DataFusionQueryJNI.class.getResourceAsStream("/native/" + libName);
                if (is != null) {
                    java.io.File tempFile = java.io.File.createTempFile("libopensearch_datafusion_jni", libExtension);
                    tempFile.deleteOnExit();

                    try (java.io.FileOutputStream fos = new java.io.FileOutputStream(tempFile)) {
                        byte[] buffer = new byte[8192];
                        int bytesRead;
                        while ((bytesRead = is.read(buffer)) != -1) {
                            fos.write(buffer, 0, bytesRead);
                        }
                    }

                    System.load(tempFile.getAbsolutePath());
                    libraryLoaded = true;
                } else {
                    throw new RuntimeException("Native library not found: " + libName, e);
                }
            } catch (Exception ex) {
                throw new RuntimeException("Failed to load native library", ex);
            }
        }
    }

    /**
     * Create a new global runtime environment
     * @return runtime env pointer for subsequent operations
     */
    public static native long createGlobalRuntime();

    public static native long createGlobalRuntimev1(long cacheManagerPtr);

    public static native long createTokioRuntime();

    /**
     * Closes global runtime environment
     * @param pointer the runtime environment pointer to close
     * @return status code
     */
    public static native long closeGlobalRuntime(long pointer);

    /**
     * Get version information
     * @return JSON string with version information
     */
    public static native String getVersionInfo();

    /**
     * Create a new DataFusion session context
     * @param runtimeId the global runtime environment ID
     * @return context ID for subsequent operations
     */
    public static native long createSessionContext(long runtimeId);

    /**
     * Close and cleanup a DataFusion context
     * @param contextId the context ID to close
     */
    public static native void closeSessionContext(long contextId);

    /**
     * Execute a Substrait query plan
     * @param cachePtr the session context ID
     * @param substraitPlan the serialized Substrait query plan
     * @return stream pointer for result iteration
     */
  //  public static native long executeSubstraitQuery(long cachePtr, byte[] substraitPlan, long runtimePtr);

    public static native long executeSubstraitQuery(long cachePtr, byte[] substraitPlan, long tokioRuntimePtr, long runtimePtr);

    public static native long createDatafusionReader(String path, String[] files);

    public static native void closeDatafusionReader(long ptr);

    /**
     * Register a directory with CSV files
     * @param contextId the session context ID
     * @param tableName the table name to register
     * @param directoryPath the directory path containing CSV files
     * @param fileNames array of file names to register
     * @return status code
     */
    public static native int registerCsvDirectory(long contextId, String tableName, String directoryPath, String[] fileNames);

    /**
     * Check if stream has more data
     * @param streamPtr the stream pointer
     * @return true if more data available
     */
    public static native boolean streamHasNext(long streamPtr);

    /**
     * Get next batch from stream
     * @param streamPtr the stream pointer
     * @return byte array containing the next batch, or null if no more data
     */
    public static native byte[] streamNext(long streamPtr);

    /**
     * Close and cleanup a result stream
     * @param streamPtr the stream pointer to close
     */
    public static native void closeStream(long streamPtr);

    public static native long initCacheManagerConfig();

    // METADATA cache specific methods

    /**
     * Create metadata cache
     * @param cacheConfigPtr cache configuration pointer
     * @param sizeLimit sizeLimit for cache
     * @return cache pointer
     */
    public static native long createMetadataCache(long cacheConfigPtr, long sizeLimit);

    /**
     * Put metadata entry into cache
     * @param cachePtr the cache pointer
     * @param filePath the file path
     * @return status code
     */
    public static native boolean metadataCachePut(long cachePtr, String filePath);

    /**
     * Get metadata from cache
     * @param cachePtr the cache pointer
     * @param filePath the file path
     * @return cached metadata or null if not found
     */
    // return type boolean to be changed
    public static native boolean metadataCacheGet(long cachePtr, String filePath);

    /**
     * Remove metadata from cache
     * @param cachePtr the cache pointer
     * @param filePath the file path
     * @return status code
     */
    public static native boolean metadataCacheRemove(long cachePtr, String filePath);

    /**
     * Memory consumed by metadataCache
     * @param cachePtr the cache pointer
     * @return memory used
     */
    public static native long metadataCacheGetSize(long cachePtr);

        /**
     * Memory consumed by metadataCache
     * @param cachePtr the cache pointer
    *  @param newSizeLimit new size limit
     * @return boolean
     */
    public static native boolean metadataCacheUpdateSizeLimit(long cachePtr, long newSizeLimit);

    /**
     * Check if a file exists in metadataCache
     * @param cachePtr the cache pointer
     * @param filePath the file path
     * @return boolean
     */
    public static native boolean metadataCacheContainsFile(long cachePtr, String filePath);

    /**
     * Get all entries from the metadata cache
     * @param cachePtr the cache pointer
     * @return String array containing cache entries in triplets: [path1, size1, hitCount1, path2, size2, hitCount2, ...]
     *         Each entry consists of 3 consecutive elements: file path, size in bytes, and hit count
     */
    public static native String[] metadataCacheGetEntries(long cachePtr);

        /**
     * Clears all entries from the metadata cache
     * @param cachePtr the cache pointer
     */
    public static native void metadataCacheClear(long cachePtr);
}
