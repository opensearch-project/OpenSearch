package com.parquet.parquetdataformat.bridge;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;

/**
 * JNI bridge to the native Rust Parquet writer implementation.
 *
 * <p>This class provides the interface between Java and the native Rust library
 * that handles low-level Parquet file operations. It automatically loads the
 * appropriate native library for the current platform and architecture.
 *
 * <p>The native library is extracted from resources and loaded as a temporary file,
 * which is automatically cleaned up on JVM shutdown.
 *
 * <p>All native methods operate on Arrow C Data Interface pointers and return
 * integer status codes for error handling.
 */
public class RustBridge {

    static {
        NativeLibraryLoader.load("parquet_dataformat_jni");
    }

    // Enhanced native methods that handle validation and provide better error reporting
    public static native void createWriter(String file, long schemaAddress) throws IOException;
    public static native void write(String file, long arrayAddress, long schemaAddress) throws IOException;
    public static native void closeWriter(String file) throws IOException;
    public static native void flushToDisk(String file) throws IOException;
    public static native long getFilteredNativeBytesUsed(String pathPrefix);

    // State and metrics methods handled on Rust side
    public static native boolean writerExists(String file);
    public static native long getWriteCount(String file);
    public static native long getTotalRows(String file);
    public static native String[] getActiveWriters();

    // Validation helpers that could be implemented natively for better performance
    public static boolean isValidFileName(String fileName) {
        return fileName != null && !fileName.trim().isEmpty();
    }

    public static boolean isValidMemoryAddress(long address) {
        return address != 0;
    }


    // DATAFUSION specific native methods starts here

    // Record batch and streaming related methods
    public static native String nativeNextBatch(long streamPtr);

    public static native void nativeCloseStream(long streamPtr);


    // Native method declarations - these will be implemented in the JNI library
    public static native void nativeRegisterDirectory(String tableName, String directoryPath, String[] files, long runtimeId);

    public static native long nativeCreateSessionContext(String[] configKeys, String[] configValues);

    public static native long nativeExecuteSubstraitQuery(long sessionContextPtr, byte[] substraitPlan);

    public static native void nativeCloseSessionContext(long sessionContextPtr);

    public static native void mergeParquetFilesInRust(List<Path> inputFiles, String outputFile);
}
