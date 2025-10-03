package com.parquet.parquetdataformat.bridge;

import org.opensearch.common.SuppressForbidden;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Locale;

/**
 * JNI bridge to the native Rust Parquet writer implementation.
 *
 * <p>This class provides the interface between Java and the native Rust library
 * that handles low-level Parquet file operations. It automatically loads the
 * appropriate native library for the current platform and architecture.
 *
 * <p>Supported platforms:
 * <ul>
 *   <li>Windows (x86, x86_64, aarch64)</li>
 *   <li>macOS (x86_64, aarch64/arm64)</li>
 *   <li>Linux (x86, x86_64, aarch64)</li>
 * </ul>
 *
 * <p>The native library is extracted from resources and loaded as a temporary file,
 * which is automatically cleaned up on JVM shutdown.
 *
 * <p>All native methods operate on Arrow C Data Interface pointers and return
 * integer status codes for error handling.
 */
public class RustBridge {

    static {
        try {
            loadNativeLibrary();
        } catch (Exception e) {
            throw new RuntimeException("Failed to load native Rust library", e);
        }
    }

    @SuppressForbidden(reason = "Need to create temp files")
    private static void loadNativeLibrary() {

        String LIB_NAME = "parquet_dataformat_jni";
        String os = System.getProperty("os.name").toLowerCase(Locale.ROOT);
        String arch = System.getProperty("os.arch").toLowerCase(Locale.ROOT);

        String osDir = os.contains("win") ? "windows" :
                os.contains("mac") ? "macos" : "linux";
        String archDir = arch.contains("aarch64") || arch.contains("arm64") ? "aarch64" :
                arch.contains("64") ? "x86_64" : "x86";

        String extension = os.contains("win") ? ".dll" :
                os.contains("mac") ? ".dylib" : ".so";

        String resourcePath = String.format(Locale.ROOT, "/native/%s/%s/lib%s%s", osDir, archDir, LIB_NAME, extension);

        try (InputStream is = RustBridge.class.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new UnsatisfiedLinkError("Native library not found in resources: " + resourcePath);
            }

            Path tempFile = Files.createTempFile("lib" + LIB_NAME, extension);

            // Register deletion hook on JVM shutdown
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                try {
                    Files.deleteIfExists(tempFile);
                } catch (IOException ignored) {}
            }));

            Files.copy(is, tempFile, StandardCopyOption.REPLACE_EXISTING);

            System.load(tempFile.toAbsolutePath().toString());
        } catch (IOException e) {
            throw new RuntimeException("Failed to load native library from resources", e);
        }
    }

    // Enhanced native methods that handle validation and provide better error reporting
    public static native void createWriter(String file, long schemaAddress) throws IOException;
    public static native void write(String file, long arrayAddress, long schemaAddress) throws IOException;
    public static native void closeWriter(String file) throws IOException;
    public static native void flushToDisk(String file) throws IOException;

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
}
