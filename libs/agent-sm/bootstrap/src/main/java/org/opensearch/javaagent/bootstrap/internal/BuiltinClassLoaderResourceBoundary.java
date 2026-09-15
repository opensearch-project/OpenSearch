/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent.bootstrap.internal;

import java.lang.StackWalker.StackFrame;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

/** Collects stack frames and identifies built-in class loading from a class-path directory. */
public final class BuiltinClassLoaderResourceBoundary implements Function<Stream<StackFrame>, List<StackFrame>> {
    /** Shared stateless instance. */
    public static final BuiltinClassLoaderResourceBoundary INSTANCE = new BuiltinClassLoaderResourceBoundary();

    private static final String URL_CLASSPATH_FILE_RESOURCE = "jdk.internal.loader.URLClassPath$FileLoader$1";
    private static final String BUILTIN_CLASS_LOADER = "jdk.internal.loader.BuiltinClassLoader";

    private BuiltinClassLoaderResourceBoundary() {}

    @Override
    public List<StackFrame> apply(Stream<StackFrame> frames) {
        return frames.toList();
    }

    /**
     * Returns whether the stack represents the built-in class loader defining a class from a class-path directory.
     *
     * @param frames current stack frames
     * @return {@code true} when the file stream is reading a class resource for the built-in class loader
     */
    public static boolean matches(List<StackFrame> frames) {
        boolean opensClassPathResource = false;
        boolean definesClass = false;

        for (StackFrame frame : frames) {
            if (URL_CLASSPATH_FILE_RESOURCE.equals(frame.getClassName()) && "getInputStream".equals(frame.getMethodName())) {
                opensClassPathResource = true;
            } else if (BUILTIN_CLASS_LOADER.equals(frame.getClassName()) && "defineClass".equals(frame.getMethodName())) {
                definesClass = true;
            }

            if (opensClassPathResource && definesClass) {
                return true;
            }
        }

        return false;
    }
}
