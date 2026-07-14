/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.internal.BuiltinClassLoaderResourceBoundary;
import org.junit.Test;

import java.lang.invoke.MethodType;
import java.util.List;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FileInputStreamInterceptorTests {

    @Test
    public void testRecognizesBuiltinClassResourceLoad() {
        assertTrue(
            BuiltinClassLoaderResourceBoundary.matches(
                List.of(
                    new FakeFrame("java.io.FileInputStream", "<init>"),
                    new FakeFrame("jdk.internal.loader.URLClassPath$FileLoader$1", "getInputStream"),
                    new FakeFrame("jdk.internal.loader.Resource", "getByteBuffer"),
                    new FakeFrame("jdk.internal.loader.BuiltinClassLoader", "defineClass"),
                    new FakeFrame("example.UntrustedCaller", "run")
                )
            )
        );
    }

    @Test
    public void testDoesNotTrustFileLoaderWithoutClassDefinition() {
        assertFalse(
            BuiltinClassLoaderResourceBoundary.matches(
                List.of(
                    new FakeFrame("java.io.FileInputStream", "<init>"),
                    new FakeFrame("jdk.internal.loader.URLClassPath$FileLoader$1", "getInputStream"),
                    new FakeFrame("example.UntrustedCaller", "run")
                )
            )
        );
    }

    @Test
    public void testDoesNotTrustClassDefinitionWithoutFileResourceLoad() {
        assertFalse(
            BuiltinClassLoaderResourceBoundary.matches(
                List.of(
                    new FakeFrame("java.io.FileInputStream", "<init>"),
                    new FakeFrame("jdk.internal.loader.BuiltinClassLoader", "defineClass"),
                    new FakeFrame("example.UntrustedCaller", "run")
                )
            )
        );
    }

    private static final class FakeFrame implements StackWalker.StackFrame {
        private final String className;
        private final String methodName;

        private FakeFrame(String className, String methodName) {
            this.className = className;
            this.methodName = methodName;
        }

        @Override
        public String getClassName() {
            return className;
        }

        @Override
        public String getMethodName() {
            return methodName;
        }

        @Override
        public Class<?> getDeclaringClass() {
            return FileInputStreamInterceptorTests.class;
        }

        @Override
        public MethodType getMethodType() {
            return MethodType.methodType(void.class);
        }

        @Override
        public String getDescriptor() {
            return "()V";
        }

        @Override
        public int getByteCodeIndex() {
            return -1;
        }

        @Override
        public String getFileName() {
            return null;
        }

        @Override
        public int getLineNumber() {
            return -1;
        }

        @Override
        public boolean isNativeMethod() {
            return false;
        }

        @Override
        public StackTraceElement toStackTraceElement() {
            return new StackTraceElement(className, methodName, null, -1);
        }
    }
}
