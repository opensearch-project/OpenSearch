/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Assume;
import org.junit.Test;

import java.lang.invoke.MethodType;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;

public class StackCallerProtectionDomainExtractorTests {

    private static List<StackWalker.StackFrame> indirectlyCaptureStackFrames() {
        return captureStackFrames();
    }

    private static List<StackWalker.StackFrame> captureStackFrames() {
        // OPTION.RETAIN_CLASS_REFERENCE lets you do f.getDeclaringClass() if you need it
        StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        return walker.walk(frames -> frames.collect(Collectors.toList()));
    }

    @Test
    public void testSimpleProtectionDomainExtraction() throws Exception {
        StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
        Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
        assertEquals(7, protectionDomains.size());
        List<String> simpleNames = protectionDomains.stream().map(pd -> {
            try {
                return pd.getCodeSource().getLocation().toURI();
            } catch (URISyntaxException e) {
                throw new RuntimeException(e);
            }
        })
            .map(URI::getPath)
            .map(Paths::get)
            .map(Path::getFileName)
            .map(Path::toString)
            // strip trailing “-VERSION.jar” if present
            .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
            // otherwise strip “.jar”
            .map(name -> name.replaceFirst("\\.jar$", ""))
            .toList();
        assertThat(
            simpleNames,
            containsInAnyOrder(
                "gradle-worker",
                "gradle-worker-main",
                "gradle-messaging",
                "gradle-testing-base-infrastructure",
                "test",    // from the build/classes/java/test directory
                "junit",
                "gradle-testing-jvm-infrastructure"
            )
        );
    }

    @Test
    public void testIndirectlyCaptureStackFramesInListOfFrames() throws Exception {
        List<StackWalker.StackFrame> stackFrames = indirectlyCaptureStackFrames();
        List<String> methodNames = stackFrames.stream().map(StackWalker.StackFrame::getMethodName).toList();
        assertThat(methodNames, hasItem("indirectlyCaptureStackFrames"));
    }

    @Test
    @SuppressWarnings("removal")
    public void testStackTruncationWithAccessController() throws Exception {
        AccessController.doPrivileged(new PrivilegedAction<Void>() {
            @Override
            public Void run() {
                StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
                Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
                assertEquals(1, protectionDomains.size());
                List<String> simpleNames = protectionDomains.stream().map(pd -> {
                    try {
                        return pd.getCodeSource().getLocation().toURI();
                    } catch (URISyntaxException e) {
                        throw new RuntimeException(e);
                    }
                })
                    .map(URI::getPath)
                    .map(Paths::get)
                    .map(Path::getFileName)
                    .map(Path::toString)
                    // strip trailing “-VERSION.jar” if present
                    .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
                    // otherwise strip “.jar”
                    .map(name -> name.replaceFirst("\\.jar$", ""))
                    .toList();
                assertThat(
                    simpleNames,
                    containsInAnyOrder(
                        "test"    // from the build/classes/java/test directory
                    )
                );
                return null;
            }
        });
    }

    @Test
    public void testStackTruncationWithOpenSearchAccessController() {
        org.opensearch.secure_sm.AccessController.doPrivileged(() -> {
            StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
            Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
            assertEquals(1, protectionDomains.size());
            List<String> simpleNames = protectionDomains.stream().map(pd -> {
                try {
                    return pd.getCodeSource().getLocation().toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            })
                .map(URI::getPath)
                .map(Paths::get)
                .map(Path::getFileName)
                .map(Path::toString)
                // strip trailing “-VERSION.jar” if present
                .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
                // otherwise strip “.jar”
                .map(name -> name.replaceFirst("\\.jar$", ""))
                .toList();
            assertThat(
                simpleNames,
                containsInAnyOrder(
                    "test"    // from the build/classes/java/test directory
                )
            );
        });
    }

    @Test
    public void testStackTruncationWithOpenSearchAccessControllerUsingSupplier() {
        org.opensearch.secure_sm.AccessController.doPrivileged((Supplier<Void>) () -> {
            StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
            Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
            assertEquals(1, protectionDomains.size());
            List<String> simpleNames = protectionDomains.stream().map(pd -> {
                try {
                    return pd.getCodeSource().getLocation().toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            })
                .map(URI::getPath)
                .map(Paths::get)
                .map(Path::getFileName)
                .map(Path::toString)
                // strip trailing “-VERSION.jar” if present
                .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
                // otherwise strip “.jar”
                .map(name -> name.replaceFirst("\\.jar$", ""))
                .toList();
            assertThat(
                simpleNames,
                containsInAnyOrder(
                    "test"    // from the build/classes/java/test directory
                )
            );
            return null;
        });
    }

    @Test
    public void testStackTruncationWithOpenSearchAccessControllerUsingCallable() throws Exception {
        org.opensearch.secure_sm.AccessController.doPrivilegedChecked(() -> {
            StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
            Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
            assertEquals(1, protectionDomains.size());
            List<String> simpleNames = protectionDomains.stream().map(pd -> {
                try {
                    return pd.getCodeSource().getLocation().toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            })
                .map(URI::getPath)
                .map(Paths::get)
                .map(Path::getFileName)
                .map(Path::toString)
                // strip trailing “-VERSION.jar” if present
                .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
                // otherwise strip “.jar”
                .map(name -> name.replaceFirst("\\.jar$", ""))
                .toList();
            assertThat(
                simpleNames,
                containsInAnyOrder(
                    "test"    // from the build/classes/java/test directory
                )
            );
            return null;
        });
    }

    @Test
    public void testAccessControllerUsingCallableThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            org.opensearch.secure_sm.AccessController.doPrivilegedChecked(() -> { throw new IllegalArgumentException("Test exception"); });
        });
    }

    @Test
    public void testStackTruncationWithOpenSearchAccessControllerUsingCheckedRunnable() throws IllegalArgumentException {
        org.opensearch.secure_sm.AccessController.doPrivilegedChecked(() -> {
            StackCallerProtectionDomainChainExtractor extractor = StackCallerProtectionDomainChainExtractor.INSTANCE;
            Set<ProtectionDomain> protectionDomains = (Set<ProtectionDomain>) extractor.apply(captureStackFrames().stream());
            assertEquals(1, protectionDomains.size());
            List<String> simpleNames = protectionDomains.stream().map(pd -> {
                try {
                    return pd.getCodeSource().getLocation().toURI();
                } catch (URISyntaxException e) {
                    throw new RuntimeException(e);
                }
            })
                .map(URI::getPath)
                .map(Paths::get)
                .map(Path::getFileName)
                .map(Path::toString)
                // strip trailing “-VERSION.jar” if present
                .map(name -> name.replaceFirst("-\\d[\\d\\.]*\\.jar$", ""))
                // otherwise strip “.jar”
                .map(name -> name.replaceFirst("\\.jar$", ""))
                .toList();
            assertThat(
                simpleNames,
                containsInAnyOrder(
                    "test"    // from the build/classes/java/test directory
                )
            );
        });
    }

    @Test
    public void testAccessControllerUsingCheckedRunnableThrowsException() {
        assertThrows(IllegalArgumentException.class, () -> {
            org.opensearch.secure_sm.AccessController.doPrivilegedChecked(() -> { throw new IllegalArgumentException("Test exception"); });
        });
    }

    private static final class FakeFrame implements StackWalker.StackFrame {
        private final Class<?> clazz;
        private final String method;

        FakeFrame(Class<?> clazz, String method) {
            this.clazz = clazz;
            this.method = method;
        }

        @Override
        public Class<?> getDeclaringClass() {
            return clazz;
        }

        @Override
        public String getClassName() {
            return clazz.getName();
        }

        @Override
        public String getMethodName() {
            return method;
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
            return new StackTraceElement(getClassName(), getMethodName(), null, -1);
        }

        // JDK 21 methods; stub minimally
        @Override
        public String getDescriptor() {
            return "()V";
        }

        @Override
        public int getByteCodeIndex() {
            return -1;
        }

        @Override
        public MethodType getMethodType() {
            return MethodType.methodType(void.class);
        }
    }

    @Test
    public void testFiltersJrtProtocol() {
        // Guard: ensure HttpClient is truly from jrt:
        ProtectionDomain pd = java.net.http.HttpClient.class.getProtectionDomain();
        Assume.assumeTrue(
            pd != null
                && pd.getCodeSource() != null
                && pd.getCodeSource().getLocation() != null
                && "jrt".equals(pd.getCodeSource().getLocation().getProtocol())
        );

        StackWalker.StackFrame jrtFrame = new FakeFrame(java.net.http.HttpClient.class, "send");
        StackWalker.StackFrame fileFrame = new FakeFrame(StackCallerProtectionDomainExtractorTests.class, "helper");

        Set<ProtectionDomain> pds = (Set<ProtectionDomain>) StackCallerProtectionDomainChainExtractor.INSTANCE.apply(
            Stream.of(jrtFrame, fileFrame)
        );

        // Only the file: PD should remain
        assertEquals(1, pds.size());
        assertThat(
            pds.stream().map(x -> x.getCodeSource().getLocation().getProtocol()).collect(Collectors.toSet()),
            containsInAnyOrder("file")
        );
    }

}
