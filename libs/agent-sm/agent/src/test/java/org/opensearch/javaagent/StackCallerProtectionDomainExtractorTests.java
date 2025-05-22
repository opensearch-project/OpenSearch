/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.junit.Test;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.security.ProtectionDomain;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.junit.Assert.assertEquals;

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
        org.opensearch.javaagent.bootstrap.AccessController.doPrivileged(() -> {
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
    public void testStackTruncationWithOpenSearchAccessControllerUsingCallable() throws Exception {
        org.opensearch.javaagent.bootstrap.AccessController.doPrivileged(new Callable<Void>() {
            @Override
            public Void call() {
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
}
