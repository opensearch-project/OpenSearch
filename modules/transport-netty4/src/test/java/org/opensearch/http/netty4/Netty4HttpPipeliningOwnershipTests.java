/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.http.netty4;

import org.opensearch.secure_sm.AccessController;
import org.opensearch.test.OpenSearchTestCase;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaFileObject;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.io.StringWriter;
import java.net.URI;
import java.util.List;

/** Compile real callers to keep lifecycle ownership inaccessible outside its implementation. */
public class Netty4HttpPipeliningOwnershipTests extends OpenSearchTestCase {
    public void testHandlerCanBeInstalled() throws IOException {
        assertCompiles("""
            class Caller {
                void install(io.netty.channel.ChannelPipeline pipeline) {
                    pipeline.addLast(new Netty4HttpPipeliningHandler(null, 16));
                }
            }
            """, null);
    }

    public void testLifecycleCannotBeOverridden() throws IOException {
        assertCompiles("""
            class Caller extends Netty4HttpPipeliningHandler {
                Caller() { super(null, 16); }
                @Override
                public void channelInactive(io.netty.channel.ChannelHandlerContext context) {}
            }
            """, "compiler.err.cant.inherit.from.final");
    }

    public void testUnboundOwnerCannotBeConstructed() throws IOException {
        assertCompiles("""
            class Caller {
                Object owner = new Netty4HttpPipeliningHandler.PendingResponses(null, null, 16);
            }
            """, "compiler.err.report.access");
    }

    public void testPendingEntriesCannotEscape() throws IOException {
        assertCompiles("""
            class Caller {
                Netty4HttpPipeliningHandler.PendingResponse entry;
            }
            """, "compiler.err.report.access");
    }

    private void assertCompiles(String body, String expectedError) throws IOException {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        assertNotNull("Ownership checks require the JDK compiler", compiler);
        final DiagnosticCollector<JavaFileObject> diagnostics = new DiagnosticCollector<>();
        final JavaFileObject source = new SimpleJavaFileObject(URI.create("string:///Caller.java"), JavaFileObject.Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) {
                return "package org.opensearch.http.netty4;\n" + body;
            }
        };
        try (
            StandardJavaFileManager files = compiler.getStandardFileManager(diagnostics, null, null);
            StringWriter output = new StringWriter()
        ) {
            final JavaCompiler.CompilationTask task = compiler.getTask(
                output,
                files,
                diagnostics,
                List.of("-proc:none", "-d", createTempDir().toString()),
                null,
                List.of(source)
            );
            final boolean success = AccessController.doPrivileged(task::call);
            if (expectedError == null) {
                assertTrue(diagnostics.getDiagnostics().toString(), success);
            } else {
                assertFalse("An unsafe caller must not compile", success);
                assertTrue(
                    diagnostics.getDiagnostics().toString(),
                    diagnostics.getDiagnostics()
                        .stream()
                        .anyMatch(d -> d.getKind() == Diagnostic.Kind.ERROR && expectedError.equals(d.getCode()))
                );
            }
        }
    }
}
