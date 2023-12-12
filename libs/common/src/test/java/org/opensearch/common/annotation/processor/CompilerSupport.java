/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.annotation.processor;

import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

import javax.tools.Diagnostic;
import javax.tools.DiagnosticCollector;
import javax.tools.JavaCompiler;
import javax.tools.JavaCompiler.CompilationTask;
import javax.tools.JavaFileObject;
import javax.tools.JavaFileObject.Kind;
import javax.tools.SimpleJavaFileObject;
import javax.tools.StandardJavaFileManager;
import javax.tools.ToolProvider;

import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.net.URI;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.security.AccessController;
import java.security.PrivilegedAction;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.stream.Collectors;
import java.util.stream.Stream;

interface CompilerSupport {
    default CompilerResult compile(String name, String... names) {
        return compileWithPackage(ApiAnnotationProcessorTests.class.getPackageName(), name, names);
    }

    default CompilerResult compileWithPackage(String pck, String name, String... names) {
        final JavaCompiler compiler = ToolProvider.getSystemJavaCompiler();
        final DiagnosticCollector<JavaFileObject> collector = new DiagnosticCollector<>();

        try (StringWriter out = new StringWriter()) {
            final StandardJavaFileManager fileManager = compiler.getStandardFileManager(collector, null, null);
            final List<JavaFileObject> files = Stream.concat(Stream.of(name), Arrays.stream(names))
                .map(f -> asSource(pck, f))
                .collect(Collectors.toList());

            final CompilationTask task = compiler.getTask(out, fileManager, collector, null, null, files);
            task.setProcessors(Collections.singleton(new ApiAnnotationProcessor()));

            if (AccessController.doPrivileged((PrivilegedAction<Boolean>) () -> task.call())) {
                return new Success();
            } else {
                return new Failure(collector.getDiagnostics());
            }
        } catch (final IOException ex) {
            throw new UncheckedIOException(ex);
        }
    }

    private static JavaFileObject asSource(String pkg, String name) {
        final String resource = "/" + pkg.replaceAll("[.]", "/") + "/" + name;
        final URL source = ApiAnnotationProcessorTests.class.getResource(resource);

        return new SimpleJavaFileObject(URI.create(source.toExternalForm()), Kind.SOURCE) {
            @Override
            public CharSequence getCharContent(boolean ignoreEncodingErrors) throws IOException {
                try (final InputStream in = ApiAnnotationProcessorTests.class.getResourceAsStream(resource)) {
                    return new String(in.readAllBytes(), StandardCharsets.UTF_8);
                }
            }
        };
    }

    class CompilerResult {}

    class Success extends CompilerResult {

    }

    class Failure extends CompilerResult {
        private final List<Diagnostic<? extends JavaFileObject>> diagnotics;

        Failure(List<Diagnostic<? extends JavaFileObject>> diagnotics) {
            this.diagnotics = diagnotics;
        }

        List<Diagnostic<? extends JavaFileObject>> diagnotics() {
            return diagnotics;
        }
    }

    class HasDiagnostic extends TypeSafeMatcher<Diagnostic<? extends JavaFileObject>> {
        private final Diagnostic.Kind kind;
        private final Matcher<String> matcher;

        HasDiagnostic(final Diagnostic.Kind kind, final Matcher<String> matcher) {
            this.kind = kind;
            this.matcher = matcher;
        }

        @Override
        public void describeTo(Description description) {
            description.appendText("diagnostic with kind ").appendValue(kind).appendText(" ");

            if (matcher != null) {
                description.appendText(" and message ");
                matcher.describeTo(description);
            }
        }

        @Override
        protected boolean matchesSafely(Diagnostic<? extends JavaFileObject> item) {
            if (!kind.equals(item.getKind())) {
                return false;
            } else if (matcher != null) {
                return matcher.matches(item.getMessage(Locale.ROOT));
            } else {
                return true;
            }
        }

        public static HasDiagnostic matching(final Diagnostic.Kind kind, final Matcher<String> matcher) {
            return new HasDiagnostic(kind, matcher);
        }

        public static HasDiagnostic matching(final Diagnostic.Kind kind) {
            return new HasDiagnostic(kind, null);
        }
    }
}
