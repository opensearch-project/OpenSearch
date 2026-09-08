/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;
import org.opensearch.javaagent.bootstrap.internal.BuiltinClassLoaderResourceBoundary;
import org.opensearch.javaagent.bootstrap.internal.StackCallerProtectionDomainChainExtractor;

import java.io.File;
import java.io.FilePermission;
import java.nio.file.Path;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Collection;
import java.util.List;

import net.bytebuddy.asm.Advice;

/**
 * Interceptor for java.io.FileInputStream to enforce file read permissions.
 * This closes the gap where legacy IO file access bypasses the NIO-based FileInterceptor.
 */
public class FileInputStreamInterceptor {
    /**
     * FileInputStreamInterceptor
     */
    public FileInputStreamInterceptor() {}

    /**
     * Intercepts FileInputStream constructor that takes a File argument
     *
     * @param file the File being opened for reading
     * @throws SecurityException if access is denied
     */
    @Advice.OnMethodEnter
    @SuppressWarnings("removal")
    public static void onEnter(@Advice.Argument(0) Object file) throws SecurityException {
        final Policy policy = AgentPolicy.getPolicy();
        if (policy == null) {
            return;
        }

        String filePath = null;
        if (file instanceof String path) {
            filePath = Path.of(path).toAbsolutePath().normalize().toString();
        } else if (file instanceof File f) {
            filePath = f.toPath().toAbsolutePath().normalize().toString();
        }

        if (filePath == null) {
            return;
        }

        final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        final List<StackWalker.StackFrame> frames = walker.walk(BuiltinClassLoaderResourceBoundary.INSTANCE);

        // Loading a class from a class-path directory is infrastructure performed by the built-in class loader.
        // The protection domain that triggered lazy class loading must not be treated as directly reading the
        // underlying .class file. Keep this boundary deliberately narrow so direct FileInputStream, URL, and XML
        // reads from an untrusted domain continue through the policy check below.
        if (BuiltinClassLoaderResourceBoundary.matches(frames)) {
            return;
        }

        final Collection<ProtectionDomain> callers = StackCallerProtectionDomainChainExtractor.INSTANCE.apply(frames.stream());

        final FilePermission permission = new FilePermission(filePath, "read");
        for (ProtectionDomain domain : callers) {
            if (!policy.implies(domain, permission)) {
                throw new SecurityException("Denied READ access to file: " + filePath + ", domain: " + domain);
            }
        }
    }
}
