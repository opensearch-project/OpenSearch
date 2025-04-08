/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.javaagent;

import org.opensearch.javaagent.bootstrap.AgentPolicy;

import java.io.FilePermission;
import java.lang.reflect.Method;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.nio.file.spi.FileSystemProvider;
import java.security.Policy;
import java.security.ProtectionDomain;
import java.util.Collection;

import net.bytebuddy.asm.Advice;

/**
 * FileInterceptor
 */
public class FileInterceptor {
    /**
     * FileInterceptor
     */
    public FileInterceptor() {}

    /**
     * Intercepts file operations
     *
     * @param args   arguments
     * @param method method
     * @throws Exception exceptions
     */
    @Advice.OnMethodEnter
    @SuppressWarnings({ "removal", "deprecation" })
    public static void intercept(@Advice.AllArguments Object[] args, @Advice.Origin Method method) throws Exception {
        final Policy policy = AgentPolicy.getPolicy();
        if (policy == null) {
            return; /* noop */
        }

        FileSystemProvider provider = null;
        String filePath = null;
        if (args.length > 0 && args[0] instanceof String pathStr) {
            filePath = Paths.get(pathStr).toAbsolutePath().toString();
        } else if (args.length > 0 && args[0] instanceof Path path) {
            filePath = path.toAbsolutePath().toString();
            provider = path.getFileSystem().provider();
        }

        if (filePath == null) {
            return; // No valid file path found
        }

        if (provider != null && AgentPolicy.isTrustedFileSystem(provider.getScheme()) == true) {
            return;
        }

        final StackWalker walker = StackWalker.getInstance(StackWalker.Option.RETAIN_CLASS_REFERENCE);
        final Collection<ProtectionDomain> callers = walker.walk(StackCallerProtectionDomainChainExtractor.INSTANCE);

        final String name = method.getName();
        boolean isMutating = name.equals("move") || name.equals("write") || name.startsWith("create");
        final boolean isDelete = isMutating == false ? name.startsWith("delete") : false;

        String targetFilePath = null;
        if (isMutating == false && isDelete == false) {
            if (name.equals("newByteChannel") == true || name.equals("open") == true) {
                if (args.length > 1 && args[1] instanceof OpenOption[] opts) {
                    for (final OpenOption opt : opts) {
                        if (opt != StandardOpenOption.READ) {
                            isMutating = true;
                            break;
                        }
                    }

                }
            } else if (name.equals("copy") == true) {
                if (args.length > 1 && args[1] instanceof String pathStr) {
                    targetFilePath = Paths.get(pathStr).toAbsolutePath().toString();
                } else if (args.length > 1 && args[1] instanceof Path path) {
                    targetFilePath = path.toAbsolutePath().toString();
                }
            }
        }

        // Check each permission separately
        for (final ProtectionDomain domain : callers) {
            // Handle FileChannel.open() separately to check read/write permissions properly
            if (method.getName().equals("open")) {
                if (isMutating == true && !policy.implies(domain, new FilePermission(filePath, "read,write"))) {
                    throw new SecurityException("Denied OPEN (read/write) access to file: " + filePath + ", domain: " + domain);
                } else if (!policy.implies(domain, new FilePermission(filePath, "read"))) {
                    throw new SecurityException("Denied OPEN (read) access to file: " + filePath + ", domain: " + domain);
                }
            }

            // Handle Files.copy() separately to check read/write permissions properly
            if (method.getName().equals("copy")) {
                if (!policy.implies(domain, new FilePermission(filePath, "read"))) {
                    throw new SecurityException("Denied COPY (read) access to file: " + filePath + ", domain: " + domain);
                }

                if (targetFilePath != null) {
                    if (!policy.implies(domain, new FilePermission(targetFilePath, "write"))) {
                        throw new SecurityException("Denied COPY (write) access to file: " + targetFilePath + ", domain: " + domain);
                    }
                }
            }

            // File mutating operations
            if (isMutating && !policy.implies(domain, new FilePermission(filePath, "write"))) {
                throw new SecurityException("Denied WRITE access to file: " + filePath + ", domain: " + domain);
            }

            // File deletion operations
            if (isDelete && !policy.implies(domain, new FilePermission(filePath, "delete"))) {
                throw new SecurityException("Denied DELETE access to file: " + filePath + ", domain: " + domain);
            }
        }
    }
}
