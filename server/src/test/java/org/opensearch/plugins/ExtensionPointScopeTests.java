/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexModule;
import org.opensearch.index.store.FsDirectoryFactory;
import org.opensearch.indices.recovery.RecoveryState;
import org.opensearch.node.MockNode;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.plugins.*;

import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;
import java.net.URL;
import java.util.Enumeration;
import java.util.jar.JarFile;
import java.util.jar.JarEntry;
// import com.google.common.reflect.ClassPath;

import static org.opensearch.test.hamcrest.RegexMatcher.matches;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasToString;
import static org.hamcrest.Matchers.contains;

public class ExtensionPointScopeTests extends OpenSearchTestCase {

    public void testAllExtensionPointsHaveAssociatedScope() throws Exception {
        // 
        final Set<String> extensionPointScopes = Arrays.stream(ExtensionPointScopes.values()).map(e -> e.name()).collect(Collectors.toSet());
        final String packageName = getClass().getPackage().getName();
        final ClassLoader classLoader = ClassLoader.getSystemClassLoader();
        final Set<String> pluginClassNames = new HashSet<>();
        Enumeration<URL> resources = classLoader.getResources(packageName.replaceAll("\\.", "/"));
        while (resources.hasMoreElements()) {
            final URL resource = resources.nextElement();
            // Open the jar file and search for class files
            String jarFileName = resource.getPath();
            try {
                jarFileName = jarFileName.substring(0, jarFileName.indexOf('!'));
            } catch (Exception e) {
                System.err.println(e);
//                continue;
            }

            try (JarFile jarFile = new JarFile(jarFileName)) {
                Enumeration<JarEntry> entries = jarFile.entries();
                while (entries.hasMoreElements()) {
                    JarEntry entry = entries.nextElement();
                    String entryName = entry.getName();
                    if (entryName.endsWith(".class") && entryName.startsWith(packageName.replace('.', '/'))) {
                        String className = entryName.substring(0, entryName.length() - ".class".length()).replace('/', '.');
                        pluginClassNames.add(className);
                    }
                }
            } catch (Exception e) {
                System.err.println(e);
                // Ignore any exceptions
                // continue;
            }

        }
        System.out.println(pluginClassNames);
        System.out.println(extensionPointScopes);
        pluginClassNames.forEach(name -> {
            assertThat(extensionPointScopes, contains(name));
        });
        
        extensionPointScopes.forEach(name -> {
            assertThat(pluginClassNames, contains(name));
        });
    }
}