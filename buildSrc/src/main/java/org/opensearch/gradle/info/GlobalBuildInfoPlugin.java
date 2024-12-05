/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.gradle.info;

import org.apache.commons.io.IOUtils;
import org.opensearch.gradle.BwcVersions;
import org.opensearch.gradle.util.Util;
import org.gradle.api.GradleException;
import org.gradle.api.JavaVersion;
import org.gradle.api.Plugin;
import org.gradle.api.Project;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.ProviderFactory;
import org.gradle.internal.jvm.Jvm;
import org.gradle.internal.jvm.inspection.JvmInstallationMetadata;
import org.gradle.internal.jvm.inspection.JvmMetadataDetector;
import org.gradle.jvm.toolchain.internal.InstallationLocation;
import org.gradle.util.GradleVersion;

import javax.inject.Inject;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.MethodType;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;

public class GlobalBuildInfoPlugin implements Plugin<Project> {
    private static final Logger LOGGER = Logging.getLogger(GlobalBuildInfoPlugin.class);
    private static final String DEFAULT_LEGACY_VERSION_JAVA_FILE_PATH = "libs/core/src/main/java/org/opensearch/LegacyESVersion.java";
    private static final String DEFAULT_VERSION_JAVA_FILE_PATH = "libs/core/src/main/java/org/opensearch/Version.java";
    private static Integer _defaultParallel = null;

    private final JvmMetadataDetector jvmMetadataDetector;
    private final ObjectFactory objects;
    private final ProviderFactory providers;

    @Inject
    public GlobalBuildInfoPlugin(JvmMetadataDetector jvmMetadataDetector, ObjectFactory objects, ProviderFactory providers) {
        this.jvmMetadataDetector = jvmMetadataDetector;
        this.objects = objects;
        this.providers = providers;
    }

    @Override
    public void apply(Project project) {
        if (project != project.getRootProject()) {
            throw new IllegalStateException(this.getClass().getName() + " can only be applied to the root project.");
        }
        GradleVersion minimumGradleVersion = GradleVersion.version(Util.getResourceContents("/minimumGradleVersion"));
        if (GradleVersion.current().compareTo(minimumGradleVersion) < 0) {
            throw new GradleException("Gradle " + minimumGradleVersion.getVersion() + "+ is required");
        }

        JavaVersion minimumCompilerVersion = JavaVersion.toVersion(Util.getResourceContents("/minimumCompilerVersion"));
        JavaVersion minimumRuntimeVersion = JavaVersion.toVersion(Util.getResourceContents("/minimumRuntimeVersion"));

        Optional<File> runtimeJavaHomeOpt = findRuntimeJavaHome();
        File runtimeJavaHome = runtimeJavaHomeOpt.orElse(Jvm.current().getJavaHome());

        File rootDir = project.getRootDir();
        GitInfo gitInfo = gitInfo(rootDir);

        BuildParams.init(params -> {
            // Initialize global build parameters
            boolean isInternal = GlobalBuildInfoPlugin.class.getResource("/buildSrc.marker") != null;

            params.reset();
            params.setRuntimeJavaHome(runtimeJavaHome);
            params.setRuntimeJavaVersion(determineJavaVersion("runtime java.home", runtimeJavaHome, minimumRuntimeVersion));
            params.setIsRuntimeJavaHomeSet(runtimeJavaHomeOpt.isPresent());
            params.setRuntimeJavaDetails(getJavaInstallation(runtimeJavaHome).getDisplayName());
            params.setJavaVersions(getAvailableJavaVersions(minimumCompilerVersion));
            params.setMinimumCompilerVersion(minimumCompilerVersion);
            params.setMinimumRuntimeVersion(minimumRuntimeVersion);
            params.setGradleJavaVersion(Jvm.current().getJavaVersion());
            params.setGitRevision(gitInfo.getRevision());
            params.setGitOrigin(gitInfo.getOrigin());
            params.setBuildDate(Util.getBuildDate(ZonedDateTime.now(ZoneOffset.UTC)));
            params.setTestSeed(getTestSeed());
            params.setIsCi(System.getenv("JENKINS_URL") != null);
            params.setIsInternal(isInternal);
            params.setDefaultParallel(findDefaultParallel(project));
            params.setInFipsJvm(Util.getBooleanProperty("tests.fips.enabled", false));
            params.setIsSnapshotBuild(Util.getBooleanProperty("build.snapshot", true));
            if (isInternal) {
                params.setBwcVersions(resolveBwcVersions(rootDir));
            }
        });

        // Print global build info header just before task execution
        project.getGradle().getTaskGraph().whenReady(graph -> logGlobalBuildInfo());
    }

    /* Introspect all versions of ES that may be tested against for backwards
     * compatibility. It is *super* important that this logic is the same as the
     * logic in VersionUtils.java. */
    private static BwcVersions resolveBwcVersions(File root) {
        // todo redesign this terrible unreliable hack; should NEVER rely on parsing a source file
        // for now, we hack the hack
        File versionsFile = new File(root, DEFAULT_VERSION_JAVA_FILE_PATH);
        File legacyVersionsFile = new File(root, DEFAULT_LEGACY_VERSION_JAVA_FILE_PATH);
        try (FileInputStream fis = new FileInputStream(versionsFile); FileInputStream fis2 = new FileInputStream(legacyVersionsFile)) {
            List<String> versionLines = IOUtils.readLines(fis, "UTF-8");
            versionLines.addAll(IOUtils.readLines(fis2, "UTF-8"));
            return new BwcVersions(versionLines);
        } catch (IOException e) {
            throw new IllegalStateException("Unable to resolve bwc versions from versionsFile.", e);
        }
    }

    private void logGlobalBuildInfo() {
        final String osName = System.getProperty("os.name");
        final String osVersion = System.getProperty("os.version");
        final String osArch = System.getProperty("os.arch");
        final Jvm gradleJvm = Jvm.current();
        final String gradleJvmDetails = getJavaInstallation(gradleJvm.getJavaHome()).getDisplayName();

        LOGGER.quiet("=======================================");
        LOGGER.quiet("OpenSearch Build Hamster says Hello!");
        LOGGER.quiet("  Gradle Version        : " + GradleVersion.current().getVersion());
        LOGGER.quiet("  OS Info               : " + osName + " " + osVersion + " (" + osArch + ")");
        if (BuildParams.getIsRuntimeJavaHomeSet()) {
            String runtimeJvmDetails = getJavaInstallation(BuildParams.getRuntimeJavaHome()).getDisplayName();
            LOGGER.quiet("  Runtime JDK Version   : " + BuildParams.getRuntimeJavaVersion() + " (" + runtimeJvmDetails + ")");
            LOGGER.quiet("  Runtime java.home     : " + BuildParams.getRuntimeJavaHome());
            LOGGER.quiet("  Gradle JDK Version    : " + gradleJvm.getJavaVersion() + " (" + gradleJvmDetails + ")");
            LOGGER.quiet("  Gradle java.home      : " + gradleJvm.getJavaHome());
        } else {
            LOGGER.quiet("  JDK Version           : " + gradleJvm.getJavaVersion() + " (" + gradleJvmDetails + ")");
            LOGGER.quiet("  JAVA_HOME             : " + gradleJvm.getJavaHome());
        }
        LOGGER.quiet("  Random Testing Seed   : " + BuildParams.getTestSeed());
        LOGGER.quiet("  In FIPS 140 mode      : " + BuildParams.isInFipsJvm());
        LOGGER.quiet("=======================================");
    }

    private JavaVersion determineJavaVersion(String description, File javaHome, JavaVersion requiredVersion) {
        JvmInstallationMetadata installation = getJavaInstallation(javaHome);
        JavaVersion actualVersion = installation.getLanguageVersion();
        if (actualVersion.isCompatibleWith(requiredVersion) == false) {
            throwInvalidJavaHomeException(
                description,
                javaHome,
                Integer.parseInt(requiredVersion.getMajorVersion()),
                Integer.parseInt(actualVersion.getMajorVersion())
            );
        }

        return actualVersion;
    }

    private JvmInstallationMetadata getJavaInstallation(File javaHome) {
        InstallationLocation location = null;

        try {
            try {
                // The InstallationLocation(File, String) is used by Gradle pre-8.8
                location = (InstallationLocation) MethodHandles.publicLookup()
                    .findConstructor(InstallationLocation.class, MethodType.methodType(void.class, File.class, String.class))
                    .invokeExact(javaHome, "Java home");
            } catch (Throwable ex) {
                // The InstallationLocation::userDefined is used by Gradle post-8.7
                location = (InstallationLocation) MethodHandles.publicLookup()
                    .findStatic(
                        InstallationLocation.class,
                        "userDefined",
                        MethodType.methodType(InstallationLocation.class, File.class, String.class)
                    )
                    .invokeExact(javaHome, "Java home");

            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Unable to find suitable InstallationLocation constructor / factory method", ex);
        }

        try {
            try {
                // The getMetadata(File) is used by Gradle pre-7.6
                return (JvmInstallationMetadata) MethodHandles.publicLookup()
                    .findVirtual(JvmMetadataDetector.class, "getMetadata", MethodType.methodType(JvmInstallationMetadata.class, File.class))
                    .bindTo(jvmMetadataDetector)
                    .invokeExact(location.getLocation());
            } catch (NoSuchMethodException | IllegalAccessException ex) {
                // The getMetadata(InstallationLocation) is used by Gradle post-7.6
                return (JvmInstallationMetadata) MethodHandles.publicLookup()
                    .findVirtual(
                        JvmMetadataDetector.class,
                        "getMetadata",
                        MethodType.methodType(JvmInstallationMetadata.class, InstallationLocation.class)
                    )
                    .bindTo(jvmMetadataDetector)
                    .invokeExact(location);
            }
        } catch (Throwable ex) {
            throw new IllegalStateException("Unable to find suitable JvmMetadataDetector::getMetadata", ex);
        }
    }

    private List<JavaHome> getAvailableJavaVersions(JavaVersion minimumCompilerVersion) {
        final List<JavaHome> javaVersions = new ArrayList<>();
        for (int v = 8; v <= Integer.parseInt(minimumCompilerVersion.getMajorVersion()); v++) {
            int version = v;
            String javaHomeEnvVarName = getJavaHomeEnvVarName(Integer.toString(version));
            if (System.getenv(javaHomeEnvVarName) != null) {
                File javaHomeDirectory = new File(findJavaHome(Integer.toString(version)));
                JvmInstallationMetadata javaInstallation = getJavaInstallation(javaHomeDirectory);
                JavaHome javaHome = JavaHome.of(version, providers.provider(() -> {
                    int actualVersion = Integer.parseInt(javaInstallation.getLanguageVersion().getMajorVersion());
                    if (actualVersion != version) {
                        throwInvalidJavaHomeException("env variable " + javaHomeEnvVarName, javaHomeDirectory, version, actualVersion);
                    }
                    return javaHomeDirectory;
                }));
                javaVersions.add(javaHome);
            }
        }
        return javaVersions;
    }

    private static String getTestSeed() {
        String testSeedProperty = System.getProperty("tests.seed");
        final String testSeed;
        if (testSeedProperty == null) {
            long seed = new Random(System.currentTimeMillis()).nextLong();
            testSeed = Long.toUnsignedString(seed, 16).toUpperCase(Locale.ROOT);
        } else {
            testSeed = testSeedProperty;
        }
        return testSeed;
    }

    private static void throwInvalidJavaHomeException(String description, File javaHome, int expectedVersion, int actualVersion) {
        String message = String.format(
            Locale.ROOT,
            "The %s must be set to a JDK installation directory for Java %d but is [%s] corresponding to [%s]",
            description,
            expectedVersion,
            javaHome,
            actualVersion
        );

        throw new GradleException(message);
    }

    private static Optional<File> findRuntimeJavaHome() {
        String runtimeJavaProperty = System.getProperty("runtime.java");

        if (runtimeJavaProperty != null) {
            return Optional.of(new File(findJavaHome(runtimeJavaProperty)));
        }

        return System.getenv("RUNTIME_JAVA_HOME") == null ? Optional.empty() : Optional.of(new File(System.getenv("RUNTIME_JAVA_HOME")));
    }

    private static String findJavaHome(String version) {
        String versionedJavaHome = System.getenv(getJavaHomeEnvVarName(version));
        if (versionedJavaHome == null) {
            final String exceptionMessage = String.format(
                Locale.ROOT,
                "$%s must be set to build OpenSearch. "
                    + "Note that if the variable was just set you "
                    + "might have to run `./gradlew --stop` for "
                    + "it to be picked up. See https://github.com/elastic/elasticsearch/issues/31399 details.",
                getJavaHomeEnvVarName(version)
            );

            throw new GradleException(exceptionMessage);
        }
        return versionedJavaHome;
    }

    private static String getJavaHomeEnvVarName(String version) {
        return "JAVA" + version + "_HOME";
    }

    private static int findDefaultParallel(Project project) {
        // It's safe to store this in a static variable since it's just a primitive so leaking memory isn't an issue
        if (_defaultParallel == null) {
            _defaultParallel = Math.max(1, Runtime.getRuntime().availableProcessors() / 2);
        }

        return _defaultParallel;
    }

    public static GitInfo gitInfo(File rootDir) {
        try {
            /*
             * We want to avoid forking another process to run git rev-parse HEAD. Instead, we will read the refs manually. The
             * documentation for this follows from https://git-scm.com/docs/gitrepository-layout and https://git-scm.com/docs/git-worktree.
             *
             * There are two cases to consider:
             *  - a plain repository with .git directory at the root of the working tree
             *  - a worktree with a plain text .git file at the root of the working tree
             *
             * In each case, our goal is to parse the HEAD file to get either a ref or a bare revision (in the case of being in detached
             * HEAD state).
             *
             * In the case of a plain repository, we can read the HEAD file directly, resolved directly from the .git directory.
             *
             * In the case of a worktree, we read the gitdir from the plain text .git file. This resolves to a directory from which we read
             * the HEAD file and resolve commondir to the plain git repository.
             */
            final Path dotGit = rootDir.toPath().resolve(".git");
            final String revision;
            if (Files.exists(dotGit) == false) {
                return new GitInfo("unknown", "unknown");
            }
            final Path head;
            final Path gitDir;
            if (Files.isDirectory(dotGit)) {
                // this is a git repository, we can read HEAD directly
                head = dotGit.resolve("HEAD");
                gitDir = dotGit;
            } else {
                // this is a git worktree, follow the pointer to the repository
                final Path workTree = Paths.get(readFirstLine(dotGit).substring("gitdir:".length()).trim());
                if (Files.exists(workTree) == false) {
                    return new GitInfo("unknown", "unknown");
                }
                head = workTree.resolve("HEAD");
                final Path commonDir = Paths.get(readFirstLine(workTree.resolve("commondir")));
                if (commonDir.isAbsolute()) {
                    gitDir = commonDir;
                } else {
                    // this is the common case
                    gitDir = workTree.resolve(commonDir);
                }
            }
            final String ref = readFirstLine(head);
            if (ref.startsWith("ref:")) {
                String refName = ref.substring("ref:".length()).trim();
                Path refFile = gitDir.resolve(refName);
                if (Files.exists(refFile)) {
                    revision = readFirstLine(refFile);
                } else if (Files.exists(gitDir.resolve("packed-refs"))) {
                    // Check packed references for commit ID
                    Pattern p = Pattern.compile("^([a-f0-9]{40}) " + refName + "$");
                    try (Stream<String> lines = Files.lines(gitDir.resolve("packed-refs"))) {
                        revision = lines.map(p::matcher)
                            .filter(Matcher::matches)
                            .map(m -> m.group(1))
                            .findFirst()
                            .orElseThrow(() -> new IOException("Packed reference not found for refName " + refName));
                    }
                } else {
                    throw new GradleException("Can't find revision for refName " + refName);
                }
            } else {
                // we are in detached HEAD state
                revision = ref;
            }
            return new GitInfo(revision, findOriginUrl(gitDir.resolve("config")));
        } catch (final IOException e) {
            // for now, do not be lenient until we have better understanding of real-world scenarios where this happens
            throw new GradleException("unable to read the git revision", e);
        }
    }

    private static String findOriginUrl(final Path configFile) throws IOException {
        Map<String, String> props = new HashMap<>();

        try (Stream<String> stream = Files.lines(configFile, StandardCharsets.UTF_8)) {
            Iterator<String> lines = stream.iterator();
            boolean foundOrigin = false;
            while (lines.hasNext()) {
                String line = lines.next().trim();
                if (line.startsWith(";") || line.startsWith("#")) {
                    // ignore comments
                    continue;
                }
                if (foundOrigin) {
                    if (line.startsWith("[")) {
                        // we're on to the next config item so stop looking
                        break;
                    }
                    String[] pair = line.trim().split("=");
                    props.put(pair[0].trim(), pair[1].trim());
                } else {
                    if (line.equals("[remote \"origin\"]")) {
                        foundOrigin = true;
                    }
                }
            }
        }

        String originUrl = props.get("url");
        return originUrl == null ? "unknown" : originUrl;
    }

    private static String readFirstLine(final Path path) throws IOException {
        String firstLine;
        try (Stream<String> lines = Files.lines(path, StandardCharsets.UTF_8)) {
            firstLine = lines.findFirst().orElseThrow(() -> new IOException("file [" + path + "] is empty"));
        }
        return firstLine;
    }

    public static class GitInfo {
        private final String revision;
        private final String origin;

        GitInfo(String revision, String origin) {
            this.revision = revision;
            this.origin = origin;
        }

        public String getRevision() {
            return revision;
        }

        public String getOrigin() {
            return origin;
        }
    }
}
