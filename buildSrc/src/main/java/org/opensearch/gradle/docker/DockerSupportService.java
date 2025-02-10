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

package org.opensearch.gradle.docker;

import org.apache.tools.ant.taskdefs.condition.Os;
import org.opensearch.gradle.Version;
import org.opensearch.gradle.info.BuildParams;
import org.gradle.api.GradleException;
import org.gradle.api.logging.Logger;
import org.gradle.api.logging.Logging;
import org.gradle.api.services.BuildService;
import org.gradle.api.services.BuildServiceParameters;
import org.gradle.process.ExecOperations;
import org.gradle.process.ExecResult;

import javax.inject.Inject;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Build service for detecting available Docker installation and checking for compatibility with OpenSearch Docker image build
 * requirements. This includes a minimum version requirement, as well as the ability to run privileged commands.
 */
public abstract class DockerSupportService implements BuildService<DockerSupportService.Parameters> {

    private static Logger LOGGER = Logging.getLogger(DockerSupportService.class);
    // Defines the possible locations of the Docker CLI. These will be searched in order.
    private static String[] DOCKER_BINARIES_UNIX = { "/usr/bin/docker", "/usr/local/bin/docker" };

    private static String[] DOCKER_BINARIES_WINDOWS = {
        System.getenv("PROGRAMFILES") + "\\Docker\\Docker\\resources\\bin\\docker.exe",
        System.getenv("SystemRoot") + "\\System32\\docker.exe" /* Github Actions */ };

    private static String[] DOCKER_BINARIES = Os.isFamily(Os.FAMILY_WINDOWS) ? DOCKER_BINARIES_WINDOWS : DOCKER_BINARIES_UNIX;

    private static String[] DOCKER_COMPOSE_BINARIES_UNIX = { "/usr/local/bin/docker-compose", "/usr/bin/docker-compose" };

    private static String[] DOCKER_COMPOSE_BINARIES_WINDOWS = {
        System.getenv("PROGRAMFILES") + "\\Docker\\Docker\\resources\\bin\\docker-compose.exe" };

    private static String[] DOCKER_COMPOSE_BINARIES = Os.isFamily(Os.FAMILY_WINDOWS)
        ? DOCKER_COMPOSE_BINARIES_WINDOWS
        : DOCKER_COMPOSE_BINARIES_UNIX;

    private static final Version MINIMUM_DOCKER_VERSION = Version.fromString("17.05.0");

    private final ExecOperations execOperations;
    private DockerAvailability dockerAvailability;

    @Inject
    public DockerSupportService(ExecOperations execOperations) {
        this.execOperations = execOperations;
    }

    /**
     * Searches for a functional Docker installation, and returns information about the search.
     *
     * @return the results of the search.
     */
    public DockerAvailability getDockerAvailability() {
        if (this.dockerAvailability == null) {
            String dockerPath = null;
            Result lastResult = null;
            Version version = null;
            boolean isVersionHighEnough = false;
            DockerComposeAvailability dockerComposeAvailability = null;

            // Check if the Docker binary exists
            final Optional<String> dockerBinary = getDockerPath();
            if (isExcludedOs() == false && dockerBinary.isPresent()) {
                dockerPath = dockerBinary.get();

                // Since we use a multi-stage Docker build, check the Docker version meets minimum requirement
                lastResult = runCommand(execOperations, dockerPath, "version", "--format", "{{.Server.Version}}");

                if (lastResult.isSuccess()) {
                    version = Version.fromString(lastResult.stdout.trim(), Version.Mode.RELAXED);

                    isVersionHighEnough = version.onOrAfter(MINIMUM_DOCKER_VERSION);

                    if (isVersionHighEnough) {
                        // Check that we can execute a privileged command
                        lastResult = runCommand(execOperations, dockerPath, "images");
                        // If docker all checks out, see if docker-compose is available and working
                        if (lastResult.isSuccess()) {
                            dockerComposeAvailability = DockerComposeAvailability.detect(execOperations, dockerPath).orElse(null);
                        }
                    }
                }
            }

            boolean isAvailable = isVersionHighEnough && lastResult != null && lastResult.isSuccess();

            this.dockerAvailability = new DockerAvailability(
                isAvailable,
                dockerComposeAvailability,
                isVersionHighEnough,
                dockerPath,
                version,
                lastResult
            );
        }

        return this.dockerAvailability;
    }

    /**
     * Given a list of tasks that requires Docker, check whether Docker is available, otherwise throw an exception.
     *
     * @throws GradleException if Docker is not available. The exception message gives the reason.
     */
    void failIfDockerUnavailable(List<String> tasks) {
        DockerAvailability availability = getDockerAvailability();

        // Docker installation is available and compatible
        if (availability.isAvailable) {
            return;
        }

        // No Docker binary was located
        if (availability.path == null) {
            final String message = String.format(
                Locale.ROOT,
                "Docker (checked [%s]) is required to run the following task%s: \n%s",
                String.join(", ", DOCKER_BINARIES),
                tasks.size() > 1 ? "s" : "",
                String.join("\n", tasks)
            );
            throwDockerRequiredException(message);
        }

        // Docker binaries were located, but did not meet the minimum version requirement
        if (availability.lastCommand.isSuccess() && availability.isVersionHighEnough == false) {
            final String message = String.format(
                Locale.ROOT,
                "building Docker images requires minimum Docker version of %s due to use of multi-stage builds yet was [%s]",
                MINIMUM_DOCKER_VERSION,
                availability.version
            );
            throwDockerRequiredException(message);
        }

        // Some other problem, print the error
        final String message = String.format(
            Locale.ROOT,
            "a problem occurred while using Docker from [%s]%s yet it is required to run the following task%s: \n%s\n"
                + "the problem is that Docker exited with exit code [%d] with standard error output:\n%s",
            availability.path,
            availability.version == null ? "" : " v" + availability.version,
            tasks.size() > 1 ? "s" : "",
            String.join("\n", tasks),
            availability.lastCommand.exitCode,
            availability.lastCommand.stderr.trim()
        );
        throwDockerRequiredException(message);
    }

    private boolean isExcludedOs() {
        // We don't attempt to check the current flavor and version of Linux unless we're
        // running in CI, because we don't want to stop people running the Docker tests in
        // their own environments if they really want to.
        if (BuildParams.isCi() == false) {
            return false;
        }

        // Only some hosts in CI are configured with Docker. We attempt to work out the OS
        // and version, so that we know whether to expect to find Docker. We don't attempt
        // to probe for whether Docker is available, because that doesn't tell us whether
        // Docker is unavailable when it should be.
        final Path osRelease = Paths.get("/etc/os-release");

        if (Files.exists(osRelease)) {
            Map<String, String> values;

            try {
                final List<String> osReleaseLines = Files.readAllLines(osRelease);
                values = parseOsRelease(osReleaseLines);
            } catch (IOException e) {
                throw new GradleException("Failed to read /etc/os-release", e);
            }

            final String id = deriveId(values);
            final boolean excluded = getLinuxExclusionList().contains(id);

            if (excluded) {
                LOGGER.warn("Linux OS id [{}] is present in the Docker exclude list. Tasks requiring Docker will be disabled.", id);
            }

            return excluded;
        }

        return false;
    }

    private List<String> getLinuxExclusionList() {
        File exclusionsFile = getParameters().getExclusionsFile();

        if (exclusionsFile.exists()) {
            try {
                return Files.readAllLines(exclusionsFile.toPath())
                    .stream()
                    .map(String::trim)
                    .filter(line -> (line.isEmpty() || line.startsWith("#")) == false)
                    .collect(Collectors.toList());
            } catch (IOException e) {
                throw new GradleException("Failed to read " + exclusionsFile.getAbsolutePath(), e);
            }
        } else {
            return Collections.emptyList();
        }
    }

    // visible for testing
    static String deriveId(Map<String, String> values) {
        return values.get("ID") + "-" + values.get("VERSION_ID");
    }

    // visible for testing
    static Map<String, String> parseOsRelease(final List<String> osReleaseLines) {
        final Map<String, String> values = new HashMap<>();

        osReleaseLines.stream().map(String::trim).filter(line -> (line.isEmpty() || line.startsWith("#")) == false).forEach(line -> {
            final String[] parts = line.split("=", 2);
            final String key = parts[0];
            // remove optional leading and trailing quotes and whitespace
            final String value = parts[1].replaceAll("^['\"]?\\s*", "").replaceAll("\\s*['\"]?$", "");

            values.put(key, value.toLowerCase());
        });

        return values;
    }

    /**
     * Searches the entries in {@link #DOCKER_BINARIES} for the Docker CLI. This method does
     * not check whether the Docker installation appears usable, see {@link #getDockerAvailability()}
     * instead.
     *
     * @return the path to a CLI, if available.
     */
    private Optional<String> getDockerPath() {
        // Check if the Docker binary exists
        return Arrays.asList(DOCKER_BINARIES).stream().filter(path -> new File(path).exists()).findFirst();
    }

    private void throwDockerRequiredException(final String message) {
        throwDockerRequiredException(message, null);
    }

    private void throwDockerRequiredException(final String message, Exception e) {
        throw new GradleException(
            message + "\nyou can address this by attending to the reported issue, or removing the offending tasks from being executed.",
            e
        );
    }

    /**
     * Runs a command and captures the exit code, standard output and standard error.
     *
     * @param args the command and any arguments to execute
     * @return a object that captures the result of running the command. If an exception occurring
     * while running the command, or the process was killed after reaching the 10s timeout,
     * then the exit code will be -1.
     */
    private static Result runCommand(ExecOperations execOperations, String... args) {
        if (args.length == 0) {
            throw new IllegalArgumentException("Cannot execute with no command");
        }

        ByteArrayOutputStream stdout = new ByteArrayOutputStream();
        ByteArrayOutputStream stderr = new ByteArrayOutputStream();

        final ExecResult execResult = execOperations.exec(spec -> {
            // The redundant cast is to silence a compiler warning.
            spec.setCommandLine((Object[]) args);
            spec.setStandardOutput(stdout);
            spec.setErrorOutput(stderr);
            spec.setIgnoreExitValue(true);
        });
        return new Result(execResult.getExitValue(), stdout.toString(), stderr.toString());
    }

    /**
     * An immutable class that represents the results of a Docker search from {@link #getDockerAvailability()}}.
     */
    public static class DockerAvailability {
        /**
         * Indicates whether Docker is available and meets the required criteria.
         * True if, and only if, Docker is:
         * <ul>
         *     <li>Installed</li>
         *     <li>Executable</li>
         *     <li>Is at least version compatibile with minimum version</li>
         *     <li>Can execute a command that requires privileges</li>
         * </ul>
         */
        public final boolean isAvailable;

        /**
         * Non-null if docker-compose v1 or v2 is available.
         */
        public final DockerComposeAvailability dockerComposeAvailability;

        /**
         * True if the installed Docker version is &gt;= 17.05
         */
        public final boolean isVersionHighEnough;

        /**
         * The path to the Docker CLI, or null
         */
        public final String path;

        /**
         * The installed Docker version, or null
         */
        public final Version version;

        /**
         * Information about the last command executes while probing Docker, or null.
         */
        final Result lastCommand;

        DockerAvailability(
            boolean isAvailable,
            DockerComposeAvailability dockerComposeAvailability,
            boolean isVersionHighEnough,
            String path,
            Version version,
            Result lastCommand
        ) {
            this.isAvailable = isAvailable;
            this.dockerComposeAvailability = dockerComposeAvailability;
            this.isVersionHighEnough = isVersionHighEnough;
            this.path = path;
            this.version = version;
            this.lastCommand = lastCommand;
        }

        public boolean isDockerComposeAvailable() {
            return dockerComposeAvailability != null;
        }
    }

    /**
     * Marker interface for Docker Compose availability
     */
    private interface DockerComposeAvailability {
        /**
         * Detects Docker Compose V1/V2 availability
         */
        private static Optional<DockerComposeAvailability> detect(ExecOperations execOperations, String dockerPath) {
            Optional<String> composePath = getDockerComposePath();
            if (composePath.isPresent()) {
                if (runCommand(execOperations, composePath.get(), "version").isSuccess()) {
                    return Optional.of(new DockerComposeV1Availability());
                }
            }

            if (runCommand(execOperations, dockerPath, "compose", "version").isSuccess()) {
                return Optional.of(new DockerComposeV2Availability());
            }

            return Optional.empty();
        }

        /**
         * Searches the entries in {@link #DOCKER_COMPOSE_BINARIES} for the Docker Compose CLI. This method does
         * not check whether the installation appears usable, see {@link #getDockerAvailability()} instead.
         *
         * @return the path to a CLI, if available.
         */
        private static Optional<String> getDockerComposePath() {
            // Check if the Docker binary exists
            return Arrays.asList(DOCKER_COMPOSE_BINARIES).stream().filter(path -> new File(path).exists()).findFirst();
        }

    }

    /**
     * Docker Compose V1 availability
     */
    public static class DockerComposeV1Availability implements DockerComposeAvailability {}

    /**
     * Docker Compose V2 availability
     */
    public static class DockerComposeV2Availability implements DockerComposeAvailability {}

    /**
     * This class models the result of running a command. It captures the exit code, standard output and standard error.
     */
    private static class Result {
        final int exitCode;
        final String stdout;
        final String stderr;

        Result(int exitCode, String stdout, String stderr) {
            this.exitCode = exitCode;
            this.stdout = stdout;
            this.stderr = stderr;
        }

        boolean isSuccess() {
            return exitCode == 0;
        }

        public String toString() {
            return "exitCode = [" + exitCode + "] " + "stdout = [" + stdout.trim() + "] " + "stderr = [" + stderr.trim() + "]";
        }
    }

    interface Parameters extends BuildServiceParameters {
        File getExclusionsFile();

        void setExclusionsFile(File exclusionsFile);
    }
}
