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

package org.opensearch.gradle;

import org.opensearch.gradle.docker.DockerSupportService;
import org.gradle.api.Buildable;
import org.gradle.api.artifacts.Configuration;
import org.gradle.api.model.ObjectFactory;
import org.gradle.api.provider.Property;
import org.gradle.api.provider.Provider;
import org.gradle.api.tasks.TaskDependency;

import java.io.File;
import java.util.Collections;
import java.util.Iterator;
import java.util.Locale;

public class OpenSearchDistribution implements Buildable, Iterable<File> {

    public enum Platform {
        DARWIN,
        FREEBSD,
        LINUX,
        WINDOWS;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }
    }

    public enum Type {
        INTEG_TEST_ZIP,
        ARCHIVE,
        RPM,
        DEB,
        DOCKER;

        @Override
        public String toString() {
            return super.toString().toLowerCase(Locale.ROOT);
        }

        public boolean shouldExtract() {
            switch (this) {
                case DEB:
                case DOCKER:
                case RPM:
                    return false;

                default:
                    return true;
            }
        }
    }

    // package private to tests can use
    public static final Platform CURRENT_PLATFORM = OS.<Platform>conditional()
        .onFreeBSD(() -> Platform.FREEBSD)
        .onLinux(() -> Platform.LINUX)
        .onMac(() -> Platform.DARWIN)
        .onWindows(() -> Platform.WINDOWS)
        .supply();

    private final String name;
    private final Provider<DockerSupportService> dockerSupport;
    // pkg private so plugin can configure
    final Configuration configuration;

    private final Property<Architecture> architecture;
    private final Property<String> version;
    private final Property<Type> type;
    private final Property<Platform> platform;
    private final Property<JavaPackageType> bundledJdk;
    private final Property<Boolean> failIfUnavailable;
    private final Configuration extracted;

    OpenSearchDistribution(
        String name,
        ObjectFactory objectFactory,
        Provider<DockerSupportService> dockerSupport,
        Configuration fileConfiguration,
        Configuration extractedConfiguration
    ) {
        this.name = name;
        this.dockerSupport = dockerSupport;
        this.configuration = fileConfiguration;
        this.architecture = objectFactory.property(Architecture.class);
        this.version = objectFactory.property(String.class).convention(VersionProperties.getOpenSearch());
        this.type = objectFactory.property(Type.class);
        this.type.convention(Type.ARCHIVE);
        this.platform = objectFactory.property(Platform.class);
        this.bundledJdk = objectFactory.property(JavaPackageType.class);
        this.failIfUnavailable = objectFactory.property(Boolean.class).convention(true);
        this.extracted = extractedConfiguration;
    }

    public String getName() {
        return name;
    }

    public String getVersion() {
        return version.get();
    }

    public void setVersion(String version) {
        Version.fromString(version); // ensure the version parses, but don't store as Version since that removes -SNAPSHOT
        this.version.set(version);
    }

    public Platform getPlatform() {
        return platform.getOrNull();
    }

    public void setPlatform(Platform platform) {
        this.platform.set(platform);
    }

    public Type getType() {
        return type.get();
    }

    public void setType(Type type) {
        this.type.set(type);
    }

    public JavaPackageType getBundledJdk() {
        return bundledJdk.getOrElse(JavaPackageType.JDK);
    }

    public boolean isDocker() {
        final Type type = this.type.get();
        return type == Type.DOCKER;
    }

    public void setBundledJdk(JavaPackageType bundledJdk) {
        this.bundledJdk.set(bundledJdk);
    }

    public boolean getFailIfUnavailable() {
        return this.failIfUnavailable.get();
    }

    public void setFailIfUnavailable(boolean failIfUnavailable) {
        this.failIfUnavailable.set(failIfUnavailable);
    }

    public void setArchitecture(Architecture architecture) {
        this.architecture.set(architecture);
    }

    public Architecture getArchitecture() {
        return this.architecture.get();
    }

    @Override
    public String toString() {
        return getName() + "_" + getType() + "_" + getVersion();
    }

    public String getFilepath() {
        return configuration.getSingleFile().toString();
    }

    public Configuration getExtracted() {
        switch (getType()) {
            case DEB:
            case DOCKER:
            case RPM:
                throw new UnsupportedOperationException(
                    "distribution type [" + getType() + "] for " + "opensearch distribution [" + name + "] cannot be extracted"
                );

            default:
                return extracted;
        }
    }

    @Override
    public TaskDependency getBuildDependencies() {
        // For non-required Docker distributions, skip building the distribution is Docker is unavailable
        if (isDocker() && getFailIfUnavailable() == false && dockerSupport.get().getDockerAvailability().isAvailable == false) {
            return task -> Collections.emptySet();
        }

        return configuration.getBuildDependencies();
    }

    @Override
    public Iterator<File> iterator() {
        return configuration.iterator();
    }

    // internal, make this distribution's configuration unmodifiable
    void finalizeValues() {

        if (getType() == Type.INTEG_TEST_ZIP) {
            if (platform.getOrNull() != null) {
                throw new IllegalArgumentException(
                    "platform cannot be set on opensearch distribution [" + name + "] of type [integ_test_zip]"
                );
            }

            if (bundledJdk.getOrNull() != null) {
                throw new IllegalArgumentException(
                    "bundledJdk cannot be set on opensearch distribution [" + name + "] of type [integ_test_zip]"
                );
            }
            return;
        }

        if (isDocker() == false && failIfUnavailable.get() == false) {
            throw new IllegalArgumentException(
                "failIfUnavailable cannot be 'false' on opensearch distribution [" + name + "] of type [" + getType() + "]"
            );
        }

        if (getType() == Type.ARCHIVE) {
            // defaults for archive, set here instead of via convention so integ-test-zip can verify they are not set
            if (platform.isPresent() == false) {
                platform.set(CURRENT_PLATFORM);
            }
        } else { // rpm, deb or docker
            if (platform.isPresent()) {
                throw new IllegalArgumentException(
                    "platform cannot be set on opensearch distribution [" + name + "] of type [" + getType() + "]"
                );
            }
            if (isDocker()) {
                if (bundledJdk.isPresent()) {
                    throw new IllegalArgumentException(
                        "bundledJdk cannot be set on opensearch distribution [" + name + "] of type " + "[docker]"
                    );
                }
            }
        }

        if (bundledJdk.isPresent() == false) {
            bundledJdk.set(JavaPackageType.JDK);
        }

        version.finalizeValue();
        platform.finalizeValue();
        type.finalizeValue();
        bundledJdk.finalizeValue();
    }
}
