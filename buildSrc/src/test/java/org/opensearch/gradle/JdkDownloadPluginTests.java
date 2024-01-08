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

import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;
import org.junit.BeforeClass;

import java.util.UUID;

import static org.hamcrest.CoreMatchers.equalTo;

public class JdkDownloadPluginTests extends GradleUnitTestCase {
    private static Project rootProject;

    @BeforeClass
    public static void setupRoot() {
        rootProject = ProjectBuilder.builder().build();
    }

    public void testMissingVendor() {
        assertJdkError(createProject(), "testjdk", null, "11.0.2+33", "linux", "x64", "vendor not specified for jdk [testjdk]");
    }

    public void testUnknownVendor() {
        assertJdkError(
            createProject(),
            "testjdk",
            "unknown",
            "11.0.2+33",
            "linux",
            "x64",
            "unknown vendor [unknown] for jdk [testjdk], must be one of [adoptium, adoptopenjdk, openjdk]"
        );
    }

    public void testMissingVersion() {
        assertJdkError(createProject(), "testjdk", "openjdk", null, "linux", "x64", "version not specified for jdk [testjdk]");
    }

    public void testBadVersionFormat() {
        assertJdkError(
            createProject(),
            "testjdk",
            "openjdk",
            "badversion",
            "linux",
            "x64",
            "malformed version [badversion] for jdk [testjdk]"
        );
    }

    public void testMissingPlatform() {
        assertJdkError(createProject(), "testjdk", "openjdk", "11.0.2+33", null, "x64", "platform not specified for jdk [testjdk]");
    }

    public void testUnknownPlatform() {
        assertJdkError(
            createProject(),
            "testjdk",
            "openjdk",
            "11.0.2+33",
            "unknown",
            "x64",
            "unknown platform [unknown] for jdk [testjdk], must be one of [darwin, freebsd, linux, mac, windows]"
        );
    }

    public void testMissingArchitecture() {
        assertJdkError(createProject(), "testjdk", "openjdk", "11.0.2+33", "linux", null, "architecture not specified for jdk [testjdk]");
    }

    public void testUnknownArchitecture() {
        assertJdkError(
            createProject(),
            "testjdk",
            "openjdk",
            "11.0.2+33",
            "linux",
            "unknown",
            "unknown architecture [unknown] for jdk [testjdk], must be one of [aarch64, x64, s390x, ppc64le]"
        );
    }

    private void assertJdkError(
        final Project project,
        final String name,
        final String vendor,
        final String version,
        final String platform,
        final String architecture,
        final String message
    ) {
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> createJdk(project, name, vendor, version, platform, architecture)
        );
        assertThat(e.getMessage(), equalTo(message));
    }

    private void createJdk(Project project, String name, String vendor, String version, String platform, String architecture) {
        @SuppressWarnings("unchecked")
        NamedDomainObjectContainer<Jdk> jdks = (NamedDomainObjectContainer<Jdk>) project.getExtensions().getByName("jdks");
        jdks.create(name, jdk -> {
            if (vendor != null) {
                jdk.setVendor(vendor);
            }
            if (version != null) {
                jdk.setVersion(version);
            }
            if (platform != null) {
                jdk.setPlatform(platform);
            }
            if (architecture != null) {
                jdk.setArchitecture(architecture);
            }
        }).finalizeValues();
    }

    private Project createProject() {
        final String name = UUID.randomUUID().toString();
        Project project = ProjectBuilder.builder().withName(name).withParent(rootProject).build();
        project.getPlugins().apply("opensearch.jdk-download");
        return project;
    }
}
