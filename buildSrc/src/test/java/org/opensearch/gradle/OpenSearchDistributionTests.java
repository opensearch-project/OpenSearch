/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.gradle;

import org.opensearch.gradle.OpenSearchDistribution.Platform;
import org.opensearch.gradle.OpenSearchDistribution.Type;
import org.opensearch.gradle.test.GradleUnitTestCase;
import org.gradle.api.NamedDomainObjectContainer;
import org.gradle.api.Project;
import org.gradle.testfixtures.ProjectBuilder;

public class OpenSearchDistributionTests extends GradleUnitTestCase {

    private Project project;

    private void initProject() {
        project = ProjectBuilder.builder().build();
        project.getPlugins().apply("opensearch.distribution-download");
    }

    private OpenSearchDistribution createDistro(String name, Type type, Platform platform, Architecture arch) {
        if (project == null) {
            initProject();
        }
        NamedDomainObjectContainer<OpenSearchDistribution> distros = DistributionDownloadPlugin.getContainer(project);
        return distros.create(name, distro -> {
            distro.setVersion("3.5.1");
            distro.setType(type);
            if (platform != null) {
                distro.setPlatform(platform);
            }
            distro.setArchitecture(arch);
        });
    }

    public void testClassifierAndExtensionLinuxX64() {
        OpenSearchDistribution distro = createDistro("linux-x64", Type.ARCHIVE, Platform.LINUX, Architecture.X64);
        distro.finalizeValues();
        assertEquals(":linux-x64@tar.gz", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionLinuxArm64() {
        OpenSearchDistribution distro = createDistro("linux-arm64", Type.ARCHIVE, Platform.LINUX, Architecture.ARM64);
        distro.finalizeValues();
        assertEquals(":linux-arm64@tar.gz", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionWindowsX64() {
        OpenSearchDistribution distro = createDistro("windows-x64", Type.ARCHIVE, Platform.WINDOWS, Architecture.X64);
        distro.finalizeValues();
        assertEquals(":windows-x64@zip", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionRpm() {
        OpenSearchDistribution distro = createDistro("rpm", Type.RPM, null, Architecture.X64);
        distro.finalizeValues();
        assertEquals(":x64@rpm", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionDeb() {
        OpenSearchDistribution distro = createDistro("deb", Type.DEB, null, Architecture.X64);
        distro.finalizeValues();
        assertEquals(":amd64@deb", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionLinuxS390x() {
        OpenSearchDistribution distro = createDistro("linux-s390x", Type.ARCHIVE, Platform.LINUX, Architecture.S390X);
        distro.finalizeValues();
        assertEquals(":linux-s390x@tar.gz", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionLinuxPpc64le() {
        OpenSearchDistribution distro = createDistro("linux-ppc64le", Type.ARCHIVE, Platform.LINUX, Architecture.PPC64LE);
        distro.finalizeValues();
        assertEquals(":linux-ppc64le@tar.gz", distro.classifierAndExtension());
    }

    public void testClassifierAndExtensionLinuxRiscv64() {
        OpenSearchDistribution distro = createDistro("linux-riscv64", Type.ARCHIVE, Platform.LINUX, Architecture.RISCV64);
        distro.finalizeValues();
        assertEquals(":linux-riscv64@tar.gz", distro.classifierAndExtension());
    }
}
