/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.cluster.service.applicationtemplates;

import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.plugins.Plugin;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

public class TestSystemTemplatesRepositoryPlugin extends Plugin implements SystemTemplatesPlugin {

    private SystemTemplateInfo info = SystemTemplateInfo.createComponentTemplateInfo("dummy", 1);

    private TemplateRepositoryInfo repoInfo = new TemplateRepositoryInfo("test", 1);

    private SystemTemplate systemTemplate = new SystemTemplate(BytesReference.fromByteBuffer(ByteBuffer.wrap("content".getBytes(StandardCharsets.UTF_8))), info, repoInfo);

    @Override
    public TemplateRepository loadRepository() throws IOException {
        return new TemplateRepository() {
            @Override
            public TemplateRepositoryInfo info() {
                return repoInfo;
            }

            @Override
            public List<SystemTemplateInfo> listTemplates() throws IOException {
                return List.of(info);
            }

            @Override
            public SystemTemplate fetchTemplate(SystemTemplateInfo template) throws IOException {
                return systemTemplate;
            }

            @Override
            public void close() throws Exception {
            }
        };
    }

    @Override
    public TemplateLoader loaderFor(SystemTemplateInfo templateInfo) {
        return new TemplateLoader() { // Asserting Loader
            @Override
            public void loadTemplate(SystemTemplate template) throws IOException {
                assert template.templateInfo() == info;
                assert template.repositoryInfo() == repoInfo;
                assert template.templateContent() == systemTemplate;
            }
        };
    }
}
