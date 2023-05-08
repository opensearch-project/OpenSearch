/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.info;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;

import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.node.ProtobufReportingService;
import org.opensearch.plugins.ProtobufPluginInfo;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * Information about plugins and modules
*
* @opensearch.internal
*/
public class ProtobufPluginsAndModules implements ProtobufReportingService.ProtobufInfo {
    private final List<ProtobufPluginInfo> plugins;
    private final List<ProtobufPluginInfo> modules;

    public ProtobufPluginsAndModules(List<ProtobufPluginInfo> plugins, List<ProtobufPluginInfo> modules) {
        this.plugins = Collections.unmodifiableList(plugins);
        this.modules = Collections.unmodifiableList(modules);
    }

    public ProtobufPluginsAndModules(CodedInputStream in) throws IOException {
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput();
        this.plugins = Collections.unmodifiableList(protobufStreamInput.readList(ProtobufPluginInfo::new, in));
        this.modules = Collections.unmodifiableList(protobufStreamInput.readList(ProtobufPluginInfo::new, in));
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput();
        protobufStreamOutput.writeCollection(plugins, (o, v) -> v.writeTo(o), out);
        protobufStreamOutput.writeCollection(modules, (o, v) -> v.writeTo(o), out);
    }

    /**
     * Returns an ordered list based on plugins name
    */
    public List<ProtobufPluginInfo> getPluginInfos() {
        List<ProtobufPluginInfo> plugins = new ArrayList<>(this.plugins);
        Collections.sort(plugins, Comparator.comparing(ProtobufPluginInfo::getName));
        return plugins;
    }

    /**
     * Returns an ordered list based on modules name
    */
    public List<ProtobufPluginInfo> getModuleInfos() {
        List<ProtobufPluginInfo> modules = new ArrayList<>(this.modules);
        Collections.sort(modules, Comparator.comparing(ProtobufPluginInfo::getName));
        return modules;
    }

    public void addPlugin(ProtobufPluginInfo info) {
        plugins.add(info);
    }

    public void addModule(ProtobufPluginInfo info) {
        modules.add(info);
    }
}
