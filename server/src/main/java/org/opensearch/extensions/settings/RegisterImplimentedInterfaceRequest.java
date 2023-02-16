package org.opensearch.extensions.settings;

import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.extensions.ExtensionsManager;

/**
 * Request to register a list of Implimented Interfaces
 *
 * @opensearch.internal
 */
public class RegisterImplimentedInterfaceRequest extends TransportRequest {
    private String uniqueId;
    private List<ExtensionsManager.ExtensionInterfaceType> implimentedInterfaces;

    public RegisterImplimentedInterfaceRequest(String uniqueId, List<ExtensionsManager.ExtensionInterfaceType> implimentedinterfacesList){
        this.uniqueId = uniqueId;
        this.implimentedInterfaces = implimentedinterfacesList;
    }

    public RegisterImplimentedInterfaceRequest(StreamInput in) throws IOException {
        super(in);
        System.out.print("it came till here ");
        this.uniqueId = in.readString();
        int size = in.readVInt();
        this.implimentedInterfaces = new ArrayList<ExtensionsManager.ExtensionInterfaceType>(size);
        for(int i=0;i<size;i++){
            ExtensionsManager.ExtensionInterfaceType extensionInterfaceType = in.readEnum(ExtensionsManager.ExtensionInterfaceType.class);
            this.implimentedInterfaces.add(extensionInterfaceType);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(uniqueId);
        out.writeVInt(implimentedInterfaces.size());
        for (ExtensionsManager.ExtensionInterfaceType interfaceEnumVal : implimentedInterfaces) {
            out.writeEnum(interfaceEnumVal);
        }
    }

    public String getUniqueId() {
        return uniqueId;
    }

    public List<ExtensionsManager.ExtensionInterfaceType> getImplimentedInterfaces(){
        return implimentedInterfaces;
    }

    @Override
    public String toString() {
        return "RegisterImplimentedInterfacesRequest{uniqueId=" + uniqueId + ", implimentedInterfaces=" + implimentedInterfaces + "}";
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        RegisterImplimentedInterfaceRequest that = (RegisterImplimentedInterfaceRequest) obj;
        return Objects.equals(uniqueId, that.uniqueId) && Objects.equals(implimentedInterfaces, that.implimentedInterfaces);
    }

    @Override
    public int hashCode() {
        return Objects.hash(uniqueId, implimentedInterfaces);
    }

}
