package org.opensearch.extensions;

import java.io.IOException;
import java.util.Objects;

import org.opensearch.Version;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;

<<<<<<< HEAD
public class ExtensionDependency implements Writeable {
    public String uniqueId;
    public Version version;

    public ExtensionDependency(String uniqueId, Version version){
=======
public class ExtensionDenpendency implements Writeable {
    private String uniqueId;
    private String version;

    public ExtensionDenpendency(String uniqueId, String version){
>>>>>>> 053b62670fa (add option field)
        this.uniqueId = uniqueId;
        this.version = version;
    }

    public ExtensionDependency(StreamInput in) throws IOException {
        uniqueId = in.readString();
        version = in.readString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(uniqueId);
        Version.fromString(getVersion());
    }

    public String getUniqueId(){
        return uniqueId;
    }

    public String getVersion(){
        return version;
    }
    
    public String toString() {
        return "ExtensionDependency:{uniqueId=" + uniqueId + ", version=" + version + "}";
    }

    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (obj == null || getClass() != obj.getClass()) return false;
        ExtensionDependency that = (ExtensionDependency) obj;
        return Objects.equals(uniqueId, that.uniqueId) && Objects.equals(version, that.version);
    }

    public int hashCode() {
        return Objects.hash(uniqueId, version);
    }
}
