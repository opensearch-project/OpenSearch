package org.opensearch.gradle.zipplugin;

public class ZipPublishExtension {

    private String zipGroup = "org.opensearch.plugin";

    public void setZipgroup(String zipGroup){
        this.zipGroup=zipGroup;
    }
    public String getZipgroup(){
		return zipGroup;
	}

}

