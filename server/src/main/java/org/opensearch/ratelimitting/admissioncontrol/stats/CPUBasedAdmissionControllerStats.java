/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.ratelimitting.admissioncontrol.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CPUBasedAdmissionController;

import java.io.IOException;
import java.util.Map;

import static org.opensearch.ratelimitting.admissioncontrol.settings.CPUBasedAdmissionControllerSettings.CPU_BASED_ADMISSION_CONTROLLER;
public class CPUBasedAdmissionControllerStats extends BaseAdmissionControllerStats {

    /**
     * Returns the name of the writeable object
     */
    @Override
    public String getWriteableName() {
        return CPU_BASED_ADMISSION_CONTROLLER;
    }

    public Map<String, Long> rejectionCount;

    public CPUBasedAdmissionControllerStats(AdmissionController admissionController){
        this.rejectionCount = admissionController.getRejectionStats();
    }

    public CPUBasedAdmissionControllerStats(StreamInput in) throws IOException {
        this.rejectionCount = in.readMap(StreamInput::readString, StreamInput::readLong);
    }
    /**
     * Write this into the {@linkplain StreamOutput}.
     *
     * @param out
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeMap(this.rejectionCount, StreamOutput::writeString, StreamOutput::writeLong);
    }

    /**
     * @param builder
     * @param params
     * @return
     * @throws IOException
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.startObject("transport");
        {
            builder.startObject("rejection_count");
            {
                this.rejectionCount.forEach((actionType, count) -> {
                    try {
                        builder.field(actionType, count);
                    } catch (IOException e) {
                        throw new RuntimeException(e);
                    }
                });
            }
            builder.endObject();
        }
        builder.endObject();
        return builder.endObject();
    }
}
