/*
 * SPDX-License-Identifier: Apache-2.0
 */
package org.opensearch.tools.cli.heapprof;

import joptsimple.OptionSet;
import joptsimple.OptionSpec;
import org.opensearch.cli.Terminal;
import org.opensearch.cli.UserException;
import javax.management.MBeanServerConnection;
import javax.management.ObjectName;

class ResetCommand extends HeapProfCommand {
    private final OptionSpec<String> lgSampleArg;

    ResetCommand() {
        super("Reset profiling state (discards data) and set sample interval");
        lgSampleArg = parser.nonOptions("lg_prof_sample (log2 of bytes between samples: 15=32KB, 17=128KB, 19=512KB)").ofType(String.class);
    }

    @Override
    protected void invokeOnMBean(MBeanServerConnection mbs, ObjectName mbean, Terminal terminal, OptionSet options) throws Exception {
        var args = lgSampleArg.values(options);
        int lgSample = 17; // default
        if (!args.isEmpty()) {
            lgSample = Integer.parseInt(args.get(0));
        }
        mbs.invoke(mbean, "reset", new Object[]{lgSample}, new String[]{"int"});
        terminal.println("Profiling reset with lg_prof_sample=" + lgSample + " (sample every ~" + ((1L << lgSample) / 1024) + "KB)");
    }
}
