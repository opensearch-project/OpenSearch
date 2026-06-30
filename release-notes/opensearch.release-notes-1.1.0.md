## Version 1.1.0 Release Notes

* __Changes to support retrieval of operations from translog based on specified range (#1257)__

  [Sai](mailto:karanas@amazon.com) - Sun, 26 Sep 2021 00:02:29 -0500

  Backport changes to support retrieval of operations from translog based on
  specified range
  Signed-off-by: Sai Kumar &lt;karanas@amazon.com&gt;

* __[Backport] Support for translog pruning based on retention leases (#1038) (#1256)__

  [Sai](mailto:karanas@amazon.com) - Sat, 25 Sep 2021 23:53:32 -0500


    Support for translog pruning based on retention leases; including deprecations
    for
    refactoring to ccr plugin in the future.
     Co-authored-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;
    Signed-off-by: Sai
    Kumar &lt;karanas@amazon.com&gt;

* __fix gradle check fail due to renameing -min in #1094 (#1289) (#1291)__

  [Xue Zhou](mailto:85715413+xuezhou25@users.noreply.github.com) - Fri, 24 Sep 2021 23:03:45 -0500


    Signed-off-by: Xue Zhou &lt;xuezhou@amazon.com&gt;

* __Rename artifact produced by the build to include -min (#1251) (#1271)__

  [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 22 Sep 2021 10:52:24 -0400


    Signed-off-by: Xue Zhou &lt;xuezhou@amazon.com&gt;
     Co-authored-by: Xue Zhou &lt;85715413+xuezhou25@users.noreply.github.com&gt;

* __[Bug] Fix InstallPluginCommand to use proper key signatures (#1233) (#1235)__

  [Nick Knize](mailto:nknize@apache.org) - Mon, 13 Sep 2021 11:55:24 -0700

  efs/remotes/origin/1.1
  The public key has changed since the initial release. This commit fixes the

  public key and uses the .sig files that are published to the artifacts site.
  Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Fix org.opensearch.index.reindex.ReindexRestClientSslTests#testClientSucceedsWithCertificateAuthorities - javax.net.ssl.SSLPeerUnverifiedException (#1212) (#1224)__

  [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 10 Sep 2021 11:49:33 -0400


    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Max scroll limit breach to throw a OpenSearchRejectedExecutionException (#1054) (#1231)__

  [Rabi Panda](mailto:adnapibar@gmail.com) - Fri, 10 Sep 2021 10:34:46 -0400


    * Changes the Exception to throw a OpenSearchRejectedExecutionException on max
    scroll limit breach
     Signed-off-by: Bukhtawar Khan bukhtawa@amazon.com
     Co-authored-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

* __[1.x] Backport opensearch-upgrade CLI tool (#1222)__

  [Rabi Panda](mailto:adnapibar@gmail.com) - Wed, 8 Sep 2021 10:39:02 -0700


    * A CLI tool to assist during an upgrade to OpenSearch. (#846)
     This change adds the initial version of a new CLI tool `opensearch-upgrade` as
    part of the OpenSearch distribution. This tool is meant for assisting during an
    upgrade from an existing Elasticsearch v7.10.2/v6.8.0 node to OpenSearch. It
    automates the process of importing existing configurations and installing of
    core plugins.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

    * Validation for official plugins for upgrade tool (#973)
     Add validation to check for official plugins during the plugins installation
    task for the upgrade tool.
     Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
     Co-authored-by: Vacha &lt;vachshah@amazon.com&gt;

* __Kept the original constructor for PluginInfo to maintain bwc (#1206) (#1209)__

    [Vacha](mailto:vachshah@amazon.com) - Thu, 2 Sep 2021 22:05:44 -0400

    efs/remotes/upstream/1.x, refs/remotes/origin/1.x, refs/heads/1.x
    Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

* __Clarify JDK requirement in the developer guide (#1153) (#1208)__

    [Tianli Feng](mailto:ftianli@amazon.com) - Thu, 2 Sep 2021 17:02:59 -0700


    * Explicitly point out the JDK 8 requirement is for runtime, but not for
    compiling.
    * Clarify the JAVAx_HOME env variables are for the &#34;backwards compatibility
    test&#34;.
    * Add explanation on how the backwards compatibility tests get the OpenSearch
    distributions for a specific version.
    Signed-off-by: Tianli Feng
    &lt;ftianli@amazon.com&gt;

* __Upgrade apache commons-compress to 1.21 (#1197) (#1203)__

    [Abbas Hussain](mailto:abbashus@amazon.com) - Fri, 3 Sep 2021 01:47:57 +0530


    Signed-off-by: Abbas Hussain &lt;abbas_10690@yahoo.com&gt;

* __Restoring alpha/beta/rc version semantics (#1112) (#1204)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Thu, 2 Sep 2021 08:01:46 -0500


    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __[Bug] Fix mixed cluster support for OpenSearch 2+ (#1191) (#1195)__

    [Nick Knize](mailto:nknize@apache.org) - Wed, 1 Sep 2021 17:04:40 -0500


    The version framework only added support for OpenSearch 1.x bwc with legacy

    clusters. This commit adds support for v2.0 which will be the last version with
     bwc support for legacy clusters (v7.10)
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Drop mocksocket &amp; securemock dependencies from sniffer and rest client (no needed) (#1174) (#1187)__

    [Andriy Redko](mailto:drreta@gmail.com) - Tue, 31 Aug 2021 19:44:42 -0400


    * Drop mocksocket &amp; securemock dependencies from sniffer and rest client (not
    needed)
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Removing .gitignore
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Backporting the fix to 1.x for fixing Copyright licensing (#1188)__

    [Minal Shah](mailto:87717056+minalsha@users.noreply.github.com) - Tue, 31 Aug 2021 19:38:56 -0400


    Signed-off-by: Minal Shah &lt;minalsha@amazon.com&gt;

* __Reduce iterations to improve test run time (#1168) (#1177)__

    [Abbas Hussain](mailto:abbashus@amazon.com) - Tue, 31 Aug 2021 01:06:29 +0530


    Signed-off-by: Abbas Hussain &lt;abbas_10690@yahoo.com&gt;

* __Tune datanode count  and shards count to improve test run time (#1170) (#1176)__

    [Abbas Hussain](mailto:abbashus@amazon.com) - Tue, 31 Aug 2021 00:14:38 +0530


    Signed-off-by: Abbas Hussain &lt;abbas_10690@yahoo.com&gt;

* __Add 1.0.1 revision (#1152) (#1160)__

    [Nick Knize](mailto:nknize@gmail.com) - Thu, 26 Aug 2021 07:10:24 -0500


    This commit stages the branch to the next 1.0.1 patch release. BWC testing
    needs
    this even if the next revision is never actually released.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __[Bug] Change 1.0.0 version check in PluginInfo (#1159)__

    [Nick Knize](mailto:nknize@gmail.com) - Wed, 25 Aug 2021 23:58:30 -0500


    PluginInfo should use .onOrAfter(Version.V_1_1_0) instead of
    .after(Version.V_1_0_0) for the new custom folder name for plugin feature.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Include sources and javadoc artifacts while publishing to a Maven repository (#1049) (#1139)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Mon, 23 Aug 2021 17:12:20 -0700


    This change fixes the issue where the sources and javadoc artifacts were not
    built and included with the publish.
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Allowing custom folder name for plugin installation (#848) (#1116)__

    [Vacha](mailto:vachshah@amazon.com) - Mon, 23 Aug 2021 14:39:27 -0700


    Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;

* __Upgrade to Lucene 8.9 (#1080) (#1115)__

    [Nick Knize](mailto:nknize@gmail.com) - Mon, 23 Aug 2021 10:06:34 -0700


    This commit upgrades to the official lucene 8.9 release
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __[DEPRECATE] SimpleFS in favor of NIOFS (#1073) (#1114)__

    [Nick Knize](mailto:nknize@gmail.com) - Fri, 20 Aug 2021 11:26:22 -0500


    Lucene 9 removes support for SimpleFS File System format. This commit
    deprecates
    the SimpleFS format in favor of NIOFS.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

* __Fix failure in SearchCancellationIT.testMSearchChildReqCancellationWithHybridTimeout (#1105)__

    [Sorabh](mailto:sohami.apache@gmail.com) - Tue, 17 Aug 2021 16:23:55 -0400


    In some cases as one shared with issue #1099, the maxConcurrentSearchRequests
    was chosen as 0 which
    will compute the final value during execution of the
    request based on processor counts. When this
    computed value is less than
    number of search request in msearch request, it will execute all the
    requests
    in multiple iterations causing the failure since test will only wait for one
    such
    iteration. Hence setting the maxConcurrentSearchRequests explicitly to
    number of search requests
    being added in the test to ensure correct behavior
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

* __Support for bwc tests for plugins (#1051) (#1090)__

    [Vacha](mailto:vachshah@amazon.com) - Sun, 15 Aug 2021 08:07:55 -0700


    * Support for bwc tests for plugins
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

    * Adding support for restart upgrades for plugins bwc
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

* __Improving the Grok circular reference check to prevent stack overflow (#1079) (#1087)__

    [kartg](mailto:85275476+kartg@users.noreply.github.com) - Thu, 12 Aug 2021 18:47:56 -0400


    This change refactors the circular reference check in the Grok processor class

    to use a formal depth-first traversal. It also includes a logic update to

    prevent a stack overflow in one scenario and a check for malformed patterns.

    This bugfix addresses CVE-2021-22144.
     Signed-off-by: Kartik Ganesh &lt;85275476+kartg@users.noreply.github.com&gt;

* __Part 1: Support for cancel_after_timeinterval parameter in search and msearch request (#986) (#1085)__

    [Sorabh](mailto:sorabh@apache.org) - Thu, 12 Aug 2021 13:52:28 -0400


    * Part 1: Support for cancel_after_timeinterval parameter in search and msearch
    request
     This commit introduces the new request level parameter to configure the
    timeout interval after which
    a search request will be cancelled. For msearch
    request the parameter is supported both at parent
    request and at sub child
    search requests. If it is provided at parent level and child search request

    doesn&#39;t have it then the parent level value is set at such child request. The
    parent level msearch
    is not used to cancel the parent request as it may be
    tricky to come up with correct value in cases
    when child search request can
    have different runtimes
     TEST: Added test for ser/de with new parameter
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Part 2: Support for cancel_after_timeinterval parameter in search and msearch
    request
     This commit adds the handling of the new request level parameter and schedule
    cancellation task. It
    also adds a cluster setting to set a global cancellation
    timeout for search request which will be
    used in absence of request level
    timeout.
     TEST: Added new tests in SearchCancellationIT
    Signed-off-by: Sorabh
    Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Address Review feedback for Part 1
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Address review feedback for Part 2
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Update CancellableTask to remove the cancelOnTimeout boolean flag
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Replace search.cancellation.timeout cluster setting with
    search.enforce_server.timeout.cancellation to control if cluster level
    cancel_after_time_interval should take precedence over request level
    cancel_after_time_interval value
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;

    * Removing the search.enforce_server.timeout.cancellation cluster setting and
    just keeping search.cancel_after_time_interval setting with request level
    parameter taking the precedence.
     Signed-off-by: Sorabh Hamirwasia &lt;sohami.apache@gmail.com&gt;
     Co-authored-by: Sorabh Hamirwasia &lt;hsorabh@amazon.com&gt;
     Co-authored-by: Sorabh Hamirwasia &lt;hsorabh@amazon.com&gt;

* __Avoid crashing on using the index.lifecycle.name in the API body (#1060) (#1070)__

    [frotsch](mailto:86320880+frotsch@users.noreply.github.com) - Tue, 10 Aug 2021 14:16:44 -0400


    * Avoid crashing on using the index.lifecycle.name in the API body
     Signed-off-by: frotsch &lt;frotsch@mailbox.org&gt;

* __Introduce RestHandler.Wrapper to help with delegate implementations (#1004) (#1031)__

    [Vlad Rozov](mailto:vrozov@users.noreply.github.com) - Tue, 3 Aug 2021 09:02:40 -0400


    Signed-off-by: Vlad Rozov &lt;vrozov@users.noreply.github.com&gt;

* __Rank feature - unknown field linear (#983) (#1025)__

    [Yevhen Tienkaiev](mailto:hronom@gmail.com) - Fri, 30 Jul 2021 15:17:47 -0400


    Signed-off-by: Yevhen Tienkaiev &lt;hronom@gmail.com&gt;

* __Replace Elasticsearch docs links in scripts (#994) (#1001)__

    [Poojita Raj](mailto:poojiraj@amazon.com) - Fri, 23 Jul 2021 14:21:31 -0700


    Replace the docs links In scripts bin/opensearch-env and config/jvm.options,
    with OpenSearch docs links.
     Signed-off-by: Poojita-Raj &lt;poojiraj@amazon.com&gt;
    (cherry picked from commit 6bc4ce017ad654cc2c8d7d37553c82d61c61b964)

    Signed-off-by: Poojita-Raj &lt;poojiraj@amazon.com&gt;

* __Introduce replaceRoutes() method and 2 new constructors to RestHandler.java (#947) (#998)__

    [Chang Liu](mailto:lc12251109@gmail.com) - Thu, 22 Jul 2021 14:26:16 -0400


    * Add addRoutesPrefix() method to RestHandler.java
     Signed-off-by: Azar Fazel &lt;azar.fazel@gmail.com&gt;
    Signed-off-by: cliu123
    &lt;lc12251109@gmail.com&gt;
     Co-authored-by: afazel &lt;afazel@users.noreply.github.com&gt;

* __Avoid override of routes() in BaseRestHandler to respect the default behavior defined in RestHandler (#889) (#991)__

    [Chang Liu](mailto:lc12251109@gmail.com) - Thu, 22 Jul 2021 10:57:18 -0400


    Signed-off-by: cliu123 &lt;lc12251109@gmail.com&gt;

* __Cleanup TESTING and DEVELOPER_GUIDE markdowns (#946) (#954)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Tue, 13 Jul 2021 14:13:26 -0500




* __Updated READMEs on releasing, maintaining, admins and security. (#853) (#950)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Mon, 12 Jul 2021 15:06:20 -0500


    Signed-off-by: dblock &lt;dblock@amazon.com&gt;

* __Pass interceptor to super constructor (#876) (#937)__

    [Sooraj Sinha](mailto:81695996+soosinha@users.noreply.github.com) - Mon, 12 Jul 2021 11:48:09 -0700


    Signed-off-by: Sooraj Sinha &lt;soosinha@amazon.com&gt;
