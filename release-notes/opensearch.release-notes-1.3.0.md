## Version 1.3.0 Release Notes

* __MapperService has to be passed in as null for EnginePlugins CodecService constructor (#2177) (#2413)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 9 Mar 2022 10:17:33 -0500

    efs/remotes/os_or/1.3
    * MapperService has to be passed in as null for EnginePlugins CodecService
    constructor

    * Addressing code review comments

    * Delayed CodecService instantiation up to the shard initialization

    * Added logger (associated with shard) to CodecServiceConfig

    * Refactored the EngineConfigFactory / IndexShard instantiation of the
    CodecService
    (cherry picked from commit 9c679cbbfcf685e3865d2cf06b8f4e10c3082d49)

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Replace exclusionary words whitelist and blacklist in the places that… (#2365)__

    [aponb](mailto:apre@gmx.at) - Mon, 7 Mar 2022 15:14:36 -0800

  * Replace the exclusionary word whitelist with allowlist, and blacklist with
      denylist, in code commet and internal variable/method/class/package name.

  Signed-off-by: Andreas &lt;apre@gmx.at&gt;


* __Install plugin command help (#2193) (#2264)__

    [Joshua Palis](mailto:jpalis@amazon.com) - Mon, 7 Mar 2022 15:24:56 -0500
  * edited opensearch-plugin install help output to include plugin URL
  * fixed unit test for plugin install help output by correctly identifying the
          beginning og the non-option argument list
  * added comments to install plugins help non option argument ouput unit test
  * fixed format violation
  * added additional details on valid plugin ids and how to use plugin URLs
  * added additional information to plugin install help output
          (cherry picked from commit b251d2b565b918708a1612ec16d1916122c7805d) Signed-off-by: Joshua Palis &lt;jpalis@amazon.com&gt;

  Signed-off-by: Joshua Palis &lt;jpalis@amazon.com&gt;


* __Add valuesField in PercentilesAggregationBuilder streamInput constructor (#2308) (#2389)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 7 Mar 2022 13:07:41 -0500

     Signed-off-by: Subhobrata Dey &lt;sbcd90@gmail.com&gt;
     (cherry picked from commit e1fd4b75b4f888d8d486baceeb9fd6fe7df44416)
      Co-authored-by: Subhobrata Dey &lt;sbcd90@gmail.com&gt;


* __Updated the url for docker distribution (#2325) (#2360)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 7 Mar 2022 11:52:46 -0500

  Signed-off-by: Owais Kazi &lt;owaiskazi19@gmail.com&gt;
    (cherry picked from commit 9224537704bb12980a129afb1e7b6ba6ab93680e)
     Co-authored-by: Owais Kazi &lt;owaiskazi19@gmail.com&gt;


* __Reintroduce negative epoch_millis #1991 (#2232) (#2380)__

    [Breno Faria](mailto:breno.faria@intrafind.com) - Mon, 7 Mar 2022 11:46:54 -0500

    * Reintroduce negative epoch_millis #1991
     Fixes a regression introduced with Elasticsearch 7 regarding the date
    field
    type that removed support for negative timestamps with sub-second
    granularity. Thanks to Ryan Kophs (https://github.com/rkophs) for allowing me to use
    his previous work.
     Signed-off-by: Breno Faria &lt;breno.faria@intrafind.de&gt;

    * applying spotless fix
     Signed-off-by: Breno Faria &lt;breno.faria@intrafind.de&gt;

    * more conservative implementation of isSupportedBy
     Signed-off-by: Breno Faria &lt;breno.faria@intrafind.de&gt;

    * adding braces to control flow statement
     Signed-off-by: Breno Faria &lt;breno.faria@intrafind.de&gt;

    * spotless fix...
     Signed-off-by: Breno Faria &lt;breno.faria@intrafind.de&gt;
     Co-authored-by: Breno Faria &lt;breno.faria@intrafind.de&gt;
     Co-authored-by: Breno Faria &lt;breno.faria@intrafind.de&gt;


* __Add &#39;key&#39; field to &#39;function_score&#39; query function definition in explanation response (#1711) (#2346)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Mon, 7 Mar 2022 11:42:03 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Fix java-version-checker source/target compatibility settings (#2354)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 4 Mar 2022 15:21:54 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Fixing the --release flag usage for javac (#2343)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 4 Mar 2022 13:49:01 -0500

    * Fixing the --release flag usage for javac
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Fixing the --html5 flag usage for javadoc
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Fixing soft deletes deprecation warning (#2339)__

    [Vacha Shah](mailto:vachshah@amazon.com) - Fri, 4 Mar 2022 10:06:11 -0500

    Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;


* __Remove Github DCO action since DCO runs via Github App now (#2317) (#2323)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 12:12:55 -0800

    Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
    (cherry picked from commit cdb42ad3013f67970def21e15c546c9c4fd08d6f)
     Co-authored-by: Vacha Shah &lt;vachshah@amazon.com&gt;


* __[Backport 1.x] Avoid logging duplicate deprecation warnings multiple times (#2315)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 14:23:05 -0500

    * Avoid logging duplicate deprecation warnings multiple times (#1660)

    * Avoid logging duplicate deprecation warnings multiple times
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

    * Fixes test failures
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

    * Adding deprecation logger tests
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

    * Using ConcurrentHashMap keySet
     Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
    (cherry picked from commit e66ea2c4f3ec583f087a82d1ebfb6383b2f159c1)

    * Fixing failing RestResizeHandlerTests in 1.x
     Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
     Co-authored-by: Vacha &lt;vachshah@amazon.com&gt;


* __Restore Java 8 compatibility for build tools. (#2300)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Thu, 3 Mar 2022 14:14:23 -0500

    * Restore Java 8 compatibility for build tools.
     Signed-off-by: dblock &lt;dblock@dblock.org&gt;

    * Make source code compatible with Java 8.
     Signed-off-by: dblock &lt;dblock@dblock.org&gt;

* __Add support of SOCKS proxies for S3 repository (#2160) (#2316)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 3 Mar 2022 13:49:40 -0500

    Signed-off-by: Andrey Pleskach &lt;ples@aiven.io&gt;
    (cherry picked from commit f13b951c7006700a9b8a8bb2cdecd67439bc1e86)
     Co-authored-by: Andrey Pleskach &lt;ples@aiven.io&gt;


* __Fix flaky test case - string profiler via global ordinals (#2226) (#2313)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 2 Mar 2022 17:48:34 -0600

    forcemerge to one segment before executing aggregation query.
     Signed-off-by: Peng Huo &lt;penghuo@gmail.com&gt;
    (cherry picked from commit 9e225dc9b85c4fc2d3d910846bd0da25bc6a40df)
     Co-authored-by: Peng Huo &lt;penghuo@gmail.com&gt;

* __Auto-increment next development iteration. (#1816) (#2164)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 2 Mar 2022 15:46:28 -0800

    * Auto-increment next development iteration.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * Make bwc increments on X.Y and main branches.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;
    Signed-off-by: dblock
    &lt;dblock@dblock.org&gt;
     Co-authored-by: Daniel Doubrovkine (dB.) &lt;dblock@dblock.org&gt;


* __Downgrade to JDK 11. (#2301)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 2 Mar 2022 13:37:23 -0500

    * Downgrade to JDK 11.
     Signed-off-by: dblock &lt;dblock@dblock.org&gt;

    * Added support for patch JDK version, like 11.0.14+1
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Use JDK 11.0.14.1+1.
     Signed-off-by: dblock &lt;dblock@dblock.org&gt;

    * ./gradlew :build-tools:spotlessApply
     Signed-off-by: dblock &lt;dblock@dblock.org&gt;
     Co-authored-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Adding shards per node constraint for predictability to testClusterGr… (#2110) (#2265)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Mon, 28 Feb 2022 15:59:57 -0600

    * Adding shards per node constraint for predictability to
    testClusterGreenAfterPartialRelocation
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Fixing precommit violation
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Adding assertion to ensure invariant
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;
    (cherry picked from commit 8ae0db5285963b8e3552ce106ef1368813dbc8b1)
     Co-authored-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;


* __Revert &#34;[Backport 1.x] Override Default Distribution Download Url with Custom Distribution Url When User Passes a Url (#2191)&#34; (#2243)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 24 Feb 2022 15:58:30 -0800

    Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;


* __added config file to git issue template directory to disable blank issue creation (#2158) (#2249)__

    [Joshua Palis](mailto:99766446+joshpalis@users.noreply.github.com) - Thu, 24 Feb 2022 15:34:24 -0800

    Signed-off-by: Joshua Palis &lt;jpalis@amazon.com&gt;
     Co-authored-by: Joshua Palis &lt;jpalis@amazon.com&gt;
    (cherry picked from commit fb187eacc26487cd644f09091e462001d8839315)


* __Case Insensitive Support in Regexp Interval (#2237) (#2246)__

    [Matt Weber](mailto:matt@mattweber.org) - Thu, 24 Feb 2022 15:58:09 -0600

    1x backport of #2237.  Add a `case_insensitive` flag to regexp interval source.

    Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Add Factory to enable Lucene ConcatenateGraphFilter (#1278) (#2152) (#2219)__

    [Mau Bach Quang](mailto:quangmaubach@gmail.com) - Thu, 24 Feb 2022 10:49:31 -0800

    Lucene has a ConcatenateGraphFilter that can concatenate tokens from a
    TokenStream
    to create a single token (or several tokens that have the same
    position if
    input TokenStream is a graph).
     The change is to enable that ConcatenateGraphFilter by adding a Factory.

    (cherry-pick from 0e95bb9dff976a9c7f9cdac63a92040043d029e2)
     Signed-off-by: Mau Bach Quang &lt;quangmaubach@gmail.com&gt;


* __[Backport 1.x] Override Default Distribution Download Url with Custom Distribution Url When User Passes a Url (#2191)__

    [Rishikesh Pasham](mailto:62345295+Rishikesh1159@users.noreply.github.com) - Mon, 21 Feb 2022 10:56:00 -0800

    * Backport Enabling Sort Optimization to make use of Lucene
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

    * Backport Enabling Sort Optimization to make use of Lucene and small change in
    a method call signature
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

    * [Backport 1.x] Override Default Distribution Download Url with Custom
    Distribution Url When User Passes a Url
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

    * Adding Spotless check to previous PR
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

* __Support unordered non-overlapping intervals (#2103) (#2172)__

    [Matt Weber](mailto:matt@mattweber.org) - Fri, 18 Feb 2022 11:52:52 -0500

    This commit exposes Intervals.unorderedNoOverlaps (LUCENE-8828).

    (#2103 backport)
     Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Add regexp interval source (#1917) (#2069)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 16 Feb 2022 14:46:35 -0500

    * Add regexp interval source
     Add a regexp interval source provider so people can use regular
    expressions
    inside of intervals queries.
     Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;

    * Fixes

      - register regexp interval in SearchModule
      - use fully-qualified name for lucene RegExp
      - get rid of unnecessary variable
       Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;
      (cherry picked from commit b9420d8f70dfc168b9d44f736850af4ef7306a99)
       Co-authored-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Add proxy username and password settings for Azure repository (#2098) (#2108)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Wed, 16 Feb 2022 12:01:51 -0500

    Added username/password proxy settings for Azure repository.
    Security
    settings:
    - azure.client.*.proxy.username - Proxy user name
    - azure.client.*.proxy.password - Proxy user password
     Signed-off-by: Andrey Pleskach &lt;ples@aiven.io&gt;
    (cherry picked from commit 62361ceafce4abb735567066d1c4865ca6d7136f)
     Co-authored-by: Andrey Pleskach &lt;ples@aiven.io&gt;


* __Support first and last parameter for missing bucket ordering in composite aggregation (#1942) (#2049)__

    [Peng Huo](mailto:penghuo@gmail.com) - Tue, 15 Feb 2022 14:07:35 -0800

    Support for &#34;first&#34; and &#34;last&#34; parameters for missing bucket ordering in
    composite aggregation.
    By default, if order is asc, missing_bucket at first,
    if order is desc, missing_bucket at last. If
    missing_order is &#34;first&#34; or
    &#34;last&#34;, regardless order, missing_bucket is at first or last respectively.
     Signed-off-by: Peng Huo &lt;penghuo@gmail.com&gt;


* __Mapping update for “date_range” field type is not idempotent (#2094) (#2106)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Tue, 15 Feb 2022 11:44:17 -0600

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;
    (cherry picked from commit 6b6f03368f49f5f8001d6d0ed85cd9af7bab76f6)
     Co-authored-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Fix integration tests failure (#2067) (#2090)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Fri, 11 Feb 2022 11:07:46 -0500

    Fixed integration tests failure on Linux with Kernel 5.16.x
     Signed-off-by: Andrey Pleskach &lt;ples@aiven.io&gt;
    (cherry picked from commit 27ed6fc82c7db7a3a741499f0dbd7722fa053f9d)
     Co-authored-by: Andrey Pleskach &lt;ples@aiven.io&gt;


* __Backport/backport 2048,2074 to 1.x (#2085)__

    [Ankit Jain](mailto:jain.ankitk@gmail.com) - Fri, 11 Feb 2022 09:36:21 -0500

    * Stabilizing org.opensearch.cluster.routing.MovePrimaryFirstTests.test…
    (#2048)

    * Stabilizing
    org.opensearch.cluster.routing.MovePrimaryFirstTests.testClusterGreenAfterPartialRelocation

     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Removing unused import
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Making code more readable
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;
    (cherry picked from commit 343b82fe24525bbab01ef5a0d9bb8917068c71bf)

    * Added timeout to ensureGreen() for testClusterGreenAfterPartialRelocation
    (#2074)
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;
    (cherry picked from commit f0984eb409d44e8b68deb1c262bf81accc300acb)


* __Removing lingering transportclient (#1955) (#2088)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 10 Feb 2022 17:21:12 -0800

    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;
    (cherry picked from commit 781156471a1827b1b66445f716c7567f714dda86)
     Co-authored-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Prioritize primary shard movement during shard allocation (#1445) (#2079)__

    [opensearch-trigger-bot[bot]](mailto:98922864+opensearch-trigger-bot[bot]@users.noreply.github.com) - Thu, 10 Feb 2022 13:48:38 -0500

    When some node or set of nodes is excluded (based on some cluster setting)
    BalancedShardsAllocator iterates over them in breadth first order picking 1
    shard from
    each node and repeating the process until all shards are balanced.
    Since shards from
    each node are picked randomly it&#39;s possible the p and r of
    shard1 is relocated first
    leaving behind both p and r of shard2. If the
    excluded nodes were to go down the
    cluster becomes red.
     This commit introduces a new setting
    &#34;cluster.routing.allocation.move.primary_first&#34;
    that prioritizes the p of both
    shard1 and shard2 first so the cluster does not become
    red if the excluded
    nodes were to go down before relocating other shards. Note that
    with this
    setting enabled performance of this change is a direct function of number
    of
    indices, shards, replicas, and nodes. The larger the indices, replicas, and
    distribution scale, the slower the allocation becomes. This should be used with
    care.
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;
    (cherry picked from commit 6eb8f6f307567892bbabbe37aff7cd42be486df0)
     Co-authored-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;


* __Adding workflow to auto delete backport merged branches from backport workflow (#2050) (#2065)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Tue, 8 Feb 2022 12:27:45 -0800

    Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;
    (cherry picked from commit 9c9e218ae697b65e410304825cac81ccdf355e66)
     Co-authored-by: Vacha &lt;vachshah@amazon.com&gt;


* __Another attempt to fix o.o.transport.netty4.OpenSearchLoggingHandlerIT fails w/ stack overflow (#2051) (#2055)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Mon, 7 Feb 2022 16:11:06 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;
    (cherry picked from commit 1e5d98329eaa76d1aea19306242e6fa74b840b75)
     Co-authored-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __added backport for 1.2.5 to 1.x branch (#2057)__

    [Abhinav Gupta](mailto:guptabhi123@gmail.com) - Mon, 7 Feb 2022 13:33:27 -0500

    Signed-off-by: Abhinav Gupta &lt;abhng@amazon.com&gt;


* __[Backport] Introduce FS Health HEALTHY threshold to fail stuck node (#1269)__

    [Bukhtawar Khan](mailto:bukhtawa@amazon.com) - Mon, 7 Feb 2022 12:48:03 -0500

    * Introduce FS Health HEALTHY threshold to fail stuck node (#1167)
     This will cause the leader stuck on IO during publication to step down and
    eventually trigger a leader election.
    * Issue Description
      * The publication of cluster state is time bound to 30s by a
        cluster.publish.timeout settings. If this time is reached before the new
        cluster state is committed, then the cluster state change is rejected and the
        leader considers itself to have failed. It stands down and starts trying to
        elect a new master.
         There is a bug in leader that when it tries to publish the new cluster state
        it first tries acquire a lock to flush the new state under a mutex to disk. The
        same lock is used to cancel the publication on timeout. Below is the state of
        the timeout scheduler meant to cancel the publication. So essentially if the
        flushing of cluster state is stuck on IO, so will the cancellation of the
        publication since both of them share the same mutex. So leader will not step
        down and effectively block the cluster from making progress.
         Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up settings
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up tests
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up tests
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up tests
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;


* __[Backport] Handle shard over allocation during partial zone/rack or independent … (#1268)__

    [Bukhtawar Khan](mailto:bukhtawa@amazon.com) - Mon, 7 Feb 2022 09:58:10 -0500

    * Handle shard over allocation during partial zone/rack or independent node
    failures  (#1149)
     The changes ensure that in the event of a partial zone failure, the surviving
    nodes in the minority zone don&#39;t get overloaded with shards, this is governed
    by a skewness limit.
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up imports
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up imports
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up imports
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;

    * Fix up check style
     Signed-off-by: Bukhtawar Khan &lt;bukhtawa@amazon.com&gt;


* __Add Version.V_1_2_5 constant__

    [Nicholas Walter Knize](mailto:nknize@apache.org) - Fri, 4 Feb 2022 18:29:46 -0600

    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __add 1.2.5 to bwcVersions__

    [Nicholas Walter Knize](mailto:nknize@apache.org) - Fri, 4 Feb 2022 18:29:35 -0600

    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __Revert &#34;Upgrading Shadow plugin to 7.1.2 (#2033) (#2037)&#34; (#2047)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Wed, 2 Feb 2022 19:40:51 -0800

    This reverts commit 8725061c15fac70a81d144ed2d79b09f5e1a2f7f.
     Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Fix AssertionError message (#2044) (#2045)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 2 Feb 2022 21:05:15 -0500

    Signed-off-by: Lukáš Vlček &lt;lukas.vlcek@aiven.io&gt;
    (cherry picked from commit 270c59f523acbb3af73ab56dbcfe754e619fdca9)
     Co-authored-by: Lukáš Vlček &lt;lukas.vlcek@aiven.io&gt;


* __Upgrading Shadow plugin to 7.1.2 (#2033) (#2037)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 2 Feb 2022 14:52:31 -0800

    Shadow plugin is used for publishing jars
    and this upgrades Log4J dependency
    for build.
     Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;
    (cherry picked from commit 1f9517c4caee48eda6eee77f603d815af1fd7770)
     Co-authored-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __[FEATURE] Add OPENSEARCH_JAVA_HOME env to override JAVA_HOME (#2040)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Wed, 2 Feb 2022 12:16:50 -0800

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __build: introduce support for reproducible builds (#1995) (#2038)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 2 Feb 2022 14:21:34 -0500

    Reproducible builds is an initiative to create an independently-verifiable path
    from source to binary code [1]. This can be done by:
    - Make all archive tasks in gradle reproducible by ignoring timestamp on files
    [2]
    - Preserve the order in side the archives [2]
    - Ensure GlobalBuildInfoPlugin.java use [SOURCE_DATE_EPOCH] when available

    [SOURCE_DATE_EPOCH]: https://reproducible-builds.org/docs/source-date-epoch/
    [1]: https://reproducible-builds.org/
    [2]:
    https://docs.gradle.org/current/userguide/working_with_files.html#sec:reproducible_archives

     Signed-off-by: Leonidas Spyropoulos &lt;artafinde@gmail.com&gt;
    (cherry picked from commit 6da253b8fff9a9d9cbbf65807efa7aeaddc9c9d3)
     Co-authored-by: Leonidas Spyropoulos &lt;artafinde@gmail.com&gt;


* __[1x] Deprecate index.merge.policy.max_merge_at_once_explicit (#1981) (#1984)__

    [Nick Knize](mailto:nknize@apache.org) - Wed, 2 Feb 2022 11:48:00 -0600

    max_merge_at_once_explicit is removed in lucene 9 so the index setting is

    deprecated for removal in the next major release.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __[Deprecate] Setting explicit version on analysis component (#1978) (#1985)__

    [Nick Knize](mailto:nknize@apache.org) - Wed, 2 Feb 2022 12:28:44 -0500

    Lucene 9 removes the ability to define an explicit version on an analysis

    component. The version parameter is deprecated at parse time and a warning is

    issued to the user through the deprecation logger.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __[BUG] Docker distribution builds are failing. Switching to http://vault.centos.org (#2024) (#2030)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 1 Feb 2022 14:16:50 -0600

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __[Backport] Enabling Sort Optimization to make use of Lucene (#1989)__

    [Rishikesh Pasham](mailto:62345295+Rishikesh1159@users.noreply.github.com) - Mon, 31 Jan 2022 10:48:13 -0600

    * Backport Enabling Sort Optimization to make use of Lucene
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

    * Backport Enabling Sort Optimization to make use of Lucene and small change in
    a method call signature
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;


* __Upgrading Jackson-Databind version (#1982) (#1987)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Sun, 30 Jan 2022 16:32:14 -0800

    * Upgrading Jackson-Databind version
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;

    * Adding jackson-databind version using getProperty method
     Signed-off-by: Rishikesh1159 &lt;rishireddy1159@gmail.com&gt;
    (cherry picked from commit 1568407c362b2534366048379f1bd93f2d164d89)
     Co-authored-by: Rishikesh Pasham
    &lt;62345295+Rishikesh1159@users.noreply.github.com&gt;


* __Update bundled JDK distribution to 17.0.2+8 (#2007) (#2009)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Sat, 29 Jan 2022 09:45:19 -0800

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Linked the formatting setting file (#1860) (#1961)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Thu, 27 Jan 2022 12:24:34 -0800

    Signed-off-by: Owais Kazi &lt;owaiskazi19@gmail.com&gt;
    (cherry picked from commit cfc9ec292dea2169495deffe6d1e25b654e5e35e)
     Co-authored-by: Owais Kazi &lt;owaiskazi19@gmail.com&gt;


* __Add hook to execute logic before Integ test task starts (#1969) (#1971)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 26 Jan 2022 17:42:45 -0600

    Add hook to execute custom logic before the integ test starts.
    This is
    required for a workaround to enable the jacoco code coverage for Integ Tests.
     Signed-off-by: Ankit Kala &lt;ankikala@amazon.com&gt;


* __Fixing typo in TESTING.md (#1849) (#1959)__

    [github-actions[bot]](mailto:41898282+github-actions[bot]@users.noreply.github.com) - Wed, 26 Jan 2022 17:41:33 -0600

    Fixes some grammar and link typos found in TESTING.md.
     Signed-off-by: Vacha Shah &lt;vachshah@amazon.com&gt;


* __Add max_expansions option to wildcard interval (#1916) (#1979)__

    [Matt Weber](mailto:matt@mattweber.org) - Wed, 26 Jan 2022 16:08:34 -0600

    Add support for setting the max expansions on a wildcard interval.
    The default
    value is still 128 and the max value is bounded by
    `BooleanQuery.getMaxClauseCount()`.
     Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Update protobuf-java to 3.19.3 (#1945) (#1949)__

    [Tianli Feng](mailto:ftl94@live.com) - Fri, 21 Jan 2022 08:52:05 -0800

    * Update protobuf-java to 3.19.3
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;

    * Exclude some API usage violations in the package com.google.protobuf for
    thirdPartyAudit task to pass
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;


* __Timeout fix backport to 1.x (#1953)__

    [Suraj Singh](mailto:79435743+dreamer-89@users.noreply.github.com) - Thu, 20 Jan 2022 20:53:43 -0600

    * [Bug] Wait for outstanding requests to complete (#1925)
     Signed-off-by: Suraj Singh &lt;surajrider@gmail.com&gt;

    * [BUG] Wait for outstanding requests to complete in LastSuccessfulSett…
    (#1939)

    * [BUG] Wait for outstanding requests to complete in
    LastSuccessfulSettingsUpdate test
     Signed-off-by: Suraj Singh &lt;surajrider@gmail.com&gt;

    * [BUG] Wait for outstanding requests to complete in
    LastSuccessfulSettingsUpdate test
     Signed-off-by: Suraj Singh &lt;surajrider@gmail.com&gt;


* __Update Netty to 4.1.73.Final (#1936) (#1937)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Wed, 19 Jan 2022 00:00:42 -0500

    efs/remotes/origin/1.3
    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Modernize and consolidate JDKs usage across all stages of the build. Use JDK-17 as bundled JDK distribution to run tests (#1922)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 18 Jan 2022 10:51:53 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Expand SearchPlugin javadocs. (#1909) (#1923)__

    [Matt Weber](mailto:matt@mattweber.org) - Tue, 18 Jan 2022 09:48:52 -0600

    Add and clarify some search plugin point documentation.
     Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Make SortBuilders pluggable (#1856) (#1915)__

    [Matt Weber](mailto:matt@mattweber.org) - Mon, 17 Jan 2022 12:28:22 -0500

    Add the ability for plugin authors to add custom sort builders.
     Signed-off-by: Matt Weber &lt;matt@mattweber.org&gt;


* __Refactor LegacyESVersion tests from Version tests (#1662) (#1663)__

    [Nick Knize](mailto:nknize@apache.org) - Mon, 17 Jan 2022 12:27:56 -0500

    In preparation for removing all LegacyESVersion support by 3.0; this commit

    largely refactors the LegacyESVersion test logic from the OpenSearch Version

    test logic into an independent test class. This PR also updates
    Version.fromString
    to ensure a proper legacy version is returned when major is
    &gt; 3 (to support
    legacy yaml test and build scripts).
     Note that bwc w/ legacy versions are still supported so some cross
    compatibility
    testing is retained in the Version test class.

     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __Fixing org.opensearch.common.network.InetAddressesTests.testForStringIPv6WithScopeIdInput (#1913) (#1914)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Sat, 15 Jan 2022 10:14:17 -0600

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __[BUG] Serialization bugs can cause node drops (#1885) (#1911)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 14 Jan 2022 14:23:25 -0600

    This commit restructures InboundHandler to ensure all data
    is consumed over
    the wire.
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Fix o.o.transport.netty4.OpenSearchLoggingHandlerIT stack overflow test failure (#1900) (#1906)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 14 Jan 2022 12:38:18 -0600

    Attempt to fix o.o.transport.netty4.OpenSearchLoggingHandlerIT fails w/ stack
    overflow by
    hardening test expectation patterns in regex patterns
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Replace JCenter with Maven Central. (#1057) (#1892)__

    [Marc Handalian](mailto:handalm@amazon.com) - Wed, 12 Jan 2022 17:03:42 -0800

    On February 3 2021, JFrog
    [announced](https://jfrog.com/blog/into-the-sunset-bintray-jcenter-gocenter-and-chartcenter/)
    the shutdown of JCenter. Later on April 27 2021, an update was provided that
    the repository will only be read only and new package and versions are no
    longer accepted on JCenter.  This means we should no longer use JCenter for our
    central artifacts repository.
     This change replaces JCenter with Maven Central as per the Gradle
    recommendation - https://blog.gradle.org/jcenter-shutdown
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;
    Signed-off-by: Marc Handalian
    &lt;handalm@amazon.com&gt;
     Co-authored-by: Rabi Panda &lt;adnapibar@gmail.com&gt;


* __Update FIPS API libraries of Bouncy Castle (#1853) (#1886)__

    [Tianli Feng](mailto:ftl94@live.com) - Wed, 12 Jan 2022 09:37:05 -0500

    * Update bc-fips to 1.0.2.1
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;

    * Update bcpg-fips to 1.0.5.1
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;

    * Update bctls-fips to 1.0.12.2
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;

    * Use the unified bouncycastle version for bcpkix-jdk15on in HDFS testing
    fixture
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;


* __[1.x] Remove remaining Flavor Serialization (#1751) (#1757)__

    [Nick Knize](mailto:nknize@apache.org) - Thu, 6 Jan 2022 11:11:31 -0800

    * [Remove] Remaining Flavor Serialization (#1751)
     This commit removes unnecessary serialization of unused flavor variable in
    build
    metadata from V_1_3_0+
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;

    * change flavor version check to V_1_3_0
     This commit changes the flavor serialization check in Build from V_2_0_0 to
    V_1_3_0.
     Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;


* __Update junit to 4.13.1 (#1837) (#1842)__

    [Ashish Agrawal](mailto:ashisagr@amazon.com) - Wed, 5 Jan 2022 08:15:56 -0500

    * Update junit to 4.13.1
     Signed-off-by: Ashish Agrawal &lt;ashisagr@amazon.com&gt;

    * update junit to 4.13.2
     Signed-off-by: Ashish Agrawal &lt;ashisagr@amazon.com&gt;

    * update SHA1 file
     Signed-off-by: Ashish Agrawal &lt;ashisagr@amazon.com&gt;


* __Upgrading bouncycastle to 1.70 (#1832) (#1834)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Tue, 4 Jan 2022 11:57:01 -0800

    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Updatting Netty to 4.1.72.Final (#1831) (#1835)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Tue, 4 Jan 2022 11:56:39 -0800

    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Intermittent java.lang.Exception: Suite timeout exceeded (&gt;= 1200000 msec) (#1827)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Wed, 29 Dec 2021 13:23:49 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Execution failed for task &#39;:test:fixtures:azure/s3/hdfs/gcs-fixture:composeDown&#39; (#1824) (#1825)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Wed, 29 Dec 2021 11:28:51 -0600

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Update to log4j 2.17.1 (#1820) (#1822)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 28 Dec 2021 17:57:26 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __RestIntegTestTask fails because of missed log4j-core dependency (#1815) (#1818)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 28 Dec 2021 17:08:37 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Gradle clean failing after a failed gradle check, folders created by Docker under &#39;root&#39; user (#1726) (#1775)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Thu, 23 Dec 2021 17:42:09 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __[plugin] repository-azure: add configuration settings for connect/write/response/read timeouts (#1789) (#1802)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Thu, 23 Dec 2021 12:26:27 -0600

    * [plugin] repository-azure: add configuration settings for
    connect/write/response/read timeouts
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Addressing code review comments: renaming connectionXxx to connectXxx
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Addressing code review comments: adding timeout comment
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Add bwc version 1.2.4 (#1797)__

    [Rabi Panda](mailto:adnapibar@gmail.com) - Thu, 23 Dec 2021 10:19:15 -0500

    Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;


* __Use try-with-resources with MockLogAppender (#1595) (#1784)__

    [Andrew Ross](mailto:andrross@amazon.com) - Tue, 21 Dec 2021 20:03:11 -0800

    I previously added a helper that started a MockLogAppender to ensure it
    was never added to a Logger before it was started. I subsequently found
    the opposite case in RolloverIT.java where the appender was stopped
    before it was closed, therefore creating a race where a concurrently
    running test in the same JVM could cause a logging failure. This seems
    like a really easy mistake to make when writing a test or introduce when
    refactoring a test. I&#39;ve made a change to use try-with-resources to
    ensure that proper setup and teardown is done. This should make it much
    harder to introduce this particular test bug in the future. Unfortunately,
    it did involve touching a lot of files. The changes here are purely structural
    to leverage try-with-resources; no testing logic has been changed.

    Signed-off-by: Andrew Ross &lt;andrross@amazon.com&gt;


* __Ignore file order in test assertion (#1755) (#1782)__

    [Andrew Ross](mailto:andrross@amazon.com) - Mon, 20 Dec 2021 19:35:36 -0600

    This unit test asserts that a SHA file for a groovy dependency gets
    created.
    However, a SHA file for javaparser-core also gets created in
    the same
    directory. For some reason, builds were failing on my machine
    because
    `Files::list` was returning the javaparser-core file first. I
    don&#39;t believe
    there are any ordering guarantees with that API, so I
    relaxed the assertion to
    not depend on ordering.

    Signed-off-by: Andrew Ross &lt;andrross@amazon.com&gt;


* __Fixing allocation filters to persist existing state on settings update (#1718) (#1780)__

    [Ankit Jain](mailto:jain.ankitk@gmail.com) - Mon, 20 Dec 2021 18:31:53 -0500

    * Fixing allocation filters to persist existing state on settings update
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Adding test for filter settings update
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Adding more tests and review comments
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Adding assertion and unit test for operation type mismatch
     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;

    * Updating test names

     Signed-off-by: Ankit Jain &lt;jain.ankitk@gmail.com&gt;


* __Update to log4j 2.17.0 (#1771) (#1773)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Sat, 18 Dec 2021 11:33:16 -0800

   Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Updating .gitattributes for additional file types (#1727) (#1766)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Fri, 17 Dec 2021 15:51:12 -0800

    * Updating .gitattributes for additional types

    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Better JDK-18 EA (and beyond) support of SecurityManager (#1750) (#1753)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 17 Dec 2021 16:08:30 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Add version 1.2.3. (#1759)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Fri, 17 Dec 2021 09:14:45 -0800

    Signed-off-by: dblock &lt;dblock@dblock.org&gt;


* __[plugin] repository-azure is not working properly hangs on basic operations (#1740)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Thu, 16 Dec 2021 15:01:50 -0500

    * [plugin] repository-azure is not working properly hangs on basic operations
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

    * Added tests cases and TODO items, addressing code review comments
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Adding 1.2.2 (#1731) (#1736)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Wed, 15 Dec 2021 14:16:16 -0500

    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;


* __Upgrade to log4j 2.16.0 (#1721) (#1723)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 14 Dec 2021 12:19:35 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Support JDK 18 EA builds (#1714)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 14 Dec 2021 06:47:00 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Fixing .gitattributes for binary content, removing *.class files (#1717) (#1720)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Mon, 13 Dec 2021 16:11:25 -0800

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Upgrade to logj4 2.15.0 (#1705)__

    [Andrew Ross](mailto:andrross@amazon.com) - Fri, 10 Dec 2021 16:35:18 -0500

    Signed-off-by: Andrew Ross &lt;andrross@amazon.com&gt;


* __Add version 1.2.1. (#1701) (#1702)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Fri, 10 Dec 2021 15:36:19 -0500

    Signed-off-by: dblock &lt;dblock@dblock.org&gt;


* __Move Gradle wrapper and precommit checks into OpenSearch repo. (#1664) (#1678)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 8 Dec 2021 11:45:55 -0500

    * Move Gradle checks into OpenSearch repo.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * Use working-directory for gradle wrapper validation.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;

    * Use https://github.com/gradle/wrapper-validation-action.
     Signed-off-by: dblock &lt;dblock@amazon.com&gt;


* __Moving DCO to workflows (#1458) (#1666)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 8 Dec 2021 07:59:34 -0500

    Signed-off-by: CEHENKLE &lt;henkle@amazon.com&gt;
     Co-authored-by: CEHENKLE &lt;henkle@amazon.com&gt;


* __Start MockLogAppender before adding to static context (#1587) (#1659)__

    [Andrew Ross](mailto:andrross@amazon.com) - Mon, 6 Dec 2021 16:45:49 -0500

    I observed a test failure with the message
    &#39;Attempted to append to non-started appender mock&#39; from an assertion in
    `OpenSearchTestCase::after`. I believe this indicates that a
    MockLogAppender
    (which is named &#34;mock&#34;) was added as an appender to the
    static logging context
    and some other test in the same JVM happened to
    cause a logging statement to
    hit that appender and cause an error, which
    then caused an unrelated test to
    fail (because they share static state
    with the logger). Almost all usages of
    MockLogAppender start it
    immediately after creation. I found a few that did
    not and fixed those.
    I also made a static helper in MockLogAppender to start
    it upon
    creation.
     Signed-off-by: Andrew Ross &lt;andrross@amazon.com&gt;


* __Revert &#34;Support Gradle 7 (#1609) (#1622)&#34; (#1657)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Mon, 6 Dec 2021 10:47:35 -0500

    This reverts commit 93bd32b14270be0da8a6b5eef8eeabfce7eb2b58.
     Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Added .gitattributes to manage end-of-line checks for Windows/*nix systems (#1638) (#1655)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Mon, 6 Dec 2021 08:08:01 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Support Gradle 7 (#1609) (#1622)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Fri, 3 Dec 2021 15:53:57 -0500

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Renaming Slave terminology to Replica in 1.x branch (backporting) (#1645)__

    [Rishikesh Pasham](mailto:62345295+Rishikesh1159@users.noreply.github.com) - Fri, 3 Dec 2021 15:52:51 -0500

    Signed-off-by: Rishikesh Pasham &lt;rishireddy1159@gmail.com&gt;


* __Upgrading commons-codec in hdfs-fixture and cleaning up dependencies in repository-hdfs (#1603) (#1621)__

    [Vacha](mailto:vachshah@amazon.com) - Tue, 30 Nov 2021 17:17:21 -0500

    Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;


* __Rename field_masking_span to span_field_masking (#1606) (#1623)__

    [Xue Zhou](mailto:85715413+xuezhou25@users.noreply.github.com) - Mon, 29 Nov 2021 23:18:13 -0500

    * Rename field_masking_span to span_field_masking
     Signed-off-by: Xue Zhou &lt;xuezhou@amazon.com&gt;

    * Update SearchModuleTests.java
     Signed-off-by: Xue Zhou &lt;xuezhou@amazon.com&gt;

    * Rename field_masking_span to span_field_masking
     Signed-off-by: Xue Zhou &lt;xuezhou@amazon.com&gt;


* __Upgrade dependency (#1571) (#1594)__

    [Vacha](mailto:vachshah@amazon.com) - Mon, 29 Nov 2021 14:48:33 -0500

    * Upgrading guava, commons-io and apache-ant dependencies.
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;

    * Adding failureaccess since guava needs it.
     Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;


* __Lower build requirement from Java 14+ to Java 11+ (#940) (#1608)__

    [Marc Handalian](mailto:handalm@amazon.com) - Tue, 23 Nov 2021 21:41:59 -0500

    * Lower build requirement from Java 14+ to Java 11+
     Avoid use of -Werror -Xlint:all, which may change significantly across
    java
    releases (new warnings could be added). Instead, just list the
    warnings
    individually.
     Workaround JDK 11 compiler bug (JDK-8209058) that only impacts test fixture
    code in the build itself.
     Signed-off-by: Robert Muir &lt;rmuir@apache.org&gt;

    * Disable warning around -source 7 -release 7 for java version checker
     The java version checker triggers some default warnings because it
    targets
    java7:

      ```
       Task :distribution:tools:java-version-checker:compileJava FAILED
       warning: [options] source value 7 is obsolete and will be removed in a future release
       warning: [options] target value 7 is obsolete and will be removed in a future release
       warning: [options] To suppress warnings about obsolete options, use -Xlint:-options.

       error: warnings found and -Werror specified
      ```

    * Suppress this warning explicitly for this module.
     Signed-off-by: Robert Muir &lt;rmuir@apache.org&gt;

    * more java14 -&gt; java11 cleanup
     Signed-off-by: Robert Muir &lt;rmuir@apache.org&gt;
     Co-authored-by: Robert Muir &lt;rmuir@apache.org&gt;
    Signed-off-by: Marc Handalian
    &lt;handalm@amazon.com&gt;
     Co-authored-by: Daniel Doubrovkine (dB.) &lt;dblock@dblock.org&gt;
    Co-authored-by:
    Robert Muir &lt;rmuir@apache.org&gt;


* __Giving informative error messages for double slashes in API call URLs- [ BACKPORT-1.x ] (#1601)__

    [Megha Sai Kavikondala](mailto:kavmegha@amazon.com) - Tue, 23 Nov 2021 13:34:31 -0500

    * Integration test that checks for settings upgrade  (#1482)

    * Made changes.
     Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;

    * Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;
     Changes made by deleting the TestSettingsIT file and adding new lines in
    FullClusterRestartSettingsUpgradeIT.java
     Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;

    * Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;
     Informative error messages related to empty index name.[Backport]


* __Enable RestHighLevel-Client to set parameter require_alias for bulk index and reindex requests (#1604)__

    [Jan Baudisch](mailto:jan.baudisch.de@gmail.com) - Tue, 23 Nov 2021 10:20:07 -0500

    Signed-off-by: Jan Baudisch &lt;jan.baudisch.libri@gmail.com&gt;
     Co-authored-by: Jan Baudisch &lt;jan.baudisch.libri@gmail.com&gt;


* __Upgrading gson to 2.8.9 (#1541). (#1545)__

    [Vacha](mailto:vachshah@amazon.com) - Fri, 19 Nov 2021 16:52:38 -0500

    Signed-off-by: Vacha &lt;vachshah@amazon.com&gt;


* __[repository-azure] Update to the latest Azure Storage SDK v12, remove privileged runnable wrapper in favor of access helper (#1521) (#1538)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Thu, 11 Nov 2021 14:05:47 -0800

    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;


* __Integration test that checks for settings upgrade  (#1482) (#1524)__

    [Megha Sai Kavikondala](mailto:kavmegha@amazon.com) - Thu, 11 Nov 2021 12:55:11 -0800

    * Made changes.
     Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;

    * Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;
     Changes made by deleting the TestSettingsIT file and adding new lines in
    FullClusterRestartSettingsUpgradeIT.java
     Signed-off-by: Megha Sai Kavikondala &lt;kavmegha@amazon.com&gt;


* __Added logic to allow {dot} files on startup (#1437) (#1516)__

    [Ryan Bogan](mailto:10944539+ryanbogan@users.noreply.github.com) - Thu, 11 Nov 2021 11:02:03 -0800

    * Added logic to allow {dot} files on startup
     Signed-off-by: Ryan Bogan &lt;rbogan@amazon.com&gt;

    * Ensures that only plugin directories are returned by findPluginDirs()
     Signed-off-by: Ryan Bogan &lt;rbogan@amazon.com&gt;

    * Prevents . files from being returned as plugins
     Signed-off-by: Ryan Bogan &lt;rbogan@amazon.com&gt;


* __Add staged version 1.1.1 (#1509)__

    [Nick Knize](mailto:nknize@apache.org) - Thu, 4 Nov 2021 14:46:57 -0500

    Signed-off-by: Nicholas Walter Knize &lt;nknize@apache.org&gt;
