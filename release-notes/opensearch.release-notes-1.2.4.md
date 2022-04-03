## 2022-01-18 Version 1.2.4

* __Update FIPS API libraries of Bouncy Castle (#1853) (#1888)__

    [Tianli Feng](mailto:ftl94@live.com) - Thu, 13 Jan 2022 10:48:38 -0500
    
    EAD -&gt; refs/heads/1.2, tag: refs/tags/1.2.4, refs/remotes/upstream/1.2
    * Update bc-fips to 1.0.2.1
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;
    
    * Update bcpg-fips to 1.0.5.1
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;
    
    * Update bctls-fips to 1.0.12.2
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;
    
    * Use the unified bouncycastle version for bcpkix-jdk15on in HDFS testing
    fixture
     Signed-off-by: Tianli Feng &lt;ftl94@live.com&gt;

* __[Backport 1.2] Replace JCenter with Maven Central. (#1057) and update plugin repository order. (#1894)__

    [Marc Handalian](mailto:handalm@amazon.com) - Wed, 12 Jan 2022 15:18:22 -0800
    
    
    * Replace JCenter with Maven Central. (#1057)
     On February 3 2021, JFrog
    [announced](https://jfrog.com/blog/into-the-sunset-bintray-jcenter-gocenter-and-chartcenter/)
    the shutdown of JCenter. Later on April 27 2021, an update was provided that
    the repository will only be read only and new package and versions are no
    longer accepted on JCenter.  This means we should no longer use JCenter for our
    central artifacts repository.
     This change replaces JCenter with Maven Central as per the Gradle
    recommendation - https://blog.gradle.org/jcenter-shutdown
     Signed-off-by: Rabi Panda &lt;adnapibar@gmail.com&gt;
    
    * Define plugin repositories order in settings.gradle.
     Signed-off-by: Marc Handalian &lt;handalm@amazon.com&gt;
     Co-authored-by: Rabi Panda &lt;adnapibar@gmail.com&gt;

* __Updatting Netty to 4.1.72.Final (#1831) (#1890)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Wed, 12 Jan 2022 08:29:25 -0800
    
    
    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;

* __Upgrading bouncycastle to 1.70 (#1832) (#1889)__

    [Sarat Vemulapalli](mailto:vemulapallisarat@gmail.com) - Tue, 11 Jan 2022 17:20:29 -0800
    
    
    Signed-off-by: Sarat Vemulapalli &lt;vemulapallisarat@gmail.com&gt;

* __RestIntegTestTask fails because of missed log4j-core dependency (#1815) (#1819)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 28 Dec 2021 17:47:13 -0500
    
    
    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Update to log4j 2.17.1 (#1820) (#1823)__

    [Andriy Redko](mailto:andriy.redko@aiven.io) - Tue, 28 Dec 2021 17:46:53 -0500
    
    
    Signed-off-by: Andriy Redko &lt;andriy.redko@aiven.io&gt;

* __Prepare for next development iteration, 1.2.4. (#1792)__

    [Daniel Doubrovkine (dB.)](mailto:dblock@dblock.org) - Wed, 22 Dec 2021 15:11:11 -0800
    
    
    Signed-off-by: dblock &lt;dblock@amazon.com&gt;


