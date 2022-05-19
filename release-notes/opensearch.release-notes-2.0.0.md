## 2022-05-19 Version 2.0.0 Release Notes

### Breaking Changes in 2.0

#### Remove Mapping types
- [Type removal] Remove redundant _type in pipeline simulate action (#3371) (#3374)
- [Type removal] Remove _type deprecation from script and conditional processor (#3239) (#3390)
- [Type removal] Remove _type from _bulk yaml test, scripts, unused constants (#3372) (#3392)
- [Type removal] _type removal from mocked responses of scroll hit tests (#3377) (#3388)
- [Type Removal] Remove TypeFieldMapper usage, remove support of `_type` in searches and from LeafFieldsLookup (#3016) (#3236)
- [Type removal] Remove _type support in NOOP bulk indexing from client benchmark (#3076) (#3081)

#### Deprecations
- Deprecated reserved node id '_must_join_elected_master_' that used by DetachClusterCommand and replace with '_must_join_elected_cluster_manager_' (#3138) (#3150)
- [Remove] TypeFieldMapper (#3196) (#3327)
- [Remove] AliasesExistAction (#3149) (#3158)
- [Remove] TypesExist Action (#3139) (#3154)
- [Remove] ShrinkAction, ShardUpgradeRequest, UpgradeSettingsRequestBuilder (#3169) (#3176)

### Documentation
- [Javadocs] add to o.o.action, index, and transport (#3277) (#3314)
- [Javadocs] add to internal classes in o.o.http, indices, and search (#3288) (#3316)
- [Javadocs] Add to remaining o.o.action classes (#3182) (#3305)
- [Javadocs] add to o.o.rest, snapshots, and tasks packages (#3219) (#3311)
- [Javadocs] add to o.o.common (#3289) (#3317)
- [Javadocs] add to o.o.dfs,fetch,internal,lookup,profile, and query packages (#3261) (#3313)
- [Javadocs] add to o.o.search.aggs, builder, and collapse packages (#3254) (#3312)
- [Javadocs] add to o.o.index and indices (#3209) (#3310)
- [Javadocs] add to o.o.monitor,persistance,plugins,repo,script,threadpool,usage,watcher (#3186) (#3309)
- [Javadocs] Add to o.o.disovery, env, gateway, http, ingest, lucene and node pkgs (#3185) (#3306)
- [Javadocs] add to o.o.action.admin (#3155) (#3304)
- [Javadoc] Add missing package-info.java files to server (#3128) (#3302)
- [Javadocs] add to o.o.search.rescore,searchafter,slice, sort, and suggest (#3264) (#3267)
- [Javadocs] add to o.o.transport (#3220) (#3225)
- [Javadocs] add to o.o.cluster (#3170) (#3178)
- [Javadocs] add to o.o.bootstrap, cli, and client (#3163) (#3171)
- [Javadocs] add remaining internal classes and reenable missingJavadoc on server (#3296) (#3319)

### Features/Enhancements
- Removing hard coded value of max concurrent shard requests (#3364) (#3376)
- Update generated ANTLR lexer/parser to match runtime version (#3297) (#3308)
- Replace 'master' terminology with 'cluster manager' in log messages in 'server/src/main' directory - Part 2 (#3174)
- Rename BecomeMasterTask to BecomeClusterManagerTask in JoinTaskExecutor (#3099) (#3179) (#3208)

### Bug Fixes
- Fix minimum index compatibility error message (#3159) (#3172)
- Fixing PublishTests tests (running against unclean build folders) (#3253) (#3257)
- Fixing Scaled float field mapper to respect ignoreMalformed setting (#2918) (#3238)
- Fixing plugin installation URL to consume build qualifier (#3193) (#3216)
- Gradle plugin `opensearch.pluginzip` Add implicit dependency. (#3189) (#3212)

### Maintenance
- Bump re2j from 1.1 to 1.6 in /plugins/repository-hdfs (#3337) (#3341)
- Update bundled JDK to 17.0.3+7 (#3093) (#3275)
- Allow to configure POM for ZIP publication (#3252) (#3271)
- Added Adoptium JDK8 support and updated DistroTestPlugin JDK version used by Gradle (#3324) (#3326)
