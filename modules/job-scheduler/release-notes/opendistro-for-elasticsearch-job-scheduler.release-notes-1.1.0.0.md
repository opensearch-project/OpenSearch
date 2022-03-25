## 2019-06-25, version 1.1.0.0

### Enhancements
* Use ROOT Locale for Strings ([#14](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/14))
* Adds equals, hashCode, toString overrides to IntervalSchedule and CronSchedule ([#13](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/13))
* Override equals and hashCode for LockModel ([#12](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/12))
* Change log level when sweeper already have latest job version ([#11](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/11))

### Maintenance
* Add Support for Elasticsearch 7.1 ([#16](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/16))

### Refactoring
* Refactor JobSweeper to do sweep on certain clusterChangedEvent ([#10](https://github.com/opendistro-for-elasticsearch/job-scheduler/pull/10))
