job("Gradle_Check_backup") {
	description("Test")
	keepDependencies(false)
	parameters {
		stringParam("sha1", "main", "")
	}
	scm {
		git {
			remote {
				name("origin")
				github("opensearch-project/OpenSearch", "https")
			}
			branch("\${sha1}")
		}
	}
	disabled(true)
	triggers {
		githubPullRequest {
			cron("H/2 * * * *")
			permitAll(false)
			triggerPhrase(".*start\\W+gradle\\W+check.*")
		}
	}
	concurrentBuild(true)
	steps {
		shell("""#!/bin/bash
bash opensearch-infra/jenkins/jobs/OpenSearch_CI/PR_Checks/Gradle_Check/gradle-check-assemble.sh""")
	}
	publishers {
		archiveArtifacts {
			pattern("gradle_check_\${BUILD_NUMBER}*")
			allowEmpty(false)
			onlyIfSuccessful(false)
			fingerprint(false)
			defaultExcludes(true)
		}
	}
	wrappers {
		preBuildCleanup {
			deleteDirectories(false)
			cleanupParameter()
		}
		timeout {
			absolute(120)
		}
		timestamps()
	}
	configure {
		it / 'properties' / 'jenkins.model.BuildDiscarderProperty' {
			strategy {
				'daysToKeep'('-1')
				'numToKeep'('150')
				'artifactDaysToKeep'('-1')
				'artifactNumToKeep'('-1')
			}
		}
		it / 'properties' / 'com.coravy.hudson.plugins.github.GithubProjectProperty' {
			'projectUrl'('https://github.com/opensearch-project/OpenSearch/')
			displayName()
		}
		it / 'properties' / 'com.sonyericsson.rebuild.RebuildSettings' {
			'autoRebuild'('false')
			'rebuildDisabled'('false')
		}
	}
}
