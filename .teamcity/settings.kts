import jetbrains.buildServer.configs.kotlin.v2018_2.*
import jetbrains.buildServer.configs.kotlin.v2018_2.buildFeatures.commitStatusPublisher
import jetbrains.buildServer.configs.kotlin.v2018_2.buildSteps.gradle
import jetbrains.buildServer.configs.kotlin.v2018_2.buildSteps.script
import jetbrains.buildServer.configs.kotlin.v2018_2.triggers.vcs

/*
The settings script is an entry point for defining a TeamCity
project hierarchy. The script should contain a single call to the
project() function with a Project instance or an init function as
an argument.

VcsRoots, BuildTypes, Templates, and subprojects can be
registered inside the project using the vcsRoot(), buildType(),
template(), and subProject() methods respectively.

To debug settings scripts in command-line, run the

    mvnDebug org.jetbrains.teamcity:teamcity-configs-maven-plugin:generate

command and attach your debugger to the port 8000.

To debug in IntelliJ Idea, open the 'Maven Projects' tool window (View
-> Tool Windows -> Maven Projects), find the generate task node
(Plugins -> teamcity-configs -> teamcity-configs:generate), the
'Debug' option is available in the context menu for the task.
*/

version = "2019.1"

project {

    buildType(Build)
}

object Build : BuildType({
    name = "Build"

    vcs {
        root(DslContext.settingsRoot)
    }

    steps {
        gradle {
            tasks = "buildDependents"
            incremental = true
            enableStacktrace = true
            dockerImage = "gradle:4.3.1"
            dockerRunParameters = "--net=host"
            coverageEngine = idea {
                includeClasses = "org.globsframework.*"
            }
        }
        script {
            name = "Fossa"
            scriptContent = """
                set -e
                ./gradlew build -x test
                fossa
                fossa test
            """.trimIndent()
            dockerImage = "gradle:4.3.1"
            dockerRunParameters = """
                -v /usr/local/bin/fossa:/usr/local/bin/fossa
                -e FOSSA_API_KEY=9009fadc3b7be531316376a3d5f3cd87
            """.trimIndent()
        }
        gradle {
            tasks = "publish"
            enableStacktrace = true
            dockerImage = "gradle:4.3.1"
            dockerRunParameters = "--net=host"
            param("teamcity.tool.jacoco", "")
        }
    }

    triggers {
        vcs {
        }
    }

    features {
        commitStatusPublisher {
            publisher = github {
                githubUrl = "https://api.github.com"
                authType = personalToken {
                    token = "credentialsJSON:5bc09a01-94ad-44aa-a6d6-3e1fcc90d6c2"
                }
            }
        }
    }

    dependencies {
        snapshot(AbsoluteId("OWP_Globsframework_Build")) {
            onDependencyFailure = FailureAction.FAIL_TO_START
        }
    }
})
