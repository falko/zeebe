// vim: set filetype=groovy:

pipelineJob('zeebe-release') {
    displayName 'Zeebe Release'
    definition {
        cps {
            script(readFileFromWorkspace('.ci/pipelines/release_zeebe.groovy'))
            sandbox()
        }
    }

    parameters {
        stringParam('RELEASE_VERSION', '0.18.0-alpha1', 'Which version to release?')
        stringParam('DEVELOPMENT_VERSION', '0.18.0-SNAPSHOT', 'Next development version?')
        booleanParam('PUSH_CHANGES', false, 'Push release to remote repositories and deploy docs?')
        booleanParam('IS_LATEST', false, 'Should the docker image be tagged as camunda/zeebe:latest?')
    }
}
