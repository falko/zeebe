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
        string(name: 'RELEASE_VERSION', defaultValue: '0.x.0', description: 'Which version to release?')
        string(name: 'DEVELOPMENT_VERSION', defaultValue: '0.y.0-SNAPSHOT', description: 'Next development version?')
        booleanParam(name: 'PUSH_CHANGES', defaultValue: true, description: 'Push release to remote repositories and deploy docs?')
        booleanParam('IS_LATEST', true, 'Should the docker image be tagged as camunda/zeebe:latest?')
    }
}
