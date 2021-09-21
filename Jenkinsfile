@NonCPS
def getChangeString() {
  MAX_MSG_LEN = 100
  def changeString = ""
  echo "Gathering SCM changes"
  def changeLogSets = currentBuild.changeSets
  for (int i = 0; i < changeLogSets.size(); i++) {
    def entries = changeLogSets[i].items
    for (int j = 0; j < entries.length; j++) {
      def entry = entries[j]
      truncated_msg = entry.msg.take(MAX_MSG_LEN)
      changeString += " - ${truncated_msg} [${entry.author}]\n"
    }
  }

  if (!changeString) {
    changeString = " - No new changes"
  }
  return changeString
}
// Changes func
properties([
    buildDiscarder(logRotator(daysToKeepStr: '', numToKeepStr: '3')),
])
timestamps {
  node {
    try {
      stage('Checkout SCM') {
        checkout([
          $class: 'GitSCM',
          branches: [[name: "${env.BRANCH_NAME}"]],
          doGenerateSubmoduleConfigurations: false,
          extensions: [],
          submoduleCfg: [],
          userRemoteConfigs: [[credentialsId: 'awx.integrations',url: "git@bitbucket.org:citeck/ecos-zookeeper.git"]]
        ])
      }
      def project_version = readMavenPom().getProperties().getProperty("revision")
      def project_id = readMavenPom().getArtifactId()
      mattermostSend endpoint: 'https://mm.citeck.ru/hooks/9ytch3uox3retkfypuq7xi3yyr', channel: "build_notifications", color: 'good', message: " :arrow_forward: **Build project ${project_id}:**\n**Branch:** ${env.BRANCH_NAME}\n**Version:** ${project_version}\n**Build id:** ${env.BUILD_NUMBER}\n**Build url:** ${env.BUILD_URL}\n**Changes:**\n" + getChangeString()
      if ((env.BRANCH_NAME != "master") && (!project_version.contains('SNAPSHOT')))  {
        echo "Assembly of release artifacts is allowed only from the master branch!"
        currentBuild.result = 'SUCCESS'
        return
      }
      stage('Assembling and publishing project artifacts') {
        withMaven(mavenLocalRepo: '/opt/jenkins/.m2/repository', tempBinDir: '') {
          sh "mvn clean deploy"
        }
      }
    }
    catch (Exception e) {
      currentBuild.result = 'FAILURE'
      error_message = e.getMessage()
      echo error_message
    }
    script{
      if(currentBuild.result != 'FAILURE'){
        mattermostSend endpoint: 'https://mm.citeck.ru/hooks/9ytch3uox3retkfypuq7xi3yyr', channel: "build_notifications", color: 'good', message: " :white_check_mark: **Build project with ID ${env.BUILD_NUMBER} complete!**"
      }
      else{
        mattermostSend endpoint: 'https://mm.citeck.ru/hooks/9ytch3uox3retkfypuq7xi3yyr', channel: "build_notifications", color: 'danger', message: " @channel :exclamation: **Build project with ID ${env.BUILD_NUMBER} failure with message:**\n```${error_message}```"
      }
    }
  }
}
