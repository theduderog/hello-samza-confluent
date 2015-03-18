DIR=$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )
ROOT_DIR=${DIR}/..

export SAMZA_CONTAINER_NAME=$1
#SAMZA_LOG_DIR
#Cannot set this now or else the defaults won't get picked up
#export JAVA_OPTS="-Dcom.sun.management.jmxremote -Dcom.sun.management.jmxremote.port=9010 -Dcom.sun.management.jmxremote.local.only=false -Dcom.sun.management.jmxremote.authenticate=false -Dcom.sun.management.jmxremote.ssl=false"

#rhoover - their run-job.sh script requires cwd be ${ROOT_DIR}/deploy/samza
(cd ${ROOT_DIR}/deploy/samza && exec ./bin/run-job.sh --config-factory=org.apache.samza.config.factories.PropertiesConfigFactory --config-path=file://${ROOT_DIR}//deploy/samza/config/$1.properties)
