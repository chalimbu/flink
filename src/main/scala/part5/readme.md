in docker/flink/application-cluster/docker-compose.yml
modified line 7 changing to a class name part2datastreams.WindowFunctions (from one of hte apps of the course) 
fully qualified name

to deploy to docker we need to package all the code with the dependencies/connector

intellij
file - project structure 
project settings - artifact
+ icon
from modules with dedpencies
flink essentials
check radio buttom copy to output directory
okay, aply,okay


build menu at the top, build artifacts, and click build

then go to folder out/artifacts and copy flink-essentials, paste it in docker/flink/artifacts
volumnes on line 8, maps the artifacts to a file in flink

in application cluster docker-compose up
the job runs
then docker-compose down

------------------
cd ../session-cluster
docker-compose up
docker ps, get the name of hte job manager
docker exec -it  session-cluster-jobmanager-1 bash
./bin/flink run --detached --class part2datastreams.WindowFunctions usrlib/flink-essentials.jar

with the flink ui
docker ps
see the port and localhost:8081 (or por)
submit job, flink essentials jar
click, fully qualified name part2datastreams.WindowFunctions
