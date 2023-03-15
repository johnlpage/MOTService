export WRITEHOST="jdbc:postgresql://johnpage-instance-1.c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59" 
export REPLICAS="jdbc:postgresql://johnpage.cluster-ro-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59,jdbc:postgresql://ro2.cluster-custom-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59"

mv MOTService.log MOTService.log_`date +%Y%m%d%H%M%S`

java -jar bin/MOTService.jar -u  $WRITEHOST -r 240 -c 45 -m 15
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 100 -m 100
java -jar bin/MOTService.jar -u  $WRITEHOST -r 300 -c 0 -m 0

java -jar bin/MOTService.jar -u  $WRITEHOST -r 240 -c 45 -m 15 -x $REPLICAS
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 100 -m 100 -x $REPLICAS
java -jar bin/MOTService.jar -u  $WRITEHOST -r 300 -c 0 -m 0 -x $REPLICAS





