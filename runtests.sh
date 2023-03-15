export WRITEHOST="jdbc:postgresql://johnpage-instance-1.c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59" 
export REPLICAS="jdbc:postgresql://johnpage.cluster-ro-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59,jdbc:postgresql://ro2.cluster-custom-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/mot?user=postgres&password=2efdaf4b59"


export WRITEHOST="jdbc:mysql://admin:2efdaf4b59@johnpage.cluster-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/MOT" 
export REPLICAS="jdbc:mysql://admin:2efdaf4b59@johnpage.cluster-ro-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/MOT

mv MOTService.log MOTService.log_`date +%Y%m%d%H%M%S`



java -jar bin/MOTService.jar -u  $WRITEHOST -r 80 -c 15 -m 5 -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 33 -c 33 -m 33 -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 0 -m 0 -s 1800

java -jar bin/MOTService.jar -u  $WRITEHOST -r 80 -c 15 -m 5 -x $REPLICAS -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 33 -c 33 -m 33 -x $REPLICAS -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 0 -m 0 -x $REPLICAS -s 1800



java -jar bin/MOTService.jar -u  $WRITEHOST -r 240 -c 45 -m 15 -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 100 -m 100 -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 300 -c 0 -m 0 -s 1800

java -jar bin/MOTService.jar -u  $WRITEHOST -r 240 -c 45 -m 15 -x $REPLICAS -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 100 -c 100 -m 100 -x $REPLICAS -s 1800
java -jar bin/MOTService.jar -u  $WRITEHOST -r 300 -c 0 -m 0 -x $REPLICAS -s 1800





