# MOTService
A simulation of the UK MOT lookup site comparing RDBMS and Document Databases

Start an EC2 instance in the region you want to use, ensure it is large enough
20 GB Disk, 4GB RAM at least.


Install MySQL  client on EC2
----------------------
```

sudo rpm --import https://repo.mysql.com/RPM-GPG-KEY-mysql-2022

curl -OL https://downloads.mysql.com/archives/get/p/23/file/mysql-5.7.33-1.el7.x86_64.rpm-bundle.tar

tar xvf mysql-5.7.33-1.el7.x86_64.rpm-bundle.tar

sudo rpm -e postfix-2.10.1-6.amzn2.0.3.x86_64

sudo yum install -y mysql-community-common-5.7.33-1.el7.x86_64.rpm
sudo yum install -y mysql-community-lib*
sudo yum install -y mysql-community-client-5.7.33-1.el7.x86_64.rpm


```

Setup Aurora/RDS
--------------

db.r5.large, MySQL compatible 5.7 2vcpu, 16GB RAM, 4750Mb/s (This may be a limit!)
Setup connection to compute resource (The instance above)

In Ireland this is $0.32x2 per hour!! - Assuming a replica obvs.
Storage is $0.11 per GM/Month (not a lot)
IO is at $0.22 per millon requests though!


Download and load data
------------------------

Edit loaddata.sh with a line like

`/usr/bin/mysql  --local-infile -uadmin -pYOUR_PASSWORD -h YOURDBNAME.cluster-c41swlgcxzrp.eu-west-1.rds.amazonaws.com "`

```
sudo yum -y install git`
git clone https://github.com/johnlpage/MOTService.git

cd MOTService 


./loaddata.sh

```

Build testbed
---------------
```
sudo yum -y install java-1.8.0-openjdk-devel.x86_64

sudo wget http://repos.fedorapeople.org/repos/dchen/apache-maven/epel-apache-maven.repo -O /etc/yum.repos.d/epel-apache-maven.repo
sudo sed -i s/\$releasever/6/g /etc/yum.repos.d/epel-apache-maven.repo
sudo yum install -y apache-maven
mvn --version

export JAVA_HOME=/usr/lib/jvm/java-1.8.0-openjdk-1.8.0.352.b08-2.amzn2.0.1.x86_64

mvn clean package
```

Run RDS Test (and warm DB)
--------------------------
```
java -jar bin/MOTService.jar -t 64 -u  "jdbc:mysql://admin:2efdaf4b59@johnpage.cluster-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/MOT?useUnicode=true&useServerPrepStmts=true&useSSL=false&zeroDateTimeBehavior=convertToNull" 

```

Start a MongoDB Atlas cluster - using M30 (2 CPU, 8GB RAM, 40GB Disk) $0.59 per hour - no additional costs (I think )


Migrate data into Atlas
------------------------

```

curl -OL https://repo.mongodb.org/yum/amazon/2022/mongodb-org/6.0/x86_64/RPMS/mongodb-mongosh-1.6.2.x86_64.rpm

sudoo rpm -i mongodb-mongosh-1.6.2.x86_64.rpm

java -jar bin/MOTService.jar -u  "jdbc:mysql://admin:2efdaf4b59@johnpage.cluster-c41swlgcxzrp.eu-west-1.rds.amazonaws.com/MOT?useUnicode=true&useServerPrepStmts=true&useSSL=false&zeroDateTimeBehavior=convertToNull" -d "mongodb+srv://jlp:2efdaf4b59@speedtest.eez2n.mongodb.net"

#Make the index

mongosh "mongodb+srv://jlp:2efdaf4b59@speedtest.eez2n.mongodb.net/mot"  --eval 'db.testresult.createIndex({vehicleid:1})
```

Test in Atlas
---------------

```
java -jar bin/MOTService.jar -t 64 -u  "mongodb+srv://jlp:2efdaf4b59@speedtest.eez2n.mongodb.net"
```