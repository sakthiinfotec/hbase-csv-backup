STEPS TO RUN HBASE-BACKUP APPLICATION
-------------------------------------
1. Take a maven build using "mvn clean package", we will get backup-utils-0.0.1-SNAPSHOT.jar
2. Copy the backup-utils-0.0.1-SNAPSHOT.jar file from /home/<username>/workspace/hbase-backup/target into "dist/hbase-backup/lib" path
3. Make necessary configuration changes in "conf/config.yml" file
4. Run the application using "./run.sh"