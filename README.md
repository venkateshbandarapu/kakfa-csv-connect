# kakfa-csv-connect
# to run the project on windows system the project:
1.mvn clean install

2.copy the project jar and opencsv-4.3.1.jar into kafka installation directory  ex. {kafka_dir}/libs 

3.copy the csv-connect-config.properties file from src/main/resources and put under {kafka_dir}/config path

4.create the kafka topic to which data needs to send and configure it in csv-connect-config.properties

5.create the directory structure for input.path,finished.path,error.path as pe mentioned in sv-connect-config.properties

6.run the zookeeper server and kafka server

7.launch the kafka consumer console window to see the data streaming coming from csv data files

8.open linux shell and navigate to kafka installation dir and run below command

    bin/connect-standalone.sh config/connect-standalone.properties config/csv-connect-config.properties
    
9. generate some .csv files inside input.path then this application starts consuming the files and will be pushed to specified kafka topic

