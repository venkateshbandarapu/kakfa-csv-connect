package com.csv.connector;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;
import  static com.csv.connector.ConfigConstants.*;
import static com.csv.connector.FileUtils.*;

public class CSVSourceTask extends SourceTask {

    Logger log=LoggerFactory.getLogger(CSVSourceTask.class);
    private Map<String, String> connectionProps;
    private ObjectMapper mapper;

    public String version() {
        return null;
    }

    public void start(Map<String, String> map) {
        this.connectionProps=map;
        this.mapper=new ObjectMapper();
        log.info("com.csv.connector.CSVSourceTask..start() method with :"+map);
    }
    /*
    actual task execution starts here..
     */

    public List<SourceRecord> poll() throws InterruptedException {

        log.info("com.csv.connector.CSVSourceTask..poll() method");
        List<SourceRecord> processedRecords=new ArrayList<>();
       // this.context.offsetStorageReader().offset()
        Iterator<File> files=getFilesListFromDir(connectionProps.get(INPUT_FILE_PATH),
                connectionProps.get(INPUT_FILE_PATTERN)).iterator();
        while (files.hasNext()){
            File file=files.next();
            List<SourceRecord> records=getRecordsFromCsvFile(file);
            if(records!=null){
                processedRecords.addAll(records);
            }
        }
        if(processedRecords.isEmpty()){
            Thread.sleep(12000);
            return null;
        }
        return processedRecords;
    }
    public void stop() {

    }

    public List<SourceRecord> getRecordsFromCsvFile(File file){
        List<SourceRecord> records= null;
        try {
            records = readCsvFile(file);
            log.info("processed the records for file:"+file.getName()+" so moving this file to "+this.connectionProps.get(FINISHED_FILE_PATH));

        } catch (IOException|ArrayIndexOutOfBoundsException e) {
            log.error("unable to process the file:"+file.getName()+" so moving this file to "+this.connectionProps.get(ERROR_FILE_PATH));
            try {
                MoveFile(file,this.connectionProps.get(ERROR_FILE_PATH));
            } catch (IOException ex) {
                log.error("unable to move the error file "+file+" to "+this.connectionProps.get(ERROR_FILE_PATH));
                ex.printStackTrace();
                return null;
            }
            e.printStackTrace();
            return null;
        }

        try {
            MoveFile(file,this.connectionProps.get("finished.path"));

        } catch (IOException e) {
            log.error("unable to move the processed file "+file+" to "+this.connectionProps.get(FINISHED_FILE_PATH));
            log.error("so skipping the records of "+file+" to write into kafka topic");
            e.printStackTrace();
            return null;
        }
        return records;

    }
    public List<SourceRecord> readCsvFile(File csvFile) throws IOException,ArrayIndexOutOfBoundsException {

        log.info("file name:"+csvFile);
        boolean headerProcessed=false;

        List<SourceRecord> records=new ArrayList<>();
         try(CSVReader reader=new CSVReader(new FileReader(csvFile))){
             String[] row;//=reader.iterator();

             while ((row=reader.readNext())!=null){
                 if (!headerProcessed){
                     headerProcessed=true;
                     continue;
                 }

                 Map<String,String> sourcePartition= new HashMap<>();
                 sourcePartition.put("fileName",csvFile.getName());

                 Map<String,Long> sourceOffset= new HashMap<>();
                 sourceOffset.put("offset",reader.getRecordsRead());

                 Emp dto=new Emp(Integer.parseInt(row[0]),row[1],Double.parseDouble(row[2]));

                 SourceRecord record=new SourceRecord(sourcePartition,sourceOffset,
                         connectionProps.get(KAFKA_TOPIC_NAME),null,
                         Schema.INT32_SCHEMA,dto.getEmp_id(),
                         Schema.STRING_SCHEMA,convertObjectToString(dto));
                 records.add(record);

        }

               // }
        }


        return records;

    }
    public String convertObjectToString(Emp e){
        String value = null;
        try {
         value = mapper.writeValueAsString(e);
        } catch (JsonProcessingException ex) {
            ex.printStackTrace();
        }
          return  value;
    }
}
