package com.csv.connector;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.opencsv.CSVReader;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.util.*;
import  static com.csv.connector.ConfigConstants.*;

public class CSVSourceTask extends SourceTask {
    private Map<String, String> connectionProps;
    private ObjectMapper mapper;

    public String version() {
        return null;
    }

    public void start(Map<String, String> map) {
        this.connectionProps=map;
        this.mapper=new ObjectMapper();
        System.out.println("com.csv.connector.CSVSourceTask..start() method with :"+map);
    }

    public List<SourceRecord> poll() throws InterruptedException {
        Thread.sleep(11000);
        System.out.println("com.csv.connector.CSVSourceTask..poll() method");
        List<SourceRecord> processedRecords=new ArrayList<>();
       // this.context.offsetStorageReader().offset()
        Iterator<File> files=getFilesList().iterator();
        while (files.hasNext()){
            File file=files.next();
            List<SourceRecord> records= null;
            try {
                records = readCsvFile(file);
                processedRecords.addAll(records);
                System.out.println("processed the records for file:"+file.getName()+" so moving this file to "+this.connectionProps.get(FINISHED_FILE_PATH));
                FileUtils.MoveFile(file,this.connectionProps.get("finished.path"));
            } catch (IOException e) {
                System.out.println("unable to read/open the file:"+file.getName()+" so moving this file to "+this.connectionProps.get(ERROR_FILE_PATH));
                FileUtils.MoveFile(file,this.connectionProps.get(ERROR_FILE_PATH));
                e.printStackTrace();
            }
        }
        if(processedRecords.isEmpty()){
            Thread.sleep(10000);
            return null;
        }
        return processedRecords;
    }

    public void stop() {

    }
    public  List<File> getFilesList(){

        List<File> csvFiles=new ArrayList<>();
        File[] files=new File(this.connectionProps.get(INPUT_FILE_PATH)).listFiles();

        for (File file:files){
            if(file.getName().matches(this.connectionProps.get(INPUT_FILE_PATTERN))){
                csvFiles.add(file);
            }
        }
        return csvFiles;

    }
    public List<SourceRecord> readCsvFile(File csvFile) throws IOException {

        System.out.println("file name:"+csvFile);
        boolean headerProcessed=false;

        List<SourceRecord> records=new ArrayList<>();
        CSVReader reader=new CSVReader(new FileReader(csvFile));

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
               // }
            }

           reader.close();

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
