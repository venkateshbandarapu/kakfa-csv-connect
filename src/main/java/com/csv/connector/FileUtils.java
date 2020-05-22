package com.csv.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import static com.csv.connector.ConfigConstants.INPUT_FILE_PATH;
import static com.csv.connector.ConfigConstants.INPUT_FILE_PATTERN;


public class FileUtils {

   private static Logger log= LoggerFactory.getLogger(FileUtils.class);

    public static void MoveFile(File source, String dest) throws IOException{
        log.info("dest path:"+(dest+"/"+source.getName()));
            if(source.exists()){
                if(source.renameTo(new File(dest+"/"+source.getName()))){
                    log.info("moving of file from "+source+" to "+dest+" is success full");
                }
                else
                {
                    log.error("moving of file from "+source+" to "+dest+" is failed");
                    throw new IOException("unable to move the file from "+source+" to "+dest);
                }

            }

    }
    public static List<File> getFilesListFromDir(String inputFilePath,String inputFilePattern){

        List<File> csvFiles=new ArrayList<>();
        File[] files=new File(inputFilePath).listFiles();

        for (File file:files){
            if(file.getName().matches(inputFilePattern)){
                csvFiles.add(file);
            }
        }
        return csvFiles;

    }
}
