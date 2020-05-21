package com.csv.connector;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;


public class FileUtils {

   private static Logger log= LoggerFactory.getLogger(FileUtils.class);

    public static void MoveFile(File source, String dest){
        log.info("dest path:"+(dest+"/"+source.getName()));
            if(source.exists()){
                if(source.renameTo(new File(dest+"/"+source.getName()))){
                    log.info("moving of file from "+source+" to "+dest+" is success full");
                }
                else
                {
                    log.error("moving of file from "+source+" to "+dest+" is failed");
                }

            }

    }
}
