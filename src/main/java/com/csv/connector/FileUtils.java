package com.csv.connector;

import java.io.File;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;

public class FileUtils {

    public static void MoveFile(File source, String dest){
        System.out.println("dest path:"+(dest+"/"+source.getName()));
            if(source.exists()){
                if(source.renameTo(new File(dest+"/"+source.getName()))){
                    System.out.println("moving of file from "+source+" to "+dest+" is success full");
                }
                else
                {
                    System.out.println("moving of file from "+source+" to "+dest+" is failed");
                }

            }

    }
}
