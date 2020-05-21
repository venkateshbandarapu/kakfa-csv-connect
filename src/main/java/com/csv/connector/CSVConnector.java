package com.csv.connector;

import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVConnector extends SourceConnector {
    private Map<String, String> connectionProps;


    public void start(Map<String, String> map) {
        System.out.println("inside the source data connector..start() method");
        System.out.println("map is:"+map);
        this.connectionProps=map;

    }

    public Class<? extends Task> taskClass() {
        System.out.println("inside taskClass method");

        return CSVSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        List<Map<String,String>> props= new ArrayList<>();
        System.out.println("inside taskConfigs method");
        props.add(this.connectionProps);
        return  props;
    }

    public void stop() {

    }

    public ConfigDef config() {
        System.out.println("inside config() method");
        return new ConfigDef()
                .define("topic",ConfigDef.Type.STRING,ConfigDef.Importance.HIGH,"topic where the will be pushed");
            }

    public String version() {
        String result;
        final  String FALLBACK_VERSION = "0.0.0.0";

        try {
            result = this.getClass().getPackage().getImplementationVersion();

            if (result==null || result.equals("")) {
                result = FALLBACK_VERSION;
            }
        } catch (Exception ex) {

            result = FALLBACK_VERSION;
        }
        return null;
    }
}
