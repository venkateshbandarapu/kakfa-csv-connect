package com.csv.connector;

import org.apache.commons.logging.Log;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CSVConnector extends SourceConnector {

    Logger log= LoggerFactory.getLogger(CSVConnector.class);
    private Map<String, String> connectionProps;


    public void start(Map<String, String> map) {
        log.info("inside the source data connector..start() method");
        log.info("map is:"+map);
        this.connectionProps=map;

    }

    public Class<? extends Task> taskClass() {
        log.info("inside taskClass method");

        return CSVSourceTask.class;
    }

    public List<Map<String, String>> taskConfigs(int i) {
        List<Map<String,String>> props= new ArrayList<>();
        log.info("inside taskConfigs method");
        props.add(this.connectionProps);
        return  props;
    }

    public void stop() {

    }

    public ConfigDef config() {
        log.info("inside config() method");
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
