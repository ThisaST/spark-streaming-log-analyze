package com.hsenidmobile.spark.logread;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

public class ApplicationConf {
    private Config conf = ConfigFactory.parseResources("TypeSafeConfig.conf");

    private String appName = getConf().getString("application.appName");
    private String master = getConf().getString("application.master");
    private int duration = getConf().getInt("application.duration");
    private String hiveConf = getConf().getString("application.hiveConf");
    private String thriftConf =  getConf().getString("application.thriftConf");
    private int shuffle = getConf().getInt("application.shuffle");

    public Config getConf() {
        return conf;
    }

    public String getAppName() {
        return appName;
    }

    public String getMaster() {
        return master;
    }

    public int getDuration() {
        return duration;
    }

    public String getHiveConf() {
        return hiveConf;
    }

    public String getThriftConf() {
        return thriftConf;
    }

    public int getShuffle() {
        return shuffle;
    }
}
