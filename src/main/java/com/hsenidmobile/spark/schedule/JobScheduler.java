package com.hsenidmobile.spark.schedule;

import com.hsenidmobile.spark.logread.WriteFile;

import java.util.Timer;

public class JobScheduler {

    public static void main(String[] args) {

        Timer timer = new Timer();
        WriteFile writeFile = new WriteFile();



        Thread updateFileThread = new Thread(new Runnable() {
            @Override
            public void run() {
                timer.scheduleAtFixedRate(writeFile, 1000,5000);
            }
        });

        Thread startStreamingThread = new Thread(new Runnable() {
            @Override
            public void run() {

            }
        });
    }

}
