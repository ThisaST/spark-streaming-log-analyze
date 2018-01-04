package com.hsenidmobile.spark.logread;

import org.apache.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.TimerTask;



public class WriteFile extends TimerTask{

    private Logger logger = Logger.getLogger(WriteFile.class);

    @Override
    public void run() {
        int count = 0;

            try {

                count ++;
                String content = "0171012180029044"+ count +"|2017-10-18 18:00:29 117|SPP_000948|hsenid217@gmail.com|APP_005541|Winter|live|" +
                        "94753456889||sms|smpp|||||mo|subscription|unknown||||||||registration|S1000|Request was successfully processed.|" +
                        "success|winter|77000|airtel||percentageFromMonthlyRevenue|70||||S1000|Success|||0|||||||||||daily|||||||" + "\n";

                File file = new File("/home/hsenid/Cloudera/created");

                // if file doesnt exists, then create it
                if (!file.exists()) {
                    file.createNewFile();
                }


                FileWriter fileWriter = new FileWriter(file.getAbsoluteFile(), true);
                BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
                bufferedWriter.write(content);
                bufferedWriter.close();

                logger.info("Done : " + count);

            } catch (IOException e) {
                e.printStackTrace();
            }
    }
}
