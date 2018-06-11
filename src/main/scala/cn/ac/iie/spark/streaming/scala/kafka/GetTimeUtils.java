package cn.ac.iie.spark.streaming.scala.kafka;

import java.text.SimpleDateFormat;
import java.util.Calendar;

public  class GetTimeUtils {

    public String getTime(){
        SimpleDateFormat  dateFormat = new SimpleDateFormat("yyyyMMddHH:mm:ss");
        Calendar cal=Calendar.getInstance();
        cal.add(Calendar.MINUTE,-5);
        String current = dateFormat.format(cal.getTime());
       return  current;
    }

}
