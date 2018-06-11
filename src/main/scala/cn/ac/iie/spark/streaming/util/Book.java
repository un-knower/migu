package cn.ac.iie.spark.streaming.util;

public class Book {


        public String name;
        public String createtime;

    public Book(String name, String createtime) {
    }

    public Book(){

    }

    public String getDate(){
            return   this.createtime;
        }

        public void setDate(String val){
            this.createtime=val;
        }


}
