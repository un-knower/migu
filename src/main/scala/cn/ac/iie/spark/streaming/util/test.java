package cn.ac.iie.spark.streaming.util;


import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;


public class test {
    public static void main(String[]args){


        List<Book> books = new ArrayList<Book>();
        Book book1=new Book("1","2");
        books.add(book1);


        //for(){
        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyyMMdd HH:mm");
        String a=dateFormat.format(new Date());
        System.out.println(books.size());

        //  }

    }



}
