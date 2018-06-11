package cn.ac.iie.presto;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;


public class TestPresto {

	public static void main(String[] args) throws ClassNotFoundException, SQLException {
		// TODO Auto-generated method stub
		     Class.forName("com.facebook.presto.jdbc.PrestoDriver");  
	         Connection connection = DriverManager.getConnection("jdbc:presto://10.0.30.101:8808/hive/ifp_source","root",null);  ;    
	         Statement stmt = connection.createStatement();    
	         ResultSet rs = stmt.executeQuery("show tables"); 
//	         ResultSet rs = stmt.executeQuery("select * from tb_r_w_cache");
//	         ResultSet rs = stmt.executeQuery("select count(*),userIp,serverIp from tb_r_w_cache group by userIp,serverIp");
//	         select * from TB_R_W_CACHE where ds < '20170807' and ds > '20170804'
//	         ResultSet rs = stmt.executeQuery("select * from tb_r_w_cache where type='small' and host='download.ccb.com' limit 10");
//	         ResultSet rs = stmt.executeQuery("select * from TB_R_W_DNS_LOG limit 10");
//	         ResultSet rs = stmt.executeQuery("select * from TB_R_W_CACHE where ds < '20170807' and ds > '20170804'");
	         
	         while (rs.next()) {   
//	        	 System.out.println(rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3));
	        	 System.out.println(rs.getString(1));

//	        	 System.out.println(rs.getString(1)+"|"+rs.getString(2)+"|"+rs.getString(3)+"|"+rs.getString(4)+"|"+rs.getString(5)+"|"+rs.getString(6)+"|"+rs.getString(7)+"|"+rs.getString(8)+"|"+rs.getString(9)+"|"+rs.getString(10)+"|"+rs.getString(11)+"|"+rs.getString(12)+"|"+rs.getString(13)+"|"+rs.getString(14)+"|"+rs.getString(15)+"|"+rs.getString(16)+"|"+rs.getString(17)+"|"+rs.getString(18)+"|"+rs.getString(19)+"|"+rs.getString(20)+"|"+rs.getString(21)+"|"+rs.getString(22)+"|"+rs.getString(23)+"|"+rs.getString(24)+"|"+rs.getString(25)+"|"+rs.getString(26)+"|"+rs.getString(27)+"|"+rs.getString(28)+"|"+rs.getString(29)+"|"+rs.getString(30)+"|"+rs.getString(31)+"|"+rs.getString(32)+"|"+rs.getString(33)+"|"+rs.getString(34)+"|"+rs.getString(35)+"|"+rs.getString(36)+"|"+rs.getString(37)+"|"+rs.getString(38));
	         }    
	         rs.close();    
	         connection.close();    
	}

}
