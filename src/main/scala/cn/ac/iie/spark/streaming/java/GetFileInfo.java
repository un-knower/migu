package cn.ac.iie.spark.streaming.java;

import java.util.regex.Pattern;

public class GetFileInfo implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String getUserAgent(String ua) {
		// TODO Auto-generated method stub
		if (ua != null && !"".equals(ua)) {
			ua = ua.toLowerCase();
			String model = "windows PC";
			// String url_type = getFileType(ua);
			if (ua.indexOf("android") >= 0) {
				if (Pattern.matches(".*(android).*?(mobile?).*", ua)) {
					model = "安卓手机（普通浏览器）";
				} else {
					model = "安卓pad";
				}
			} else if (ua.indexOf("fennec") >= 0) {
				model = "Android Firefox手机版Fennec";
			} else if (ua.indexOf("juc") >= 0) {
				model = "Android UC For android";
			} else if (ua.indexOf("iphone") >= 0) {
				model = "苹果手机";
			} else if (ua.indexOf("ipad") >= 0) {
				model = "iPad";
			} else if (ua.indexOf("blackberry") >= 0) {
				model = "BlackBerry";
			} else if (ua.indexOf("nokian97") >= 0) {
				model = "NokiaN97";
			} else if (ua.indexOf("windows phone") >= 0) {
				model = "Windows Phone";
			}
			return model;
		} else {
			return "windows PC";
		}

	}

	public String getUrlType(String url) {
		String url_type = "0";

		url = url.toLowerCase();
		if (Pattern.matches(".*(\\.avi|\\.rmvb|\\.mpeg|\\.mpg|\\.mov|\\.movie|\\.rm|\\.mp4|\\.flv|\\.f4v|\\.hlv|\\.m4v|\\.letv|\\.pfv|\\.ts|\\.wmv|\\.mkv)([^a-zA-Z0-9])?.*",url)) {
			url_type = "1";
		} else if (Pattern.matches(".*(\\.rar|\\.exe|\\.zip|\\.bz2|\\.z|\\.iso|\\.mpga|\\.ra|\\.wmp|\\.3gp|\\.tar|\\.arj|\\.gzip|\\.gz|\\.ipa|\\.cab|\\.msu|\\.ogg|\\.kprar|\\.aac|\\.tgz|\\.rtp|\\.spk|\\.apk|\\.deb|\\.pak|\\.xazp|\\.patch|\\.rp|\\.package|\\.pcf|\\.gmz|\\.mpq)([^a-zA-Z0-9])?.*",url)) {
			url_type = "2";
		} else if (Pattern.matches(".*(\\.mp3|\\.wma|\\.m4a|\\.m4r)([^a-zA-Z0-9])?.*", url)) {
			url_type = "3";
		}
		return url_type;
	}

	public static void main(String[] args) {
		GetFileInfo fileInfo = new GetFileInfo();
		String ua = "Huawei U8800    Android 2.3.3   Baidu 2.2   Mozilla/5.0 (Linux; U; Android 2.3.5; zh-cn) AppleWebKit/530.17 (KHTML, like Gecko) FlyFlow/2.2 Version/4.0 Mobile Safari/530.17        有用(0)";
//		String url = "http://d2.sina.com.cn/litong/zhitou/sinaads/src/spec/sinaads_ck.rmvba";
		String url = "http://103.41.140.79/youku/6774F6EC6693A7AA0CF6E65DC/030002070056FAA80929BE03BAF2B13843A00C-DE6C-DBEB-D3C2-8D95B6BF77F6.wma?&start=122";

		System.out.println("文件类型: "+fileInfo.getUrlType(url));

		System.out.println("UserAgent: "+fileInfo.getUserAgent(ua));
	}

}
