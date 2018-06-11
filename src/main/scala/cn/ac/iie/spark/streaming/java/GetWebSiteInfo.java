package cn.ac.iie.spark.streaming.java;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.FileUtils;

public class GetWebSiteInfo {

	// private static String path = Config.getPath();
	private static BufferedReader br;
	public static List<String> list = new ArrayList<String>();

	public static Map<String, String> readToMap(String path) throws IOException {
		Map<String, String> map = new HashMap<String, String>();
		if (path == null || "".equals(path)) {
			throw new RuntimeException("文件路径为空");
		}
		// String filepath=
		// UrlFromTxtReader.class.getClassLoader().getResource(path).getFile();
		br = new BufferedReader(new InputStreamReader(new FileInputStream(path), "utf-8"));
		String line = null;
		while ((line = br.readLine()) != null) {
			String[] tmps = line.split("#", 10);
			String webInfo = tmps[0] + "|" + tmps[1]+"|"+tmps[2]+"|"+tmps[3]+"|"+tmps[4]+"|"+tmps[5];
			if (tmps[6] != null && !tmps[6].equals("")) {
				String[] strs = tmps[6].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}

			} else if (tmps[7] != null && !tmps[7].equals("")) {
				String[] strs = tmps[7].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}
			} else if (tmps[8] != null && !tmps[8].equals("")) {
				String[] strs = tmps[8].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}
			}
		}
		return map;
	}

	public static Map<String, String> readMap(File f) throws IOException {
		list = FileUtils.readLines(f, "UTF-8");
		Map<String, String> map = new HashMap<String, String>();
		for (String line : list) {
			String[] tmps = line.split("#", 10);
			String webInfo = tmps[0] + "|" + tmps[1]+"|"+tmps[2]+"|"+tmps[3]+"|"+tmps[4]+"|"+tmps[5];
//			System.out.println(webInfo);
			if (tmps[6] != null && !tmps[6].equals("")) {
				String[] strs = tmps[6].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}

			} else if (tmps[7] != null && !tmps[7].equals("")) {
				String[] strs = tmps[7].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}
			} else if (tmps[8] != null && !tmps[8].equals("")) {
				String[] strs = tmps[8].split("\\$");
				for (String str : strs) {
					map.put(str, webInfo);
				}
			}
		}
		return map;
	}
	
	public static void main(String[] args) throws IOException {
		String url = ".qqvideo.tc.qq.com";
		Map<String, String> urlMap = new HashMap<String, String>();
		File in = new File("D:\\Users\\zhangmin\\workspace-mars-64\\rmw-piggybank\\src\\main\\resources\\url-website-mapping.txt");
		urlMap = GetWebSiteInfo.readMap(in);
//		System.out.println(urlMap.size());
		for (Entry<String, String> entry : urlMap.entrySet()) {
//			System.out.println(entry.getKey()+"   value:="+entry.getValue());
			if (url.contains(entry.getKey()) || url.equals(entry.getKey())) {
				System.out.println("result===" + entry.getValue());
			}
		}
		// System.out.println(path);
		// System.out.println(UrlFromTxtReader.class.getClassLoader().getResource(path).getFile());
	}
}
