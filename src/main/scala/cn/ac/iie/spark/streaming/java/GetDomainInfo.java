package cn.ac.iie.spark.streaming.java;

import java.net.MalformedURLException;
import java.net.URL;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class GetDomainInfo implements java.io.Serializable{

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	public String getDomainInfo(String domain, String flag) {
		String results[] = new String[3];
		String result = "";

		results = getDomainAndlevels(domain);
		if ("domain".equals(flag)) {
			result = results[0];
		} else if ("lever".equals(flag)) {
			result = results[1];
		} else if ("cname".equals(flag)) {
			result = results[2];
		} else {
			result = results[0];
		}
		return result;
	}

	private static String[] getDomainAndlevels(String url) {
		String results[] = new String[3];
		try {

			// Pattern p =
			// Pattern.compile("[^.]*\\.(com|cn|edu|net|org|biz|info|gov|pro|name|museum|coop|aero|idv|tv|co|tm|lu|me|cc|mobi|fm|sh|la|dj|hu|so|us|hk|to|tw|ly|tl|in|es|jp|mx|vc|io|am|sc|cm|pw|de|sg|cd|fr|arpa|asia|im|tel|mil|jobs|travel)(\\..+)?$",Pattern.CASE_INSENSITIVE);
			Pattern p = Pattern.compile(
					"[^.]*\\.(top|com|xyz|xin|vip|win|red|com|com|net|org|wang|gov|edu|mil|co|biz|name|info|mobi|pro|travel|club|museum|int|aero|post|rec|asia|au|ad|ae|af|ag|ai|al|am|an|ao|aa|ar|as|at|au|aw|az|ba|bb|bd|be|bf|bg|bh|bi|bj|bm|bn|bo|br|bs|bt|bv|bw|by|bz|ca|cc|cf|cd|ch|ci|ck|cl|cm|cn|co|cq|cr|cu|cv|cx|cy|cz|de|dj|dk|dm|do|dz|ec|ee|eg|eh|er|es|et|ev|fi|fj|fk|fm|fo|fr|ga|gd|ge|gf|gg|gh|gi|gl|gm|gn|gp|gr|gs|gt|gu|gw|gy|hk|hm|hn|hr|ht|hu|id|ie|il|im|in|io|iq|ir|is|it|jm|jo|jp|je|ke|kg|kh|ki|km|kn|kp|kr|kw|ky|kz|la|lb|lc|li|lk|lr|ls|lt|lu|lv|ly|ma|mc|md|me|mg|mh|mk|ml|mm|mn|mo|mp|mq|mr|ms|mt|mu|mv|mw|mx|my|mz|na|nc|ne|nf|ng|ni|nl|no|np|nr|nt|nu|nz|om|qa|pa|pe|pf|pg|ph|pk|pl|pm|pn|pr|pt|pw|py|re|rs|ro|ru|rw|sa|sb|sc|sd|se|sg|sh|si|sj|sk|sl|sm|sn|so|sr|st|sv|su|sy|sz|sx|tc|td|tf|tg|th|tj|tk|tl|tm|tn|to|tr|tt|tv|tw|tz|ua|ug|uk|um|us|uy|uz|va|vc|ve|vg|vi|vn|vu|wf|ws|ye|yt|za|zm|zw|arts|com|edu|firm|gov|info|net|nom|org|rec|store|web)(\\..+)?$",
					Pattern.CASE_INSENSITIVE);
			Matcher matcher = p.matcher(url);
			if (matcher.find()) {
				results[0] = matcher.group().toString();
				results[2] = "0";
			}
			if (results[0] == null) {
				results[0] = "";
			}
			String newstr[] = results[0].split("\\.");
			int num = 0;
			int a = 0;
			boolean flag = false;
			boolean flag1 = false;
			boolean flag2 = false;
			String first = "";
			String last = "";
			for (int i = 0; i < newstr.length; i++) {

				if (newstr[i].equals("top") || newstr[i].equals("com") || newstr[i].equals("xyz")
						|| newstr[i].equals("xin") || newstr[i].equals("vip") || newstr[i].equals("win")
						|| newstr[i].equals("red") || newstr[i].equals("com") || newstr[i].equals("com")
						|| newstr[i].equals("net") || newstr[i].equals("org") || newstr[i].equals("wang")
						|| newstr[i].equals("gov") || newstr[i].equals("edu") || newstr[i].equals("mil")
						|| newstr[i].equals("co") || newstr[i].equals("biz") || newstr[i].equals("name")
						|| newstr[i].equals("info") || newstr[i].equals("mobi") || newstr[i].equals("pro")
						|| newstr[i].equals("travel") || newstr[i].equals("club") || newstr[i].equals("museum")
						|| newstr[i].equals("int") || newstr[i].equals("aero") || newstr[i].equals("post")
						|| newstr[i].equals("rec") || newstr[i].equals("asia") || newstr[i].equals("au")
						|| newstr[i].equals("ad") || newstr[i].equals("ae") || newstr[i].equals("af")
						|| newstr[i].equals("ag") || newstr[i].equals("ai") || newstr[i].equals("al")
						|| newstr[i].equals("am") || newstr[i].equals("an") || newstr[i].equals("ao")
						|| newstr[i].equals("aa") || newstr[i].equals("ar") || newstr[i].equals("as")
						|| newstr[i].equals("at") || newstr[i].equals("au") || newstr[i].equals("aw")
						|| newstr[i].equals("az") || newstr[i].equals("ba") || newstr[i].equals("bb")
						|| newstr[i].equals("bd") || newstr[i].equals("be") || newstr[i].equals("bf")
						|| newstr[i].equals("bg") || newstr[i].equals("bh") || newstr[i].equals("bi")
						|| newstr[i].equals("bj") || newstr[i].equals("bm") || newstr[i].equals("bn")
						|| newstr[i].equals("bo") || newstr[i].equals("br") || newstr[i].equals("bs")
						|| newstr[i].equals("bt") || newstr[i].equals("bv") || newstr[i].equals("bw")
						|| newstr[i].equals("by") || newstr[i].equals("bz") || newstr[i].equals("ca")
						|| newstr[i].equals("cc") || newstr[i].equals("cf") || newstr[i].equals("cd")
						|| newstr[i].equals("ch") || newstr[i].equals("ci") || newstr[i].equals("ck")
						|| newstr[i].equals("cl") || newstr[i].equals("cm") || newstr[i].equals("cn")
						|| newstr[i].equals("co") || newstr[i].equals("cq") || newstr[i].equals("cr")
						|| newstr[i].equals("cu") || newstr[i].equals("cv") || newstr[i].equals("cx")
						|| newstr[i].equals("cy") || newstr[i].equals("cz") || newstr[i].equals("de")
						|| newstr[i].equals("dj") || newstr[i].equals("dk") || newstr[i].equals("dm")
						|| newstr[i].equals("do") || newstr[i].equals("dz") || newstr[i].equals("ec")
						|| newstr[i].equals("ee") || newstr[i].equals("eg") || newstr[i].equals("eh")
						|| newstr[i].equals("er") || newstr[i].equals("es") || newstr[i].equals("et")
						|| newstr[i].equals("ev") || newstr[i].equals("fi") || newstr[i].equals("fj")
						|| newstr[i].equals("fk") || newstr[i].equals("fm") || newstr[i].equals("fo")
						|| newstr[i].equals("fr") || newstr[i].equals("ga") || newstr[i].equals("gd")
						|| newstr[i].equals("ge") || newstr[i].equals("gf") || newstr[i].equals("gg")
						|| newstr[i].equals("gh") || newstr[i].equals("gi") || newstr[i].equals("gl")
						|| newstr[i].equals("gm") || newstr[i].equals("gn") || newstr[i].equals("gp")
						|| newstr[i].equals("gr") || newstr[i].equals("gs") || newstr[i].equals("gt")
						|| newstr[i].equals("gu") || newstr[i].equals("gw") || newstr[i].equals("gy")
						|| newstr[i].equals("hk") || newstr[i].equals("hm") || newstr[i].equals("hn")
						|| newstr[i].equals("hr") || newstr[i].equals("ht") || newstr[i].equals("hu")
						|| newstr[i].equals("id") || newstr[i].equals("ie") || newstr[i].equals("il")
						|| newstr[i].equals("im") || newstr[i].equals("in") || newstr[i].equals("io")
						|| newstr[i].equals("iq") || newstr[i].equals("ir") || newstr[i].equals("is")
						|| newstr[i].equals("it") || newstr[i].equals("jm") || newstr[i].equals("jo")
						|| newstr[i].equals("jp") || newstr[i].equals("je") || newstr[i].equals("ke")
						|| newstr[i].equals("kg") || newstr[i].equals("kh") || newstr[i].equals("ki")
						|| newstr[i].equals("km") || newstr[i].equals("kn") || newstr[i].equals("kp")
						|| newstr[i].equals("kr") || newstr[i].equals("kw") || newstr[i].equals("ky")
						|| newstr[i].equals("kz") || newstr[i].equals("la") || newstr[i].equals("lb")
						|| newstr[i].equals("lc") || newstr[i].equals("li") || newstr[i].equals("lk")
						|| newstr[i].equals("lr") || newstr[i].equals("ls") || newstr[i].equals("lt")
						|| newstr[i].equals("lu") || newstr[i].equals("lv") || newstr[i].equals("ly")
						|| newstr[i].equals("ma") || newstr[i].equals("mc") || newstr[i].equals("md")
						|| newstr[i].equals("me") || newstr[i].equals("mg") || newstr[i].equals("mh")
						|| newstr[i].equals("mk") || newstr[i].equals("ml") || newstr[i].equals("mm")
						|| newstr[i].equals("mn") || newstr[i].equals("mo") || newstr[i].equals("mp")
						|| newstr[i].equals("mq") || newstr[i].equals("mr") || newstr[i].equals("ms")
						|| newstr[i].equals("mt") || newstr[i].equals("mu") || newstr[i].equals("mv")
						|| newstr[i].equals("mw") || newstr[i].equals("mx") || newstr[i].equals("my")
						|| newstr[i].equals("mz") || newstr[i].equals("na") || newstr[i].equals("nc")
						|| newstr[i].equals("ne") || newstr[i].equals("nf") || newstr[i].equals("ng")
						|| newstr[i].equals("ni") || newstr[i].equals("nl") || newstr[i].equals("no")
						|| newstr[i].equals("np") || newstr[i].equals("nr") || newstr[i].equals("nt")
						|| newstr[i].equals("nu") || newstr[i].equals("nz") || newstr[i].equals("om")
						|| newstr[i].equals("qa") || newstr[i].equals("pa") || newstr[i].equals("pe")
						|| newstr[i].equals("pf") || newstr[i].equals("pg") || newstr[i].equals("ph")
						|| newstr[i].equals("pk") || newstr[i].equals("pl") || newstr[i].equals("pm")
						|| newstr[i].equals("pn") || newstr[i].equals("pr") || newstr[i].equals("pt")
						|| newstr[i].equals("pw") || newstr[i].equals("py") || newstr[i].equals("re")
						|| newstr[i].equals("rs") || newstr[i].equals("ro") || newstr[i].equals("ru")
						|| newstr[i].equals("rw") || newstr[i].equals("sa") || newstr[i].equals("sb")
						|| newstr[i].equals("sc") || newstr[i].equals("sd") || newstr[i].equals("se")
						|| newstr[i].equals("sg") || newstr[i].equals("sh") || newstr[i].equals("si")
						|| newstr[i].equals("sj") || newstr[i].equals("sk") || newstr[i].equals("sl")
						|| newstr[i].equals("sm") || newstr[i].equals("sn") || newstr[i].equals("so")
						|| newstr[i].equals("sr") || newstr[i].equals("st") || newstr[i].equals("sv")
						|| newstr[i].equals("su") || newstr[i].equals("sy") || newstr[i].equals("sz")
						|| newstr[i].equals("sx") || newstr[i].equals("tc") || newstr[i].equals("td")
						|| newstr[i].equals("tf") || newstr[i].equals("tg") || newstr[i].equals("th")
						|| newstr[i].equals("tj") || newstr[i].equals("tk") || newstr[i].equals("tl")
						|| newstr[i].equals("tm") || newstr[i].equals("tn") || newstr[i].equals("to")
						|| newstr[i].equals("tr") || newstr[i].equals("tt") || newstr[i].equals("tv")
						|| newstr[i].equals("tw") || newstr[i].equals("tz") || newstr[i].equals("ua")
						|| newstr[i].equals("ug") || newstr[i].equals("uk") || newstr[i].equals("um")
						|| newstr[i].equals("us") || newstr[i].equals("uy") || newstr[i].equals("uz")
						|| newstr[i].equals("va") || newstr[i].equals("vc") || newstr[i].equals("ve")
						|| newstr[i].equals("vg") || newstr[i].equals("vi") || newstr[i].equals("vn")
						|| newstr[i].equals("vu") || newstr[i].equals("wf") || newstr[i].equals("ws")
						|| newstr[i].equals("ye") || newstr[i].equals("yt") || newstr[i].equals("za")
						|| newstr[i].equals("zm") || newstr[i].equals("zw") || newstr[i].equals("arts")
						|| newstr[i].equals("com") || newstr[i].equals("edu") || newstr[i].equals("firm")
						|| newstr[i].equals("gov") || newstr[i].equals("info") || newstr[i].equals("net")
						|| newstr[i].equals("nom") || newstr[i].equals("org") || newstr[i].equals("rec")
						|| newstr[i].equals("store") || newstr[i].equals("web")) {
					num++;
					flag1 = true;
					last = newstr[i];
					if (i == newstr.length - 1) {
						flag2 = true;
					}
				} else {
					flag1 = false;
				}
				if (num == 1 && flag1) {
					a = i;
					first = newstr[i];
				}
				if ((num == 2) && (i - a == 1)) {
					flag = true;
					first = first + "." + newstr[i];
				}

			}
			if (results[0].contains("akadns.net") || results[0].contains("edgekey.net")
					|| results[0].contains("akamaiedge.net") || results[0].contains("hubspot.net")
					|| results[0].contains("cloudfront.net") || results[0].contains("cdngs.net")
					|| results[0].contains("cdngc.net") || results[0].contains("lxdns.com")
					|| results[0].contains("wscdns.com") || results[0].contains("cdn20.com")
					|| results[0].contains("fastcdn.com") || results[0].contains("fwcdn.net")
					|| results[0].contains("fwdns.net") || results[0].contains("cloudcdn.net")
					|| results[0].contains("ccgslb.net") || results[0].contains("chinacache.net")
					|| results[0].contains("llnwd.net") || results[0].contains("edgecastcdn.net")) {
				results[2] = "cname";
				results[0] = results[0].split(first)[0] + first;

			} else if ((num > 2) || (num == 2 && !flag)) {
				if (results[0].split(first).length > 2) {

					String newurl = results[0].split(first)[1] + first + results[0].split(first)[2];

					results = getDomainAndlevels(newurl);
				} else if (flag2 && first.equals(last)) {
					String newurl = results[0].split(first)[1] + first;

					results = getDomainAndlevels(newurl);
				} else {
					results = getDomainAndlevels(results[0].split(first)[1]);
				}

			}

			String temp[] = url.split(results[0]);
			if (temp.length != 0) {
				results[1] = Integer.toString((temp[0].split("\\.").length + 1));
			} else {
				results[1] = "1";
			}
			if (results[0].equals("")) {
				results[0] = "-1";
				results[1] = "-1";
				results[2] = "-1";
			}
			return results;
		} catch (Exception e) {
			results[0] = "-1";
			results[1] = "-1";
			results[2] = "-1";
			return results;
		}

	}

	public boolean validDomain(String domain) {
		boolean flag = false;
//		String result = "0";

		if (domain.endsWith(".") || domain.endsWith("?"))
			domain = domain.substring(0, domain.length() - 1);
		if (domain.length() <= 256) {
			Pattern pattern = Pattern.compile("^[0-9a-zA-Z]+[0-9a-zA-Z\\.-]*\\.[a-zA-Z]{2,6}$");
			Matcher matcher = pattern.matcher(domain);
			flag = matcher.matches();
//			if (flag) {
//				result = domain;
//			}
		}
		return flag;
	}
	
	public static String getHostByUrl(String curl) {
		// TODO Auto-generated method stub
		String q = "";
		URL url = null;
		try {
			url = new URL(curl);
			q = url.getHost();
		} catch (MalformedURLException e) {
		}
		url = null;
		return q;
	}

	public static void main(String[] args) {
//		String curl = "http://wn.pos.baidu.com/adx.php?c=d25pZD04ZGMxZTNjNTQ5ZTExYWQyAHM9OGRjMWUzYzU0OWUxMWFkMgB0PTE0NjY0Mzc4ODAAc2U9MgBidT00AHByaWNlPVYyZ1EtQUFPS0dCN2pFcGdXNUlBOGpLamNTcElib29ORlR4UkhBAGNoYXJnZV9wcmljZT0xAHNoYXJpbmdfcHJpY2U9MTAwMAB3aW5fZHNwPTQAY2htZD0xAGJkaWQ9AGNwcm9pZD0Ad2Q9MABiY2htZD0wAHY9MQBpPTljNjQ3ZDZm&ext=ZmNfdWlkPSZmY19paWQ9NjAwNiZmY190az1MVV9TVFJBX1NUSUQmZmNfZXg9MTEzMjAmZmNfbG89bmpfbHVfc3RyYQ";
//		System.out.println("11111111111111==="+getHostByUrl(curl));
		String domain = "s4.qhimg.com";
		String flag = "domain";
		// String flag="lever";
		GetDomainInfo getdomain = new GetDomainInfo();
		System.out.println(getdomain.validDomain(domain)+"==="+getdomain.getDomainInfo(domain, flag));

	}
}
