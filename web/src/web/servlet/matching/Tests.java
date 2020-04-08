package web.servlet.matching;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Tests {

	public static void main(String[] args) {
		testGetEdges();
	}

	public static void testGetEdges() {
		Map<String, Set<String>> mapEdges = new LinkedHashMap<String, Set<String>>();
		String sTest = "{http://purl.org/dc/terms/date=["
				+ "92e6476d73c8325b5b6d748f9f33254a.hdt, "
				+ "360893bfc33da07215e05ba634016e8d.hdt, "
				+ "dd5f8c65f7ddc4b327b3a2d235f9c656.hdt], "
				+ "http://purl.org/dc/terms/subject=["
				+ "aa92e6476d73c8325b5b6d748f9f33254a.hdt, "
				+ "bb360893bfc33da07215e05ba634016e8d.hdt]}";
		mapEdges.putAll(MatchingServlet.getEdges(sTest));
		System.out.println(mapEdges);
	}

}
