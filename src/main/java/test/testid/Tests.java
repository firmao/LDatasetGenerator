package web.servlet.matching;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;

public class Tests {

	public static void main(String[] args) throws IOException {
		String cSPARQL = "Select * where {?s ?p ?o} limit 10";
		String source = "http://141.57.11.86:8082/dirHDTLaundromat/decompressed/76/7682278f9dd608f09c4c073a915e58a1/7682278f9dd608f09c4c073a915e58a1.hdt";
		System.out.println("OUT: " + Util.execQueryHDTRes(cSPARQL, source, -1));
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
