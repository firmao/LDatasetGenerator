package test.testid;

//https://github.com/BorderCloud/SPARQL-JAVA
import com.bordercloud.sparql.Endpoint;
import com.bordercloud.sparql.EndpointException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Set;

public class WikidataQuery {

	public static void main(String[] args) {
		//String endpointUrl = "https://query.wikidata.org/sparql";
		String endpointUrl = "http://lod2.openlinksw.com/sparql";
		String querySelect = "Select DISTINCT ?p where {?s ?p ?o}";

		try {
			HashMap data = retrieveData(endpointUrl, querySelect);
			printResult(data, 30);
		} catch (EndpointException eex) {
			eex.printStackTrace();
		}
	}

	public static HashMap<String, HashMap> retrieveData(String endpointUrl, String query) throws EndpointException {
		Endpoint sp = new Endpoint(endpointUrl, false);
		HashMap<String, HashMap> rs = sp.query(query);
		return rs;
	}

	public static void printResult(HashMap<String, HashMap> rs, int size) {
		for (String variable : (ArrayList<String>) rs.get("result").get("variables")) {
			System.out.print(String.format("%-" + size + "." + size + "s", variable) + " | ");
		}
		System.out.print("\n");
		for (HashMap value : (ArrayList<HashMap>) rs.get("result").get("rows")) {
			for (String variable : (ArrayList<String>) rs.get("result").get("variables")) {
				System.out.print(String.format("%-" + size + "." + size + "s", value.get(variable)) + " | ");
			}
			System.out.print("\n");
		}
	}
	
	public static Set<String> getResult(String cSparql) {
		String endpointUrl = "https://query.wikidata.org/sparql";
		HashMap<String, HashMap> rs = null;
		try {
			rs = retrieveData(endpointUrl, cSparql);
		} catch (EndpointException eex) {
			eex.printStackTrace();
		}
		
		final Set<String> ret = new LinkedHashSet<String>();
		if(rs == null) return ret;
		for (HashMap value : (ArrayList<HashMap>) rs.get("result").get("rows")) {
			for (String variable : (ArrayList<String>) rs.get("result").get("variables")) {
				//System.out.print(String.format("%-" + size + "." + size + "s", value.get(variable)) + " | ");
				ret.add(value.get(variable).toString());
			}
		}
		return ret;
	}
}
