package test.testid;

import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;

public class SchemaMatching {
	public static final Map<String, String> mapProps = new HashMap<String, String>();
	public static final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
	
	public static void main(String[] args) throws UnsupportedEncodingException {
		long start = System.currentTimeMillis();	
		Set<String> datasets = new HashSet<String>();
		//datasets.add("http://dbpedia.org/sparql");
		//datasets.add("http://lod2.openlinksw.com/sparql");
		//datasets.add("https://query.wikidata.org/");
		//datasets.add("http://download.lodlaundromat.org/85d5a476b56fde200e770cefa0e5033c?type=hdt");
		datasets.add("85d5a476b56fde200e770cefa0e5033c?type.hdt");
		datasets.add("b7081efa178bc4ab3ff3a6ef5abac9b2?type.hdt");
		datasets.add("c66ff6bbdb8eeac9c17adbe7dfe4efd5?type.hdt");
		
		final String cSparql = "SELECT ?name ?area ?pop ?lat ?long WHERE {\n" + 
				" ?s a <http://dbpedia.org/ontology/City> ;\n" + 
				" <http://dbpedia.org/ontology/PopulatedPlace/areaTotal> ?area ;\n" + 
				" <http://www.w3.org/2000/01/rdf-schema#label> ?name ;\n" + 
				" <http://dbpedia.org/property/populationTotal> ?pop ;\n" + 
				" <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat ;\n" + 
				" <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long .\n" + 
				"FILTER ( regex(str(?name), \"Leipzig\" ) && langMatches(lang(?name),\"en\"))\n" + 
				"}";
		for (String ds : datasets) {
			mapProps.putAll(extractURIS(cSparql));
			mapPropsDs.put(ds, mapProps);
			//mapPropsDs.get(ds).putAll(getProps(mapProps, "http://dbpedia.org/sparql", ds));
			mapPropsDs.get(ds).putAll(getProps(mapProps, ds));
			String nSparql = replaceURIs(cSparql, mapPropsDs.get(ds));
			Set<String> ret = execSparql(nSparql,ds);
			System.out.println(ret);
		}
		
		long total = System.currentTimeMillis() - start;
		System.out.println("FINISHED in " + TimeUnit.MILLISECONDS.toMinutes(total) + " minutes");
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();
		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
			Runnable block = new Runnable() {
				public void run() {
					if (Util.isEndPoint(source)) {
						//ret.addAll(execQueryEndPoint(cSparql, source));
						ret.addAll(Util.execQueryEndPoint(cSparql, source));
					} else {
						ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
		}
		return ret;
	}

	private static String replaceURIs(String cSparql, Map<String, String> mProps) {
		for (Entry<String, String> entry : mProps.entrySet()) {
			cSparql = cSparql.replaceAll(entry.getKey(), entry.getValue());
		}
		return cSparql;
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 * Try to discover the Source dataset of the properties.
	 */
	private static Map<String, String> getProps(Map<String, String> mProps, String dsT) {
		String dsS = getDataset(mProps.keySet());
		return getProps(mProps, dsS, dsT);
	}
	
	private static String getDataset(Set<String> setURIs) {
		System.out.println("@TODO: USE WIMU TO DISCOVER THE DATASET OF THE URI, NOW IS USING http://dbpedia.org/sparql");
		Set<String> setDatasets = new HashSet<String>();
		for (String uri : setURIs) {
			String ds = getDataset(uri);
			setDatasets.add(ds);
		}
		String ret = getMostUpdatedDs(setDatasets);
		return ret;
	}

	private static String getMostUpdatedDs(Set<String> setDatasets) {
		
		return "http://dbpedia.org/sparql";
	}

	private static String getDataset(String uri) {
		return "TODO: Use wimu to find the dataset";
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 */
	private static Map<String, String> getProps(Map<String, String> mProps, String dsS, String dsT) {
		Map<String, String> mRet = new HashMap<String, String>();
		for (Entry<String, String> entry : mProps.entrySet()) {
			String prop = entry.getKey();
			String equivProp = getEquivProp(prop, dsS, dsT);
			mRet.put(prop, equivProp);
		}
		
		return mRet;
	}

	/*
	 * Get the equivalent property
	 * 1. Check if @propDs exists in dsT.
	 * 2. Compare the values from all properties of dsT.  
	 * 3. Using string similarity(Threshold=0.8), search in a set of all properties from dsT(values).
	 * 4. Using string similarity(Threshold=0.8), search in a set of all properties from dsT(names).
	 */
	private static String getEquivProp(String propDs, String dsS, String dsT) {
		//Check if @propDs exists in dsT.
		if(propExists(propDs, dsT)) {
			return propDs;
		}
		String pValue = getValueProp(propDs,dsS);
		
		/*
		 * Compare the values from all properties of dsT.
		 */
		String equivProp = searchValueProp(pValue,dsT); 
		if(equivProp != null) {
			return equivProp;
		}
		
		equivProp = similatySearch(pValue, SchemaMatching.mapPropValueDsT);
		if(equivProp != null) {
			return equivProp;
		}
		
		equivProp = similatySearch(propDs, SchemaMatching.mapPropValueDsT);
		if(equivProp != null) {
			return equivProp;
		}
		return null;
	}

	private static Map<String, String> extractURIS(String cSparql) throws UnsupportedEncodingException {
		Map<String, String> containedUrls = new HashMap<String, String>();
		
		String fixSparql = replacePrefixes(cSparql);
		
		String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
		Matcher urlMatcher = pattern.matcher(fixSparql);
		
		while (urlMatcher.find()) {
			String key = fixSparql.substring(urlMatcher.start(0), urlMatcher.end(0));
			containedUrls.put(key, null);
		}
		return containedUrls;
	}

	public static String replacePrefixes(String query) throws UnsupportedEncodingException{
		PrefixMapping pmap = PrefixMapping.Factory.create();
//		Map<String, String> mPrefixURI = getMapPrefix(query);
//		pmap.setNsPrefixes(mPrefixURI);
	    pmap.setNsPrefixes(PrefixMapping.Extended);
		Prologue prog = new Prologue();
	    prog.setPrefixMapping(pmap);
	    Query q = QueryFactory.parse(new Query(prog), query, null, null);
	    //Set Prefix Mapping
	    q.setPrefixMapping(pmap);
	    //remove PrefixMapping so the prefixes will get replaced by the full uris
	    q.setPrefixMapping(null);       
	    return q.serialize();
	}
}
