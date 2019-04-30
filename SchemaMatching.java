package test.testid;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Collection;
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
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class SchemaMatching {
	
	public static final Map<String, String> mapPropValueDsT = new HashMap<String, String>();
	
	public static void main(String[] args) throws IOException, InterruptedException {
		final Map<String, String> mapProps = new HashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
		
		long start = System.currentTimeMillis();	
		Set<String> datasets = new HashSet<String>();
		//datasets.add("http://dbpedia.org/sparql");
		//datasets.add("http://lod2.openlinksw.com/sparql");
		//datasets.add("https://query.wikidata.org/");
		//datasets.add("http://download.lodlaundromat.org/85d5a476b56fde200e770cefa0e5033c?type=hdt");
		datasets.add("85d5a476b56fde200e770cefa0e5033c?type.hdt");
		//datasets.add("b7081efa178bc4ab3ff3a6ef5abac9b2?type.hdt");
		datasets.add("b7081efa178bc4ab3ff3a6ef5abac9b2.hdt");
		datasets.add("c66ff6bbdb8eeac9c17adbe7dfe4efd5?type.hdt");
		
//		final String cSparql = "SELECT ?name ?area ?pop ?lat ?long WHERE {\n" + 
//				" ?s a <http://dbpedia.org/ontology/City> ;\n" + 
//				" <http://dbpedia.org/ontology/PopulatedPlace/areaTotal> ?area ;\n" + 
//				" <http://www.w3.org/2000/01/rdf-schema#label> ?name ;\n" + 
//				" <http://dbpedia.org/property/populationTotal> ?pop ;\n" + 
//				" <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat ;\n" + 
//				" <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long .\n" + 
//				"FILTER ( regex(str(?name), \"Leipzig\" ) && langMatches(lang(?name),\"en\"))\n" + 
//				"}";
		
		final String cSparql = "Select * where {<http://citeseer.rkbexplorer.com/id/resource-CS116606> <http://www.aktors.org/ontology/portal#has-author> ?o}";
		
		for (String ds : datasets) {
			mapProps.putAll(extractURIS(cSparql));
			mapPropsDs.put(ds, mapProps);
			//mapPropsDs.get(ds).putAll(getProps(mapProps, "http://dbpedia.org/sparql", ds));
			Map<String, String> props = getProps(mapProps, ds);
			if(props == null) {
				System.out.println("Problem ds: " + ds);
				continue;
			}
			mapPropsDs.get(ds).putAll(props);
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
	private static Map<String, String> getProps(Map<String, String> mProps, String dsT) throws IOException, InterruptedException {
		String dsS = getDataset(mProps.keySet());
		return getProps(mProps, dsS, dsT);
	}
	
	private static String getDataset(Set<String> setURIs) throws InterruptedException, IOException {
		Set<String> setDatasets = new HashSet<String>();
		for (String uri : setURIs) {
			setDatasets.addAll(WimuUtil.getDsWIMUs(uri));
		}
		String ret = getMostUpdatedDs(setDatasets);
		return ret;
	}
	
	public static String getMostUpdatedDs(Set<String> datasets) {
		String cSparql = "Select ?date where{\n" + 
				"?s <http://purl.org/dc/terms/modified> ?date\n" + 
				"} order by DESC(?date) limit 1";
		final Map<String, String> mDsTimeStamp = new HashMap<String, String>();
		Set<String> ret = new HashSet<String>();
		String sRet = null;
		
		for (String source : datasets) {
		//lstSources.parallelStream().forEach( source -> {
			try {
				TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
				Runnable block = new Runnable() {
					public void run() {
						
						if (Util.isEndPoint(source)) {
							//ret.addAll(execQueryEndPoint(cSparql, source));
							ret.addAll(Util.execQueryEndPoint(cSparql, source, true, -1));
						} else {
							ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
						}
						String sRet = null;
						String previous = mDsTimeStamp.get(source);
						for (String timeStamp : ret) {
							if(Util.isGreaterDate(timeStamp, previous)) {
								sRet = source;
							}
							previous = timeStamp;
							mDsTimeStamp.put(sRet, previous);
							Main.goodSources.add(source);
						}
						ret.clear();
					}
				};
				timeoutBlock.addBlock(block);// execute the runnable block
			} catch (Throwable e) {
				System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
			}
		}
		for(String sret : mDsTimeStamp.keySet()) {
			sRet = sret;
		}
		
		return sRet;
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 */
	private static Map<String, String> getProps(Map<String, String> mProps, String dsS, String dsT) throws IOException {
		Map<String, String> mRet = new HashMap<String, String>();
		
		if(!Main.goodSources.contains(dsS)) {
			return null;
		}
		
		for (Entry<String, String> entry : mProps.entrySet()) {
			String prop = entry.getKey();
			Set<String> equivProps = getEquivProp(prop, dsS, dsT);
			if(equivProps != null) {
				for (String equivProp : equivProps) {
					mRet.put(prop, equivProp);
				}
			}
		}
		
		return mRet;
	}

	/*
	 * Get the equivalent property
	 * 1. Check if @propDs exists in dsT.
	 * 2. Do the best practice that is using the owl:equivalentProperty.
	 * 3. Compare the values from all properties of dsT.  
	 * 4. Using string similarity(Threshold=0.8), search in a set of all properties from dsT(values).
	 * 5. Using string similarity(Threshold=0.8), search in a set of all properties from dsT(names).
	 */
	private static Set<String> getEquivProp(String propDs, String dsS, String dsT) throws IOException {
		Set<String> equivProps = new HashSet<String>();
		//Check if @propDs exists in dsT.
		if(propExists(propDs, dsT)) {
			equivProps.add(propDs);
			return equivProps;
		}
		String pValue = getValueProp(propDs,dsS);
		if(pValue == null) {
			return null;
		}
		
		String cSparql = "SELECT * WHERE {<"+propDs+"> <https://www.w3.org/2002/07/owl#equivalentProperty> ?o}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
		
		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentProperty> <"+propDs+">}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
		
		cSparql = "SELECT * WHERE {<"+propDs+"> <https://www.w3.org/2002/07/owl#equivalentClass> ?o}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
		
		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentClass> <"+propDs+">}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
		
		if(equivProps.size() > 0) {
			return equivProps;
		}
		/*
		 * Compare the values from all properties of dsT.
		 */
		String equivProp = searchValueProp(pValue,dsT); 
		if(equivProp != null) {
			equivProps.add(equivProp);
			return equivProps;
		}
		
		equivProps.addAll(similaritySearchValue(pValue, dsT));
		if(equivProps.size() > 0) {
			return equivProps;
		}
		
		equivProps.addAll(similaritySearchProp(propDs, dsT));
		if(equivProps.size() > 0) {
			return equivProps;
		}
		System.out.println("THERE NO EQUIVALENT property.");
		System.out.println("PropSource: " + propDs);
		System.out.println("dsSource: " + dsS);
		System.out.println("dsTarget: " + dsT);
		return null;
	}

	private static Set<String> similaritySearchProp(String propDs, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = HDTManager.mapIndexedHDT(dsT, null);
		HDTGraph graph = new HDTGraph(hdt);
		Model model = ModelFactory.createModelForGraph(graph);
		String functionUri = "http://www.valdestilhas.org/JaccardSim";
		FunctionRegistry.get().put(functionUri, JaccardFilter.class);
		
		String cSparql = "Select ?p where {?s ?p ?o . "
				+ "FILTER(<" + functionUri + ">(?p, \"" + propDs + "\") > 0.8) }";

		QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			ret.add(rs.next().toString());
		}
		return ret;
	}

	private static Set<String> similaritySearchValue(String pValue, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = HDTManager.mapIndexedHDT(dsT, null);
		HDTGraph graph = new HDTGraph(hdt);
		Model model = ModelFactory.createModelForGraph(graph);
		String functionUri = "http://www.valdestilhas.org/JaccardSim";
		FunctionRegistry.get().put(functionUri, JaccardFilter.class);
		
		String cSparql = "Select ?o where {?s ?p ?o . "
				+ "FILTER(<" + functionUri + ">(?o, \"" + pValue + "\") > 0.8) }";

		QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			ret.add(rs.next().toString());
		}
		return ret;
	}

	private static String searchValueProp(String pValue, String dsT) throws IOException {
		String cSparql = "Select ?o where {?s ?p <"+pValue+">}";
		 
		// if is literal
		if(!pValue.startsWith("http")) {
			cSparql = "Select ?o where {?s ?p \""+pValue+"\"}";
		}
		Set<String> ret = Util.execQueryHDTRes(cSparql, dsT, -1);
		if(ret.size() > 0) {
			return ret.toString();
		} else {
			return null;
		}
	}

	private static String getValueProp(String propDs, String dsS) throws IOException {
		String cSparql = "Select ?o where {?s <"+propDs+"> ?o}";
		Set<String> ret = Util.execQueryHDTRes(cSparql, dsS, -1);
		
		if(ret.size() > 0) {
			return ret.toString();
		} else {
			return null;
		}
	}

	private static boolean propExists(String propDs, String dsT) throws IOException {
		String cSparql = "Select * where {?s <"+propDs+"> ?o}";
		Set<String> ret = Util.execQueryHDTRes(cSparql, dsT, -1);
		return (ret.size() > 0);
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
