package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
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

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.RDFFormat;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

public class SparqlMatching {

	public static final Map<String, String> mapPropValueDsT = new HashMap<String, String>();
	public static final Map<String, Long> mapDsNumTriples = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResBefore = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResAfter = new HashMap<String, Long>();

	public static long totalNumTriples = 0;

	public static void main(String[] args) throws IOException, InterruptedException, IllegalArgumentException, ParserException {
		StopWatch stopWatch = new StopWatch();
		ExperimentSparql exp = new ExperimentSparql();
//		final String cSparql = "Select * where {?s ?p ?o ." + "Filter(" + "?p=<http://dbpedia.org/ontology/birth> || "
//				+ "?p=<http://dbpedia.org/ontology/death> ||" + "?p=<http://creativecommons.org/ns#license> " + ")}";		
//		exp.setSparqlQuery(cSparql);
//		exp.setDs("dirHDT/3_ds_tests/hdt/d2.hdt");
		
//		final String cSparql = "SELECT ?name ?area ?pop ?lat ?long WHERE {\n" + 
//		" ?s a <http://dbpedia.org/ontology/City> ;\n" + 
//		" <http://dbpedia.org/ontology/PopulatedPlace/areaTotal> ?area ;\n" + 
//		" <http://www.w3.org/2000/01/rdf-schema#label> ?name ;\n" + 
//		" <http://dbpedia.org/property/populationTotal> ?pop ;\n" + 
//		" <http://www.w3.org/2003/01/geo/wgs84_pos#lat> ?lat ;\n" + 
//		" <http://www.w3.org/2003/01/geo/wgs84_pos#long> ?long .\n" + 
//		"FILTER ( regex(str(?name), \"Leipzig\" ) && langMatches(lang(?name),\"en\"))\n" + 
//		"}";
//		exp.setSparqlQuery(cSparql);
//		exp.setDs("http://dbpedia.org/sparql");
		
//		final String cSparql = "SELECT DISTINCT * WHERE { <http://dbpedia.org/ontology/areaCode> ?p ?o }";		
//		exp.setSparqlQuery(cSparql);
//		exp.setDs("dirHDT/DBPedia-3.9-en.hdt");
//		Set<String> manyDt = exp.getDatasets(new File("dirHDT"), 10);
//		exp.setManyDt(manyDt);
		
		final String cSparql = "SELECT DISTINCT * WHERE { ?s ?p ?o . "
				+ "Filter ("
				+ "?p=<file:///media/andre/DATA/Dropbox/ws1/LDatasetGenerator/AmazonProducts.csv#price> || "
				+ "?p=<file:///media/andre/DATA/Dropbox/ws1/LDatasetGenerator/AmazonProducts.csv#title>)"
				+ "}";		
		exp.setSparqlQuery(cSparql);
		exp.setDs("AmazonProducts.hdt");
		Set<String> manyDt = exp.getDatasets(new File("manyDt"), 10);
		exp.setManyDt(manyDt);
		
		stopWatch.start();
		exp.execute();
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
		//basedSparql();
	}

	private static void basedSparql() throws IOException, InterruptedException, IllegalArgumentException, ParserException {
		final Map<String, String> mapProps = new HashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
		final Map<String, Map<String, String>> newMapPropsDs = new HashMap<String, Map<String, String>>();

		long start = System.currentTimeMillis();
		Set<String> datasets = new LinkedHashSet<String>();
		// datasets.add("http://dbpedia.org/sparql");
		// datasets.add("http://lod2.openlinksw.com/sparql");
		// datasets.add("https://query.wikidata.org/");
		// datasets.add("http://download.lodlaundromat.org/85d5a476b56fde200e770cefa0e5033c?type=hdt");
//		datasets.add("dirHDT/85d5a476b56fde200e770cefa0e5033c.hdt");
//		datasets.add("b7081efa178bc4ab3ff3a6ef5abac9b2.hdt");
//		datasets.add("c66ff6bbdb8eeac9c17adbe7dfe4efd5.hdt");
		datasets.add("dirHDT/3_ds_tests/hdt/d1.hdt");
		datasets.add("dirHDT/3_ds_tests/hdt/d2.hdt");
		datasets.add("dirHDT/3_ds_tests/hdt/d3.hdt");
		int numberOfDs = 100;
		datasets.addAll(getDatasets(new File("dirHDT"), numberOfDs));

		final String cSparql = "Select * where {?s ?p ?o ." + "Filter(" + "?p=<http://dbpedia.org/ontology/birth> || "
				+ "?p=<http://dbpedia.org/ontology/death> ||" + "?p=<http://creativecommons.org/ns#license> " + ")}";

//		<http://dbpedia.org/ontology/birth>
//		<http://dbpedia.org/ontology/death>
//		<http://dbpedia.org/ontology/birthDate>
//		<http://creativecommons.org/ns#license>
//		<http://dbpedia.org/ontology/deathDate>
		PrintWriter writer = new PrintWriter("SparqlMatching_" + datasets.size() + ".txt", "UTF-8");
		long numberTriples = 0;
		writer.println(cSparql);
		Set<String> resultsSPARQL = new LinkedHashSet<String>();
		for (String ds : datasets) {

			mapProps.putAll(extractURIS(cSparql));
			mapPropsDs.put(ds, mapProps);
			// Map<String, String> props = getPropsWimuQ(mapProps, ds);
			Map<String, String> props = null;
			// if((props == null) || (props.size() < 1)) {
			// System.out.println("wimuQ could not identify a dataset for the SPARQL query:
			// " + cSparql);
			//String dsS = "dirHDT/c926d22eb49788382ffc87a5942f7fb3.hdt";
			String dsS = "dirHDT/3_ds_tests/hdt/d2.hdt";
			System.out.println("Using a predifined dataset :" + dsS);
			try {
				props = getPropsDsS(mapProps, ds, dsS);
			} catch (Exception e) {
				if (e.getMessage().contains("Adjacency list bitmap")) {
					writer.println("Empty dataset: " + ds);
					continue;
				}
			}
			if (mapDsNumTriples.get(ds) != null) {
				numberTriples = mapDsNumTriples.get(ds);
				totalNumTriples += numberTriples;
			}
			if ((props == null) || (props.size() < 1)) {

				writer.println("Problem with dataset where the SPARQL are supposed to run: " + dsS);
				continue;
			}
			// }
			mapPropsDs.get(ds).putAll(props);
			String nSparql = replaceURIs(cSparql, mapPropsDs.get(ds));

			Set<String> retBefore = execSparql(cSparql, ds);
			Set<String> retAfter = execSparql(nSparql, ds);
			resultsSPARQL.addAll(retBefore);
			resultsSPARQL.addAll(retAfter);
			writer.println("***************");
			writer.println(nSparql);
			writer.println("Dataset: " + ds);
			writer.println("Properties matched: " + props);
			newMapPropsDs.put(ds, props);
			writer.println("Number results before:" + retBefore.size());
			writer.println("Number results after:" + retAfter.size());
			writer.println("Different ontology/property/attribute_name: YES");
			writer.println("Number of triples: " + numberTriples);
			writer.println("***************");
		}

		long total = System.currentTimeMillis() - start;
		writer.println("Total number of triples: " + totalNumTriples);
		writer.println("FINISHED in " + TimeUnit.MILLISECONDS.toSeconds(total) + " seconds");
		writer.println("Number of results Total after the schema aligment: " + resultsSPARQL.size());
		writer.close();
		writeDsPropsMatched(newMapPropsDs);
		writeHDTResult(resultsSPARQL);
	}

	private static void writeDsPropsMatched(Map<String, Map<String, String>> mapPropsDs) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter("RelationDsPropsMatched.tsv", "UTF-8");
		writer.println("Dataset\t#Matches");
		for (Entry<String, Map<String, String>> dsProp : mapPropsDs.entrySet()) {
			String ds = dsProp.getKey();
			Map<String, String> matches = dsProp.getValue();
			int excludeCases = 0;
			for (Entry<String, String> match : matches.entrySet()) {
				if(match.getValue() == null) {
					excludeCases++;
				}
			}
			writer.println(ds + "\t" + (matches.size()-excludeCases));
		}
		writer.close();
	}

	private static void writeHDTResult(Set<String> resultsSPARQL)
			throws IllegalArgumentException, IOException, ParserException {
		// Configuration variables
		String baseURI = "http://example.com/mydataset";
		String fileNT = "UnionResultsSchemaMatching.nt";
		String hdtOutput = "UnionResultsSchemaMatching.hdt";

		createNtFile(resultsSPARQL, fileNT);
		// Create HDT from RDF file
		HDT hdt = HDTManager.generateHDT(fileNT, // Input RDF File
				baseURI, // Base URI
				RDFNotation.guess(fileNT), // Input Type
				new HDTSpecification(), // HDT Options
				null // Progress Listener
		);
		hdt.saveToHDT(hdtOutput, null);
	}

	private static void createNtFile(Set<String> resultsSPARQL, String fileNT) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileNT, "UTF-8");
		
		for (String triple : resultsSPARQL) {
			try {
				String split[] = triple.split("http");
				String o = split[0].trim();
				String s = "http" + split[1].trim();
				String p = "http" + split[2].trim();
				
				if(o.startsWith("://")) {
					o = "http" + o;
				}
				
				Triple t = null;
				if (o.startsWith("http")) {
					t = Triple.create(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createURI(o));
					String tp = "<"+t.getSubject().getURI()+">\t<"+t.getPredicate().getURI()+">\t"+t.getObject().getURI()+"\t.";
					writer.println(tp);
				} else {
					t = Triple.create(NodeFactory.createURI(s), NodeFactory.createURI(p), NodeFactory.createLiteral(o));
					String tp = "<"+t.getSubject().getURI()+">\t<"+t.getPredicate().getURI()+">\t"+t.getObject()+"^^"+t.getObject().getLiteralDatatypeURI()+"\t.";
					writer.println(tp);
				}
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		writer.close();
	}

	public static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();
		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
			Runnable block = new Runnable() {
				public void run() {
					if (Util.isEndPoint(source)) {
						// ret.addAll(execQueryEndPoint(cSparql, source));
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

	public static String replaceURIs(String cSparql, Map<String, String> mProps) {
		for (Entry<String, String> entry : mProps.entrySet()) {
			String orig = entry.getKey();
			String replace = entry.getValue();
			if ((orig != null) && (replace != null)) {
				cSparql = cSparql.replaceAll(orig, replace);
			}
			System.out.println("orig=" + orig);
			System.out.println("replace=" + replace);
		}
		return cSparql;
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target. Try to
	 * discover the Source dataset of the properties using wimuQ.
	 */
	private static Map<String, String> getPropsWimuQ(Map<String, String> mProps, String dsT)
			throws IOException, InterruptedException {
		String dsS = getDataset(mProps.keySet());
		// String dsS = "dirHDT/85d5a476b56fde200e770cefa0e5033c.hdt";
		// Main.goodSources.add(dsS);
		System.out.println("Datase where the SPARQL works: " + dsS);
		return getProps(mProps, dsS, dsT);
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 */
	private static Map<String, String> getPropsDsS(Map<String, String> mProps, String dsT, String dsS)
			throws IOException {
		// String dsS = getDataset(mProps.keySet());
		Main.goodSources.add(dsS);
		System.out.println("Datase where the SPARQL works: " + dsS);
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
		String cSparql = "Select ?date where{\n" + "?s <http://purl.org/dc/terms/modified> ?date\n"
				+ "} order by DESC(?date) limit 1";
		final Map<String, String> mDsTimeStamp = new HashMap<String, String>();
		Set<String> ret = new HashSet<String>();
		String sRet = null;

		for (String source : datasets) {
			// lstSources.parallelStream().forEach( source -> {
			try {
				TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
				Runnable block = new Runnable() {
					public void run() {

						if (Util.isEndPoint(source)) {
							// ret.addAll(execQueryEndPoint(cSparql, source));
							ret.addAll(Util.execQueryEndPoint(cSparql, source, true, -1));
						} else {
							ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
						}
						String sRet = null;
						String previous = mDsTimeStamp.get(source);
						for (String timeStamp : ret) {
							if (Util.isGreaterDate(timeStamp, previous)) {
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
		for (String sret : mDsTimeStamp.keySet()) {
			sRet = sret;
		}

		return sRet;
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 */
	private static Map<String, String> getProps(Map<String, String> mProps, String dsS, String dsT) throws IOException {
		Map<String, String> mRet = new HashMap<String, String>();

		if (!Main.goodSources.contains(dsS)) {
			return null;
		}

		for (Entry<String, String> entry : mProps.entrySet()) {
			String prop = entry.getKey().trim();
			Set<String> equivProps = getEquivProp(prop, dsS, dsT);
			if (equivProps != null) {
				for (String equivProp : equivProps) {
					mRet.put(prop, equivProp);
				}
			}
		}

		return mRet;
	}

	/*
	 * Get the equivalent property 1. Check if @propDs exists in dsT. 2. Do the best
	 * practice that is using the owl:equivalentProperty. 3. Compare the values from
	 * all properties of dsT. 4. Using string similarity(Threshold=0.8), search in a
	 * set of all properties from dsT(values). 5. Using string
	 * similarity(Threshold=0.8), search in a set of all properties from dsT(names).
	 */
	private static Set<String> getEquivProp(String propDs, String dsS, String dsT) throws IOException {
		// This implementation is better, evaluated,etc... ;)
		return PropertyMatching.getEquivProp(propDs, dsS, dsT);

//		Set<String> equivProps = new HashSet<String>();
//		//Check if @propDs exists in dsT.
//		if(propExists(propDs, dsT)) {
//			equivProps.add(propDs);
//			return equivProps;
//		}
//		String pValue = getValueProp(propDs,dsS);
//		if(pValue == null) {
//			return null;
//		}
//		
//		String cSparql = "SELECT * WHERE {<"+propDs+"> <https://www.w3.org/2002/07/owl#equivalentProperty> ?o}";
//		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
//		
//		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentProperty> <"+propDs+">}";
//		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
//		
//		cSparql = "SELECT * WHERE {<"+propDs+"> <https://www.w3.org/2002/07/owl#equivalentClass> ?o}";
//		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
//		
//		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentClass> <"+propDs+">}";
//		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));
//		
//		if(equivProps.size() > 0) {
//			return equivProps;
//		}
//		/*
//		 * Compare the values from all properties of dsT.
//		 */
//		String equivProp = searchValueProp(pValue,dsT); 
//		if(equivProp != null) {
//			equivProps.add(equivProp);
//			return equivProps;
//		}
//		
//		equivProps.addAll(similaritySearchValue(pValue, dsT));
//		if(equivProps.size() > 0) {
//			return equivProps;
//		}
//		
//		equivProps.addAll(similaritySearchProp(propDs, dsT));
//		if(equivProps.size() > 0) {
//			return equivProps;
//		}
//		System.out.println("THERE NO EQUIVALENT property.");
//		System.out.println("PropSource: " + propDs);
//		System.out.println("dsSource: " + dsS);
//		System.out.println("dsTarget: " + dsT);
//		return null;
	}

	private static Set<String> similaritySearchProp(String propDs, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = HDTManager.mapIndexedHDT(dsT, null);
		HDTGraph graph = new HDTGraph(hdt);
		Model model = ModelFactory.createModelForGraph(graph);
		String functionUri = "http://www.valdestilhas.org/JaccardSim";
		FunctionRegistry.get().put(functionUri, SimilarityFilter.class);

		String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?p, \"" + propDs
				+ "\") > 0.8) }";

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
		FunctionRegistry.get().put(functionUri, SimilarityFilter.class);

		String cSparql = "Select ?o where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?o, \"" + pValue
				+ "\") > 0.8) }";

		QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
		ResultSet rs = qexec.execSelect();
		while (rs.hasNext()) {
			ret.add(rs.next().toString());
		}
		return ret;
	}

	private static String searchValueProp(String pValue, String dsT) throws IOException {
		String sRet = null;
		String cSparql = "Select ?p where {?s ?p <" + pValue + ">}";

		// if is literal
		if (!pValue.startsWith("http")) {
			cSparql = "Select ?p where {?s ?p \"" + pValue + "\"}";
		}
		Set<String> ret = Util.execQueryHDTRes(cSparql, dsT, -1);
		if (ret.size() > 0) {
			for (String line : ret) {
				sRet = line;
				break;
			}
			return sRet.trim();
		} else {
			return null;
		}
	}

	private static String getValueProp(String propDs, String dsS) throws IOException {
		String sRet = null;
		String cSparql = "Select ?o where {?s <" + propDs + "> ?o}";
		Set<String> ret = Util.execQueryHDTRes(cSparql, dsS, -1);

		if (ret.size() > 0) {
			for (String line : ret) {
				sRet = line;
				break;
			}
			return sRet.trim();
		} else {
			return null;
		}
	}

	private static boolean propExists(String propDs, String dsT) throws IOException {
		String cSparql = "Select * where {?s <" + propDs + "> ?o}";
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

	public static String replacePrefixes(String query) throws UnsupportedEncodingException {
		PrefixMapping pmap = PrefixMapping.Factory.create();
//		Map<String, String> mPrefixURI = getMapPrefix(query);
//		pmap.setNsPrefixes(mPrefixURI);
		pmap.setNsPrefixes(PrefixMapping.Extended);
		Prologue prog = new Prologue();
		prog.setPrefixMapping(pmap);
		Query q = QueryFactory.parse(new Query(prog), query, null, null);
		// Set Prefix Mapping
		q.setPrefixMapping(pmap);
		// remove PrefixMapping so the prefixes will get replaced by the full uris
		q.setPrefixMapping(null);
		return q.serialize();
	}

	private static Set<String> getDatasets(File file, int limit) {
		Set<String> ret = new LinkedHashSet<String>();
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			int count = 0;
			for (File source : files) {
				if (source.isFile()) {
					if (count >= limit)
						break;
					ret.add(source.getAbsolutePath());
					count++;
				}
			}
		} else {
			System.err.println(file.getName() + " is not a directory !");
		}

		return ret;
	}
}
