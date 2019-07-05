package test.testid;

import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.text.similarity.JaccardSimilarity;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class PropertyMatchingNN {

	public static final Map<String, String> mapPropValueDsT = new HashMap<String, String>();
	public static final Map<String, Long> mapDsNumTriples = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResBefore = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResAfter = new HashMap<String, Long>();

	public static long totalNumTriples = 0;

	public static void main(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		ExperimentNN exp = new ExperimentNN();

//		exp.setDs("dirHDT/3_ds_tests/hdt/d1.hdt");
//		exp.setDt("dirHDT/3_ds_tests/hdt/d2.hdt");
//		exp.setGoldStandard("goldStandardPersonal.tsv");

//		exp.setDs("dirHDT/3_ds_tests/hdt/personne1_vldb.hdt");
//		exp.setDt("dirHDT/3_ds_tests/hdt/personne2_vldb.hdt");
//		exp.setGoldStandard("goldPersonVldb.tsv");

//		exp.setDs("AmazonProducts.csv");
//		exp.setDt("GoogleProducts.csv");
//		exp.setGoldStandard("goldStandardAmazonGoogleProds.tsv");

//		exp.setDs("Abt.csv");
//		exp.setDt("Buy.csv");
//		exp.setGoldStandard("goldStandardAbtBuy.tsv");

//		exp.setDs("ACM.csv");
//		exp.setDt("DBLP.csv");
//		exp.setGoldStandard("goldStandardACMDBLP.tsv");

//		exp.setDs("DBLP.csv");
//		exp.setDt("Scholar.csv");
//		exp.setGoldStandard("goldStandardDBLPScholar.tsv");

//		exp.setDs("dirHDT/3_ds_tests/hdt/d1.hdt");
//		Set<String> manyDt = exp.getDatasets(new File("dirHDT"), 10);
//		exp.setManyDt(manyDt);

//		exp.setDs("AmazonProducts.hdt");
//		Set<String> manyDt = exp.getDatasets(new File("manyDt"), 10);
//		exp.setManyDt(manyDt);

		Set<String> manyDs = exp.getDatasets(new File("dirDBpediaSubsetCSV"), 99999999);
		exp.setManyDs(manyDs);
		//exp.setDs("dirHDT/mappingbased_properties_en.hdt");
		Set<String> manyDt = exp.getDatasets(new File("webTables_1"), 99999999);
		exp.setManyDt(manyDt);
		exp.setGoldStandard("dirGoldStandard");

		stopWatch.start();
		//exp.execute();
		//exp.compare1N();
		exp.compareNN();
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	public static Map<String, String> schemaMatching(String dsS, String dsT) throws IOException {
		Map<String, String> resPropDsSDsT = null;

		Set<String> setPropDsS = getProps(dsS);
		System.out.println("Properties from " + dsS + ": " + setPropDsS.size());
		System.out.println("Starting Matching...");
		resPropDsSDsT = matching(setPropDsS, dsS, dsT);
		// resPropDsSDsT = matchingJoinSim(setPropDsS, dsS, dsT);

		return resPropDsSDsT;

//		Map<String, Set<String>> resPropDsTDsS = new LinkedHashMap<String, Set<String>>();
//		for (String pDsT : setPropDsT) {
//			Set<String> sRet = getEquivProp(pDsT, dsT, dsS);
//			resPropDsTDsS.put(pDsT, sRet);
//		}
	}

	private static Map<String, String> matchingJoinSim(Set<String> setPropDsS, String dsS, String dsT) {
		final Map<String, String> resPropDsSDsT = new LinkedHashMap<String, String>();

		for (String pDsS : setPropDsS) {
			// setPropDsS.parallelStream().forEach(pDsS -> {
			Map<String, Integer> mRet = null;
			try {
				mRet = getEquivPropJoinSim(pDsS, dsS, dsT);
				System.out.println("Props Matched: " + mRet);
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (mRet == null) {
				// return;
				continue;
			}
			for (String propEquiv : mRet.keySet()) {
				if (propEquiv != null) {
					resPropDsSDsT.put(pDsS, propEquiv);
				}
			}
			// });

		}
		return resPropDsSDsT;
	}

	private static Map<String, String> matching(Set<String> setPropDsS, String dsS, String dsT) {
		final Map<String, String> resPropDsSDsT = new LinkedHashMap<String, String>();

		for (String pDsS : setPropDsS) {
			// setPropDsS.parallelStream().forEach(pDsS -> {
			Set<String> mRet = null;
			// Map<String, Integer> mRet = null;
			try {
				if (!pDsS.equals("http://w3c/future-csv-vocab/row")) {
					mRet = getEquivProp(pDsS, dsS, dsT);
					// mRet = getEquivPropJoinSim(pDsS, dsS, dsT);
					if (mRet != null) {
						mRet.remove("http://w3c/future-csv-vocab/row");
					}
				}
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			if (mRet == null) {
				// return;
				continue;
			}
			System.out.println("Candidates Props Matched: " + mRet);
			String bestMatch = getBestMatch(pDsS, mRet);
			resPropDsSDsT.put(pDsS, bestMatch);
			System.out.println("Matched pair: " + pDsS + "," + bestMatch);
//			for (String propEquiv : mRet) {
//				if (propEquiv != null) {
//					resPropDsSDsT.put(pDsS, propEquiv);
//				}
//			}
		}
		if(resPropDsSDsT.size() > 0) {
			System.out.println(resPropDsSDsT);
		}
		return resPropDsSDsT;
	}

	private static String getBestMatch(String pDsS, Set<String> mRet) {
		String ret = null;
		double sim = 0.0;
		String onlyName = Util.getURLName(pDsS);
		JaccardSimilarity jacSim = new JaccardSimilarity();
		for (String propEquiv : mRet) {
			if (propEquiv != null) {
				String nameProp = Util.getURLName(propEquiv);
				double newSim = jacSim.apply(onlyName, nameProp);
				if(newSim > sim) {
					sim = newSim;
					ret = propEquiv;
				}
			}
		}
		return ret;
	}

	private static Set<String> getProps(String dsS) {
		String cSparql = "Select DISTINCT ?p where {?s ?p ?o}";
		return execSparql(cSparql, dsS);
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();
//		try {
//			TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
//			Runnable block = new Runnable() {
//				public void run() {
		if (Util.isEndPoint(source)) {
			ret.addAll(Util.execQueryEndPoint(cSparql, source));
		} else {
			ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
		}
//				}
//			};
//			timeoutBlock.addBlock(block);// execute the runnable block
//		} catch (Throwable e) {
//			System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
//		}
		return ret;
	}

	/*
	 * Get the equivalent property 1. Do the best practice that is using the
	 * owl:equivalentProperty/Class. 2. Check if @propDs exists in dsT(if exists,
	 * check how similar is the value). 3. Compare the values from all properties of
	 * dsT. 4. Using string similarity(Threshold=0.8), search in a set of all
	 * properties from dsT(values). 5. Using string similarity(Threshold=0.8),
	 * search in a set of all properties from dsT(names).
	 */
	public static Map<String, Integer> getEquivPropJoinSim(String propDs, String dsS, String dsT) throws IOException {
		final Map<String, Integer> equivProps = new HashMap<String, Integer>();
		final Set<String> retSparql = new LinkedHashSet<String>();

		String cSparql = "SELECT * WHERE {<" + propDs + "> <http://www.w3.org/2002/07/owl#equivalentProperty> ?o}";
		retSparql.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {?s <http://www.w3.org/2002/07/owl#equivalentProperty> <" + propDs + ">}";
		retSparql.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {<" + propDs + "> <http://www.w3.org/2002/07/owl#equivalentClass> ?o}";
		retSparql.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {?s <http://www.w3.org/2002/07/owl#equivalentClass> <" + propDs + ">}";
		retSparql.addAll(execSparql(cSparql, dsS));

		if (equivProps.size() > 0) {
			equivProps.put(retSparql.toString(), 1);
			return equivProps;
		}

		String propValue = null;
		// Check if @propDs exists in dsT.
		if (propExists(propDs, dsT)) {
			propValue = getValueProp(propDs, dsS);
			if (propValue == null) {
				return null;
			}
			String propValueDt = getValueProp(propDs, dsT);
			if (propValueDt == null) {
				return null;
			}
			JaccardSimilarity sim = new JaccardSimilarity();
			if (sim.apply(propValue, propValueDt) > 0.9) {
				equivProps.put(propDs, 2);
				return equivProps;
			}
		}

		if (propValue == null) {
			propValue = getValueProp(propDs, dsS);
			if (propValue == null)
				return null;
		}

		/*
		 * Compare the values from all properties of dsT.
		 */
		String equivProp = searchValueProp(propValue, dsT);
		if (equivProp != null) {
			equivProps.put(equivProp, 3);
			return equivProps;
		}

		retSparql.addAll(similaritySearchValue(propValue, dsT));
		if (retSparql.size() > 0) {
			for (String pMatch : retSparql) {
				equivProps.put(pMatch, 4);
			}
			// equivProps.put(retSparql.toString(),4);
			return equivProps;
		}

		retSparql.addAll(similaritySearchProp(propDs, dsT));
		if (retSparql.size() > 0) {
			for (String pMatch : retSparql) {
				equivProps.put(pMatch, 5);
			}
			// equivProps.put(retSparql.toString(),5);
			return equivProps;
		}
		// System.out.println("THERE ARE NO EQUIVALENT property: " + propDs + " in " +
		// dsT);
		return null;
	}

	/*
	 * Get the equivalent property 1. Check if @propDs exists in dsT. 2. Do the best
	 * practice that is using the owl:equivalentProperty. 3. Compare the values from
	 * all properties of dsT. 4. Using string similarity(Threshold=0.8), search in a
	 * set of all properties from dsT(values). 5. Using string
	 * similarity(Threshold=0.8), search in a set of all properties from dsT(names).
	 */
	public static Set<String> getEquivProp(String propDs, String dsS, String dsT) throws IOException {
		Set<String> equivProps = new HashSet<String>();
		// Check if @propDs exists in dsT.
		//String onlyPropName = Util.getURLName(propDs)
		if (propExists(propDs, dsT)) {
			equivProps.add(propDs);
			return equivProps;
		}
		String pValue = getValueProp(propDs, dsS);
		if (pValue == null) {
			return null;
		}

		String cSparql = "SELECT * WHERE {<" + propDs + "> <http://www.w3.org/2002/07/owl#equivalentProperty> ?o}";
		equivProps.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {?s <http://www.w3.org/2002/07/owl#equivalentProperty> <" + propDs + ">}";
		equivProps.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {<" + propDs + "> <http://www.w3.org/2002/07/owl#equivalentClass> ?o}";
		equivProps.addAll(execSparql(cSparql, dsS));

		cSparql = "SELECT * WHERE {?s <http://www.w3.org/2002/07/owl#equivalentClass> <" + propDs + ">}";
		equivProps.addAll(execSparql(cSparql, dsS));

		if (equivProps.size() > 0) {
			return equivProps;
		}
		/*
		 * Compare the values from all properties of dsT.
		 */
//		String equivProp = searchValueProp(pValue, dsT);
//		if (equivProp != null) {
//			equivProps.add(equivProp);
//			return equivProps;
//		}

//		equivProps.addAll(similaritySearchValue(pValue, dsT));
//		if (equivProps.size() > 0) {
//			int equivSizeBefore = equivProps.size();
//			equivProps.addAll(similaritySearchProp(propDs, dsT));
//			if (equivProps.size() > equivSizeBefore) {
//				return equivProps;
//			}
//			return equivProps;
//		}

		equivProps.addAll(similaritySearchProp(propDs, dsT));
		if (equivProps.size() > 0) {
			return equivProps;
		}
		// System.out.println("THERE ARE NO EQUIVALENT property: " + propDs + " in " +
		// dsT);
		return null;
	}

	private static Set<String> similaritySearchProp(String propDs, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = null;
		Model model = null;
		try {
			if (dsT.endsWith(".hdt")) {
				hdt = HDTManager.mapIndexedHDT(dsT, null);
				HDTGraph graph = new HDTGraph(hdt);
				model = ModelFactory.createModelForGraph(graph);
			} else {
				model = obtainModel(dsT);
			}
			String functionUri = "http://www.valdestilhas.org/AndreSim";
			FunctionRegistry.get().put(functionUri, SimilarityFilter.class);

//			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?p, \"" + propDs
//					+ "\") < 7) }";
			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(STRAFTER(str(?p),\"#\"), \"" + Util.getURLName(propDs)
			+ "\") >= 0.9) }";

			QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
			ResultSet rs = qexec.execSelect();
			while (rs.hasNext()) {
				for (String var : rs.getResultVars()) {
					ret.add(rs.next().get(var).toString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hdt != null) {
				hdt.close();
			}
		}

		return ret;
	}

	private static Set<String> similaritySearchValue(String pValue, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = null;
		Model model = null;
		try {
			if (dsT.endsWith(".hdt")) {
				hdt = HDTManager.mapIndexedHDT(dsT, null);
				HDTGraph graph = new HDTGraph(hdt);
				model = ModelFactory.createModelForGraph(graph);
			} else {
				model = obtainModel(dsT);
			}
			String functionUri = "http://www.valdestilhas.org/AndreSim";
			FunctionRegistry.get().put(functionUri, SimilarityFilter.class);
			
//			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?o, \"" + pValue
//					+ "\") < 7) }";
			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(STRAFTER(str(?o),\"#\"), \"" + Util.getURLName(pValue)
					+ "\") >= 0.9) }";

			QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
			ResultSet rs = qexec.execSelect();
			while (rs.hasNext()) {
				for (String var : rs.getResultVars()) {
					ret.add(rs.next().get(var).toString());
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			if (hdt != null) {
				hdt.close();
			}
		}
		return ret;
	}

	private static Model obtainModel(String dsT) throws IOException {
		if (Util.isEndPoint(dsT)) {
			return Util.obtainModelEndPoint(dsT);
		} else {
			return Util.ObtainModelRDF(dsT);
		}
	}

	private static String searchValueProp(String pValue, String dsT) throws IOException {
		String sRet = null;
		String cSparql = "Select ?p where {?s ?p <" + pValue + ">}";

		// if is literal
		if (!pValue.startsWith("http")) {
			// String nValue = Util.formatLiteral(pValue);
			cSparql = "Select ?p where {?s ?p ?o ." + " filter contains(str(?o),\"" + pValue + "\")} limit 1";
			// cSparql = "Select ?p where {?s ?p \""+pValue.replaceAll("^^http",
			// "\"^^http")+"}";
		}
		Set<String> ret = execSparql(cSparql, dsT);
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
		// Set<String> ret = Util.execQueryHDTRes(cSparql, dsS, -1);
		Set<String> ret = execSparql(cSparql, dsS);

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
		Set<String> ret = execSparql(cSparql, dsT);
		return (ret.size() > 0);
	}
}
