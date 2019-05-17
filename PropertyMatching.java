package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.ResultSet;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.sparql.function.FunctionRegistry;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

public class PropertyMatching {

	public static final Map<String, String> mapPropValueDsT = new HashMap<String, String>();
	public static final Map<String, Long> mapDsNumTriples = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResBefore = new HashMap<String, Long>();
	public static final Map<String, Long> mapDsNumResAfter = new HashMap<String, Long>();

	public static long totalNumTriples = 0;

	public static void main(String[] args) throws IOException, InterruptedException {
		StopWatch stopWatch = new StopWatch();
		Experiment exp = new Experiment();
		
		//exp.setDs("dirHDT/3_ds_tests/hdt/personne1_vldb.hdt");
		exp.setDs("AmazonProducts.csv");
		exp.setDt("GoogleProducts.csv");
//		Set<String> manyDt = exp.getDatasets(new File("dirHDT"),10);
//		exp.setManyDt(manyDt);
		
		exp.setGoldStandard("goldStandardAmazonGoogleProds.tsv");
		stopWatch.start();
		exp.execute();
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	public static Map<String, String> schemaMatching(String dsS, String dsT) throws IOException {
		final Map<String, String> resPropDsSDsT = new LinkedHashMap<String, String>();

		Set<String> setPropDsS = getProps(dsS);
		// Set<String> setPropDsT = getProps(dsT);

		for (String pDsS : setPropDsS) {
			Set<String> sRet = getEquivProp(pDsS, dsS, dsT);
			if(sRet == null) continue;
			for (String propEquiv : sRet) {
				if (propEquiv != null) {
					resPropDsSDsT.put(pDsS, propEquiv);
				}
			}
		}

		return resPropDsSDsT;

//		Map<String, Set<String>> resPropDsTDsS = new LinkedHashMap<String, Set<String>>();
//		for (String pDsT : setPropDsT) {
//			Set<String> sRet = getEquivProp(pDsT, dsT, dsS);
//			resPropDsTDsS.put(pDsT, sRet);
//		}
	}

	private static Set<String> getProps(String dsS) {
		String cSparql = "Select ?p where {?s ?p ?o}";
		return execSparql(cSparql, dsS);
	}

	private static Set<String> execSparql(String cSparql, String source) {
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
		if (propExists(propDs, dsT)) {
			equivProps.add(propDs);
			return equivProps;
		}
		String pValue = getValueProp(propDs, dsS);
		if (pValue == null) {
			return null;
		}

		String cSparql = "SELECT * WHERE {<" + propDs + "> <https://www.w3.org/2002/07/owl#equivalentProperty> ?o}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));

		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentProperty> <" + propDs + ">}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));

		cSparql = "SELECT * WHERE {<" + propDs + "> <https://www.w3.org/2002/07/owl#equivalentClass> ?o}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));

		cSparql = "SELECT * WHERE {?s <https://www.w3.org/2002/07/owl#equivalentClass> <" + propDs + ">}";
		equivProps.addAll(Util.execQueryHDTRes(cSparql, dsS, -1));

		if (equivProps.size() > 0) {
			return equivProps;
		}
		/*
		 * Compare the values from all properties of dsT.
		 */
		String equivProp = searchValueProp(pValue, dsT);
		if (equivProp != null) {
			equivProps.add(equivProp);
			return equivProps;
		}

		equivProps.addAll(similaritySearchValue(pValue, dsT));
		if (equivProps.size() > 0) {
			return equivProps;
		}

		equivProps.addAll(similaritySearchProp(propDs, dsT));
		if (equivProps.size() > 0) {
			return equivProps;
		}
		//System.out.println("THERE ARE NO EQUIVALENT property: " + propDs + " in " + dsT);
		return null;
	}

	private static Set<String> similaritySearchProp(String propDs, String dsT) throws IOException {
		Set<String> ret = new HashSet<String>();
		HDT hdt = null;
		try {
			hdt = HDTManager.mapIndexedHDT(dsT, null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = ModelFactory.createModelForGraph(graph);
			String functionUri = "http://www.valdestilhas.org/AndreSim";
			FunctionRegistry.get().put(functionUri, SimilarityFilter.class);

			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?p, \"" + propDs
					+ "\") < 7) }";

			QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
			ResultSet rs = qexec.execSelect();
			while (rs.hasNext()) {
				for ( String var : rs.getResultVars() ) {
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
		try {
			hdt = HDTManager.mapIndexedHDT(dsT, null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = ModelFactory.createModelForGraph(graph);
			String functionUri = "http://www.valdestilhas.org/AndreSim";
			FunctionRegistry.get().put(functionUri, SimilarityFilter.class);

			String cSparql = "Select ?p where {?s ?p ?o . " + "FILTER(<" + functionUri + ">(?o, \"" + pValue
					+ "\") < 7) }";

			QueryExecution qexec = QueryExecutionFactory.create(cSparql, model);
			ResultSet rs = qexec.execSelect();
			while (rs.hasNext()) {
				for ( String var : rs.getResultVars() ) {
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
}
