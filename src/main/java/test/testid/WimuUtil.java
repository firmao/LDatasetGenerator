package test.testid;

import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.net.URLEncoder;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;

import com.google.gson.Gson;

public class WimuUtil {
	public static Set<String> getDsWIMUs(String uri) throws InterruptedException, IOException {
		Set<String> sRet = new HashSet<String>();

		String nURI = URLEncoder.encode(uri, "UTF-8");
		
		URL urlSearch = new URL("http://wimu.aksw.org/Find?uri=" + nURI);
		InputStreamReader reader = null;
		try {
			reader = new InputStreamReader(urlSearch.openStream());
		} catch (Exception e) {
			Thread.sleep(5000);
			reader = new InputStreamReader(urlSearch.openStream());
		}
		try {
			WIMUDataset[] wData = new Gson().fromJson(reader, WIMUDataset[].class);
			for (WIMUDataset wDs : wData) {
				if (sRet.size() > 10)
					break;
				sRet.add(wDs.getDataset());
				if (sRet.size() < 1) {
					sRet.add(wDs.getHdt());
				}
			}
		} catch (Exception e) {
			System.err.println("No dataset for the URI: " + uri);
		}
		System.out.println("Resource: " + uri + " NumberDatasets: " + sRet.size());
		return sRet;
	}

	public static Map<String, Set<String>> getFromWIMUq(String resource) throws InterruptedException, IOException {
		//System.out.println("getFromWIMU()  - IMPLEMENT !!!!!");
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		Set<String> dsWIMUs = getDsWIMUs(resource);

		for (final String ds : dsWIMUs) {
			if ((ds != null) && ds.contains("dbpedia")) {
				mPropValue.putAll(getEndPoint(ds, resource));
			}
			if ((ds != null) && ds.contains("https://hdt.lod.labs.vu.nl")) {
				mPropValue.putAll(getLODalot(ds, resource));
			}

			try {
				mPropValue.putAll(getFromDump(ds, resource));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return mPropValue;
	}

	private static Map<String, Set<String>> getLODalot(String ds, String resource) {
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		System.out.println("NEED TO IMPLEMENT: getLODalot");
		return mPropValue;
	}

	private static Map<String, Set<String>> getEndPoint(String ds, String resource) {
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		
		String cSparql = "select distinct ?property ?o ?s {\n" + 
				"  { <"+resource+"> ?property ?o }\n" + 
				"  union\n" + 
				"  { ?s ?property <"+resource+"> }\n" + 
				"\n" + 
				"  optional { \n" + 
				"    ?property <http://www.w3.org/2000/01/rdf-schema#label> ?label .\n" + 
				"    filter langMatches(lang(?label), 'en')\n" + 
				"  }\n" + 
				"}";
		
		Query query = QueryFactory.create(cSparql);
		QueryExecution qexec = QueryExecutionFactory.sparqlService("http://dbpedia.org/sparql", query);
		// QueryEngineHTTP qexec = new QueryEngineHTTP(endPoint, cSparql);
		try {

			ResultSet results = qexec.execSelect();
			List<QuerySolution> lst = ResultSetFormatter.toList(results);
			for (QuerySolution qSolution : lst) {
				if(mPropValue.containsKey(qSolution.get("?property").toString().trim())) {
					if((qSolution.get("?o") != null) && (qSolution.get("?o").toString().trim().length() > 0)) {
						mPropValue.get(qSolution.get("?property").toString().trim()).add(qSolution.get("?o").toString().trim());
					} else {
						mPropValue.get(qSolution.get("?property").toString().trim()).add(qSolution.get("?s").toString().trim());
					}
				} else {
					Set<String> value = new HashSet<String>(); 
					if((qSolution.get("?o") != null) && (qSolution.get("?o").toString().trim().length() > 0)) {
						value.add(qSolution.get("?o").toString().trim());
						mPropValue.put(qSolution.get("?property").toString().trim(), value);
					} else {
						value.add(qSolution.get("?s").toString().trim());
						mPropValue.put(qSolution.get("?property").toString().trim(), value);
					}
				}
			}
		} catch (Exception e) {
			e.printStackTrace();
			// System.out.println(sSparql);
		} finally {
			qexec.close();
		}
		
		return mPropValue;
	}

	public static Map<String, Set<String>> getFromDump(String ds, String resource) throws IOException {
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		if (ds == null)
			return mPropValue;

		if (ds.endsWith("hdt")) {
			mPropValue.putAll(execHDT(ds, resource));
		} else {
			mPropValue.putAll(execRDF(ds, resource));
		}
		return mPropValue;
	}

	private static Map<String, Set<String>> execHDT(String ds, String resource) throws IOException {
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		File file = null;
		HDT hdt = null;
		try {
			System.out.println("Dataset: " + ds);
			long start = System.currentTimeMillis();
			if (ds.startsWith("http")) {
				URL url = new URL(ds);
				file = new File(Util.getURLFileName(url));
				if (!file.exists()) {
					FileUtils.copyURLToFile(url, file);
				}
			} else {
				file = new File(ds);
			}
			file = Util.unconpress(file);

			long total = System.currentTimeMillis() - start;
			System.out.println("Time to download dataset: " + total + "ms");
			start = System.currentTimeMillis();
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);

			String cSparql = "select distinct ?property ?o ?s {\n" + 
					"  { <"+resource+"> ?property ?o }\n" + 
					"  union\n" + 
					"  { ?s ?property <"+resource+"> }\n" + 
					"\n" + 
					"  optional { \n" + 
					"    ?property <http://www.w3.org/2000/01/rdf-schema#label> ?label .\n" + 
					"    filter langMatches(lang(?label), 'en')\n" + 
					"  }\n" + 
					"}";
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();
			List<QuerySolution> lst = ResultSetFormatter.toList(results);
			for (QuerySolution qSolution : lst) {
				if(mPropValue.containsKey(qSolution.get("?property").toString().trim())) {
					if((qSolution.get("?o") != null) && (qSolution.get("?o").toString().trim().length() > 0)) {
						mPropValue.get(qSolution.get("?property").toString().trim()).add(qSolution.get("?o").toString().trim());
					} else {
						mPropValue.get(qSolution.get("?property").toString().trim()).add(qSolution.get("?s").toString().trim());
					}
				} else {
					Set<String> value = new HashSet<String>(); 
					if((qSolution.get("?o") != null) && (qSolution.get("?o").toString().trim().length() > 0)) {
						value.add(qSolution.get("?o").toString().trim());
						mPropValue.put(qSolution.get("?property").toString().trim(), value);
					} else {
						value.add(qSolution.get("?s").toString().trim());
						mPropValue.put(qSolution.get("?property").toString().trim(), value);
					}
				}
			}

			total = System.currentTimeMillis() - start;
			System.out.println("Time to query dataset: " + total + "ms");
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + ds + " Error: " + e.getMessage());
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
		
		return mPropValue;
	}

	private static Map<String, Set<String>> execRDF(String ds, String resource) {
		final Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		File file = null;
		try {
			long start = System.currentTimeMillis();
			URL url = new URL(ds);
			file = new File(Util.getURLFileName(url));
			if (!file.exists()) {
				FileUtils.copyURLToFile(url, file);
			}

			file = Util.unconpress(file);

			long total = System.currentTimeMillis() - start;
			System.out.println("Time to download dataset: " + total + "ms");
			if (file.getName().endsWith("hdt")) {
				return execHDT(file.getAbsolutePath(), resource);
			}

			start = System.currentTimeMillis();
			mPropValue.putAll(Util.processUnzipRDF(file, resource));
			total = System.currentTimeMillis() - start;
			System.out.println("Time to query dataset: " + total + "ms");
			// file.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		return mPropValue;
	}
}
