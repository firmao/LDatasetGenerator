package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Statement;
import org.apache.jena.rdf.model.StmtIterator;
import org.apache.jena.riot.RDFDataMgr;
import org.apache.jena.riot.lang.PipedRDFIterator;
import org.apache.jena.riot.lang.PipedRDFStream;
import org.apache.jena.riot.lang.PipedTriplesStream;
import org.apache.jena.util.FileManager;

public class DsGenerator {

	public static void main(String[] args) throws IOException, InterruptedException {
		Set<String> resources = new HashSet<String>();
		Set<String> resError = new HashSet<String>();
		// resources.add("http://sws.geonames.org/78428/");
		// resources.add("http://dbpedia.org/resource/Leipzig");
		long start = System.currentTimeMillis();
		File f = new File("out/");
		if (!f.exists())
			f.mkdir();

		File fResources = new File("resources.txt");
		if (fResources.exists()) {
			resources.addAll(getResources(fResources));
		} else {
			fResources.createNewFile();
		}
		resources.addAll(getResources());
		System.out.println("Number of resources(Parallel): " + resources.size());
		Util.writeFile(resources, fResources);
		//resources.parallelStream().forEach(resource -> {
		int iCount = 0;
		for (String resource : resources) {
			System.out.println("Resource: " + (++iCount) + " from " + resources.size());
			try {
				Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();
				mapPropValue.putAll(generatePropertiesValues(resource));
				if (mapPropValue.size() > 0) {
					Util.writeFile(resource, mapPropValue);
				} else {
					resError.add(resource);
				}
			} catch (Exception e) {
				e.printStackTrace();
				resError.add(resource);
			}
		}
		//});
		System.out.println("Finished and writing resErrors.txt");
		Util.writeFile(resError, new File("resErrors.txt"));
		long total = System.currentTimeMillis() - start;
		System.out.println("FINISHED in " + TimeUnit.MILLISECONDS.toMinutes(total) + " minutes");
	}

	private static Set<String> getResources(File fResources) throws IOException {
		Set<String> ret = new HashSet<String>();
		List<String> lstLines = FileUtils.readLines(fResources, "UTF-8");
		for (String resource : lstLines) {
			ret.add(resource.trim());
		}
		return ret;
	}

	private static Set<String> getResources() {
		Set<String> ret = new HashSet<String>();
		List<String> lstQueries = getSampleQueries(new File("dbpediaCitiesPop.txt"));
		String endPoint = "http://dbpedia.org/sparql";

		for (String cSparql : lstQueries) {
			ret.addAll(execQueryEndPoint(cSparql, endPoint));
		}

//		String cSparql = "PREFIX owl: <http://www.w3.org/2002/07/owl#>\n" + 
//				"PREFIX dbo: <http://dbpedia.org/ontology/>\n" + 
//				"\n" + 
//				"SELECT DISTINCT ?so2  \n" + 
//				"WHERE\n" + 
//				"  {\n" + 
//				"    ?city  a                          dbo:City ; \n" + 
//				"           (owl:sameAs|^owl:sameAs)*  ?so2 .\n" + 
//				"    FILTER ( !regex(str(?so2), \"dbpedia\" ) )\n" + 
//				"  } \n" + 
//				"ORDER BY ?city";
//		ret.addAll(execQueryEndPoint(cSparql, endPoint));

		return ret;
	}

	public static Map<String, Set<String>> generatePropertiesValues(String resource)
			throws InterruptedException, IOException {
		Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();

		System.out.println("Resource: " + resource);
		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(180000); // 3 minutes
			Runnable block = new Runnable() {
				public void run() {
					try {
						mapPropValue.putAll(parseRDF(resource));
					} catch (Exception ex) {
						System.err.println("Error parseRDF(" + resource + ") " + ex.getMessage());
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.out.println("TIME-OUT-ERROR - parseRDF (Resource): " + resource);
		}

//		try {
//			TimeOutBlock timeoutBlock = new TimeOutBlock(180000); // 3 minutes
//			Runnable block = new Runnable() {
//				public void run() {
//					try {
//						mapPropValue.putAll(parseRDF2(resource));
//					} catch (Exception e) {
//						System.err.println("Error parseRDF2(" + resource + ") " + e.getMessage());
//					}
//				}
//			};
//			timeoutBlock.addBlock(block);// execute the runnable block
//		} catch (Throwable e) {
//			System.out.println("TIME-OUT-ERROR - parseRDF2 (Resource): " + resource);
//		}
//
		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(180000); // 3 minutes
			Runnable block = new Runnable() {
				public void run() {
					try {
						mapPropValue.putAll(WimuUtil.getFromWIMUq(resource));
					} catch (Exception e) {
						System.err.println("Error WimuUtil.getFromWIMUq(" + resource + ") " + e.getMessage());
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.out.println("TIME-OUT-ERROR - WimuUtil.getFromWIMUq(Resource): " + resource);
		}
		return mapPropValue;
	}

	private static Map<String, String> parseRDF2(String resource) {
		Map<String, String> mapPropValue = new HashMap<String, String>();

		// Model model = FileManager.get().loadModel(resource);
		Model model = ModelFactory.createDefaultModel();
		InputStream file = FileManager.get().open(resource);
		model.read(file, null);
		StmtIterator stmts = model.listStatements();
		while (stmts.hasNext()) {
			Statement stmt = stmts.next();
			if (resource.equals(stmt.getSubject().toString())) {
				mapPropValue.put(stmt.getPredicate().toString(), stmt.getObject().toString());
			} else {
				mapPropValue.put(stmt.getPredicate().toString(), stmt.getSubject().toString());
			}
		}

		return mapPropValue;
	}

	private static Map<String, Set<String>> parseRDF(final String resource)
			throws FileNotFoundException, UnsupportedEncodingException {
		Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		PipedRDFIterator<Triple> iter = new PipedRDFIterator<Triple>();
		final PipedRDFStream<Triple> inputStream = new PipedTriplesStream(iter);

		// PipedRDFStream and PipedRDFIterator need to be on different threads
		ExecutorService executor = Executors.newSingleThreadExecutor();

		// Create a runnable for our parser thread
		Runnable parser = new Runnable() {

			public void run() {
				// Call the parsing process.
				RDFDataMgr.parse(inputStream, resource);
			}
		};

		// Start the parser on another thread
		executor.submit(parser);

		while (iter.hasNext()) {
			Triple triple = iter.next();
			if(resource.equals(triple.getSubject().toString())) {
				if(mPropValue.containsKey(triple.getPredicate().toString().trim())) {
					mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getObject().toString());
				} else {
					Set<String> value = new HashSet<String>();
					value.add(triple.getObject().toString());
					mPropValue.put(triple.getPredicate().toString(), value);
				}
			} else {
				if(mPropValue.containsKey(triple.getPredicate().toString().trim())) {
					mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getSubject().toString());
				} else {
					Set<String> value = new HashSet<String>();
					value.add(triple.getSubject().toString());
					mPropValue.put(triple.getPredicate().toString(), value);
				}
				//mPropValue.put(triple.getPredicate().toString(), triple.getSubject().toString());
			}
		}

		return mPropValue;
	}

	public static Set<String> execQueryEndPoint(String cSparql, String endPoint) {
		System.out.println("Query endPoint: " + endPoint);
		final Set<String> ret = new HashSet<String>();
		final long offsetSize = 9999;
		long offset = 0;
		String sSparql = null;
		long start = System.currentTimeMillis();
		do {
			sSparql = cSparql;
			// int indOffset = cSparql.toLowerCase().indexOf("offset");
			// int indLimit = cSparql.toLowerCase().indexOf("limit");
			// if((indLimit < 0) && (indOffset < 0)) {
			// sSparql = cSparql + " offset " + offset + " limit " + offsetSize;
			sSparql = cSparql + " offset " + offset;
			// }
			// System.out.println(sSparql);
			Query query = QueryFactory.create(sSparql);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			// QueryEngineHTTP qexec = new QueryEngineHTTP(endPoint, cSparql);
			try {

				ResultSet results = qexec.execSelect();
				List<QuerySolution> lst = ResultSetFormatter.toList(results);
				for (QuerySolution qSolution : lst) {
					final StringBuffer sb = new StringBuffer();
					for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
						final String varName = varNames.next();
						ret.add(qSolution.get(varName).toString());
					}
				}
			} catch (Exception e) {
				// e.printStackTrace();
				// System.out.println(sSparql);
				break;
			} finally {
				qexec.close();
			}
			offset += offsetSize;
		} while (true);

		long total = System.currentTimeMillis() - start;
		long seconds = TimeUnit.MILLISECONDS.toSeconds(total);
		System.out.println("Time to get all resources(seconds): " + seconds);

		return ret;
	}

	public static List<String> getSampleQueries(File file) {
		List<String> ret = new ArrayList<String>();
		try {
			List<String> lstLines = FileUtils.readLines(file, "UTF-8");
			String query = "";
			for (String line : lstLines) {
				// if(!line.equals("§")){
				if (!line.startsWith("#------")) {
					query += line + "\n";
				} else {
					ret.add(query);
					query = "";
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}
}
