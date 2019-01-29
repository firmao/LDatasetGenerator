package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Collection;
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

public class Main {
	public static Set<String> goodSources = new HashSet<String>();
	public static final Set<String> setPropFilter = new HashSet<String>();
	
	public static void main(String[] args) throws IOException, InterruptedException, URISyntaxException {
		Set<String> resources = new HashSet<String>();
		Set<String> resourcesNotDeref = new HashSet<String>();
		Set<String> resErrorParallel = new HashSet<String>();
		Set<String> resErrorSerial = new HashSet<String>();

		setPropFilter.addAll(getResources(new File("filterProp.txt")));
		boolean useWimu = false;
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
			resources.addAll(getResources(args[0]));
			Util.writeFile(resources, fResources);
			Util.writeFile(goodSources, new File("goodSources.txt"));
		}

		System.out.println("Number of resources(Not verified yet): " + resources.size());
		
		//resourcesNotDeref.addAll(Util.getNotDeref(resources));
		//System.out.println("Resources Not dereferenceable: " + resourcesNotDeref.size());

		Util.writeFileStatistics(resources, "statistics.tsv");

		//resources.removeAll(resourcesNotDeref);

		System.out.println("Number of resources(Parallel)Starting with: " + resources.size());
		//System.exit(0);
		System.out.println("Starting to generate the entity files");
		resErrorParallel.addAll(executeParallel(resources, useWimu));

		System.out.println("Number of resources(Serial): " + resErrorParallel.size());
		
		resErrorSerial.addAll(processSerial(resErrorParallel, useWimu));
		resErrorParallel.clear();

		resourcesNotDeref.addAll(resErrorSerial);
		System.out.println("Resources Not dereferenceable(+previousErrors): " + resourcesNotDeref.size());
		useWimu = true;
		System.out.println("Number of resources WIMU(Parallel): " + resErrorSerial.size());
		resErrorParallel.addAll(executeParallel(resErrorSerial, useWimu));
		resErrorSerial.clear();

		System.out.println("Number of resources WIMU(Serial): " + resErrorParallel.size());
		resErrorSerial.addAll(processSerial(resErrorParallel, useWimu));
		resErrorParallel.clear();

		System.out.println("Finished and writing resErrors.txt");
		Util.writeFile(resErrorSerial, new File("resErrors.txt"));
		long total = System.currentTimeMillis() - start;
		System.out.println("FINISHED in " + TimeUnit.MILLISECONDS.toMinutes(total) + " minutes");
		System.exit(0);
	}

	private static Set<String> processSerial(Set<String> resErrorParallel, boolean useWimu) {
		Set<String> resErrorSerial = new HashSet<String>();
		int iCount = 0;
		for (String resource : resErrorParallel) {
			System.out.println("ResourceError: " + (++iCount) + " from " + resErrorParallel.size());
			try {
				Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();
				mapPropValue.putAll(generatePropertiesValues(resource, useWimu));
				if (mapPropValue.size() > 0) {
					Util.writeFile(resource, mapPropValue);
				} else {
					resErrorSerial.add(resource);
				}
			} catch (Exception e) {
				e.printStackTrace();
				resErrorSerial.add(resource);
			}
		}
		return resErrorSerial;
	}

	public static Set<String> executeParallel(Set<String> resources, boolean useWimu) {
		Set<String> newErrors = new HashSet<String>();
		int previousErrors = resources.size();
		newErrors.addAll(processParallel(resources, useWimu));
		if (newErrors.size() < previousErrors) {
			executeParallel(newErrors, useWimu);
		} else {
			return newErrors;
		}
		return newErrors;
	}

	public static Set<String> processParallel(Set<String> resources, boolean useWimu) {
		Set<String> resErrorParallel = new HashSet<String>();
		System.out.println("Number of resources(Parallel): " + resources.size());
		resources.parallelStream().forEach(resource -> {
			try {
				Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();
				mapPropValue.putAll(generatePropertiesValues(resource, useWimu));
				filterProperties(mapPropValue, setPropFilter);
				if (mapPropValue.size() > 0) {
					Util.writeFile(resource, mapPropValue);
				} else {
					resErrorParallel.add(resource);
				}
			} catch (Exception e) {
				e.printStackTrace();
				resErrorParallel.add(resource);
			}
		});
		return resErrorParallel;
	}

	private static void filterProperties(Map<String, Set<String>> map, Set<String> setPropFilter) {
		Map<String, Set<String>> newMapPropValue = new HashMap<String, Set<String>>();
		for (String prop : setPropFilter) {
			newMapPropValue.put(prop, map.get(prop));
		}
		map.clear();
		map.putAll(newMapPropValue);
	}
	
	private static Set<String> getResources(File fResources) throws IOException {
		Set<String> ret = new HashSet<String>();
		List<String> lstLines = FileUtils.readLines(fResources, "UTF-8");
		for (String resource : lstLines) {
			ret.add(resource.trim());
		}
		return ret;
	}

	private static Set<String> getResources(String fQuery) throws FileNotFoundException, UnsupportedEncodingException {
		Set<String> ret = new HashSet<String>();
		List<String> lstSources = new ArrayList<String>();
		List<String> lstQueries = getSampleQueries(new File(fQuery));
		String fEndPoints = "endpoints.txt";
		//String dirHDT = "/media/andre/Seagate/wimuLuceneIndex/allDsLaundromat/dirHDT/";
		String dirHDT = "dirHDT";
		// String endPoint1 = "http://dbpedia.org/sparql";
		// String endPoint2 = "http://lod.openlinksw.com/sparql";
		lstSources.addAll(getSources(new File(fEndPoints)));
		lstSources.addAll(getSources(new File(dirHDT)));
		System.out.println("Sources/Datasets: " + lstSources.size());
		
		LODStatistics.generateStatistics(lstSources, "http://www.w3.org/2002/07/owl#sameAs");
		Util.generateStatistics(lstSources, "ListDsTimeStamp.tsv");
		System.out.println("File with Dataset Time Stamps - Most Updated datasets - generated with success");
		
		for (String cSparql : lstQueries) {
			for (String source : lstSources) {
			//lstSources.parallelStream().forEach( source -> {
				try {
					TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
					Runnable block = new Runnable() {
						public void run() {
							if (Util.isEndPoint(source)) {
								//ret.addAll(execQueryEndPoint(cSparql, source));
								ret.addAll(Util.execQueryEndPoint(cSparql, source, true));
							} else {
								ret.addAll(Util.execQueryRDFRes(cSparql, source));
							}
						}
					};
					timeoutBlock.addBlock(block);// execute the runnable block
				} catch (Throwable e) {
					System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
				}
			}
			//});	
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

	private static List<String> getSources(File file) {
		List<String> ret = new ArrayList<String>();
		try {
			if(file.isDirectory()) {
				File[] files = file.listFiles();
				for (File source : files) {
				    if (source.isFile()) {
				        ret.add(source.getAbsolutePath());
				    }
				}
			} else {
				return FileUtils.readLines(file, "UTF-8");
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	public static Map<String, Set<String>> generatePropertiesValues(String resource, boolean wimu)
			throws InterruptedException, IOException {
		Map<String, Set<String>> mapPropValue = new HashMap<String, Set<String>>();

		// System.out.println("Resource: " + resource);
		if (!wimu) {
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
		} else {
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
			if (resource.equals(triple.getSubject().toString())) {
				if (mPropValue.containsKey(triple.getPredicate().toString().trim())) {
					mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getObject().toString());
				} else {
					Set<String> value = new HashSet<String>();
					value.add(triple.getObject().toString());
					mPropValue.put(triple.getPredicate().toString(), value);
				}
			} else {
				if (mPropValue.containsKey(triple.getPredicate().toString().trim())) {
					mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getSubject().toString());
				} else {
					Set<String> value = new HashSet<String>();
					value.add(triple.getSubject().toString());
					mPropValue.put(triple.getPredicate().toString(), value);
				}
				// mPropValue.put(triple.getPredicate().toString(),
				// triple.getSubject().toString());
			}
		}

		return mPropValue;
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
