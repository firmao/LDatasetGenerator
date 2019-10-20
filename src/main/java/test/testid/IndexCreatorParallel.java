package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.text.similarity.JaccardSimilarity;

public class IndexCreatorParallel {

	public static final Map<String, String> mAlreadyCompared = new LinkedHashMap<String, String>();
	public static final Map<String, Set<String>> mapDatasetProperties = new LinkedHashMap<String, Set<String>>();
	public static final boolean IN_MEMORY = true;
	public static final String OUTPUT_DIR = "index_5000";
	public static Map<String, String> mapDsError = new LinkedHashMap<String, String>();

	public static void main(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		File fDir = new File(OUTPUT_DIR);
		if (!fDir.exists()) {
			fDir.mkdir();
		}
		System.out.println("PARALLEL-IN_MEMORY: " + IN_MEMORY);

		ExperimentNN exp = new ExperimentNN();
		Set<String> ds = new LinkedHashSet<String>();
		//ds.addAll(exp.getDatasets(new File("/media/andre/Seagate/personalDatasets/"), -1));
//		ds.addAll(exp.getDatasets(new File("dirHDT"), -1));
		//ds.addAll(exp.getDatasets(new File("dirHDTFamous"), -1));
//		ds.addAll(exp.getDatasets(new File("/media/andre/Seagate/tomcat9_p8082/webapps/ROOT/dirHDTLaundromat/"), 2000));
		ds.addAll(exp.getDatasets(new File("/home/andrevaldestilhas/LODDatasetIndex/dirHDTLaundromat"), 2000)); //LIMBO server
//		ds.addAll(exp.getDatasets(new File("dirHDTtests"), -1));
		ds.addAll(getEndpoints(new File("endpoints.txt")));
//		Map<String, String> mapQuerySource = getSampleQueries(new File("queryDsInfo.txt"));
//		ds.addAll(mapQuerySource.keySet());

//		ds.add("http://dbpedia.org/sparql");
		// ds.add("http://linkedgeodata.org/sparql");
		// ds.add("dirHDT/mappingbased_properties_en.hdt");
		// ds.add("dirHDT/DBPedia-3.9-en.hdt");
		System.out.println("Total datasets(before filter): " + ds.size());
		ds.removeAll(readFromFile("duplicates.txt"));
		System.out.println("Total datasets(after filter): " + ds.size());
		Set<String> dt = new LinkedHashSet<String>();
		// dt.addAll(mapQuerySource.keySet());
		dt.addAll(ds);
		final Map<String, Set<String>> mapExactMatch = new LinkedHashMap<String, Set<String>>();
		final Map<String, Map<String, String>> mapSim = new LinkedHashMap<String, Map<String, String>>();

		// for (String source : mapQuerySource.keySet()) {
		System.out.println("Total datasets: " + ds.size());
		//for (String source : ds) {
		ds.parallelStream().forEach(source -> {
			for (String target : dt) {
			//dt.parallelStream().forEach(target -> {
				if (source.equals(target)) {
					continue;
					//return;
				}
				if (alreadyCompared(source, target)) {
					continue;
					//return;
				}

				System.out.println(source + "---" + target);
				// count++;
				try {
					mapExactMatch.putAll(getExactMatches(source, target));
					mapSim.putAll(getSimMatches(source, target, 0.8, mapExactMatch));
				} catch (Exception e) {
					System.err.println("FAIL: " + source + "<>" + target);
				}
				mAlreadyCompared.put(source, target);
				mAlreadyCompared.put(target, source);
				System.out.println("Comparisons already done: " + mAlreadyCompared.size());
			//});
			}
		});
		//}
		String sFile = OUTPUT_DIR + "/tableMatches.tsv";
		System.out.println("Printing file: " + sFile);
		printMap(mapExactMatch, mapSim, sFile);
		System.out.println("Printing errors...");
		printErrors(mapDsError);
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	private static Set<String> readFromFile(String file) {
		List<String> lstLines = null;
		try {
			lstLines = Files.readAllLines(Paths.get(file));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new LinkedHashSet<String>(lstLines);
	}
	
	private static void printGoodDs(Set<String> ds) throws FileNotFoundException, UnsupportedEncodingException {
		System.out.println("JUST TESTING DATASETS");
		String cSparql = "SELECT ?s WHERE { ?s a <http://dbpedia.org/ontology/City> }";
		PrintWriter writer = new PrintWriter(OUTPUT_DIR + "/GoodDatasets.txt", "UTF-8");
		for (String source : ds) {
			Set<String> goodDs = execSparql(cSparql, source);
			if (goodDs.size() > 1) {
				writer.println(ds);
			}
		}
		writer.close();
		System.exit(0);
	}

	private static void printErrors(Map<String, String> mapDsError2)
			throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(OUTPUT_DIR + "/DatasetErrors.tsv", "UTF-8");
		writer.println("Dataset\t#Error");
		for (Entry<String, String> entry : mapDsError2.entrySet()) {
			String dataset = entry.getKey();
			String error = entry.getValue();
			writer.println(dataset + "\t" + error);
		}

		writer.close();
	}

	private static Set<String> getEndpoints(File file) throws IOException {
		Set<String> setLines = new LinkedHashSet<String>(FileUtils.readLines(file, "UTF-8"));
		return setLines;
	}

	private static Map<String, Map<String, String>> getSimMatches(String source, String target, double threshold,
			Map<String, Set<String>> mapExactMatch) throws FileNotFoundException, UnsupportedEncodingException {
		final Set<String> propsSource = new LinkedHashSet<String>();
		final Set<String> propsTarget = new LinkedHashSet<String>();
		final Map<String, String> propsMatched = new LinkedHashMap<String, String>();
		final Map<String, Map<String, String>> mapSim = new LinkedHashMap<String, Map<String, String>>();

		String s[] = source.split("/");
		String fSource = null;
		String fTarget = null;
		if (s.length > 2) {
			fSource = s[2] + "_" + s[s.length - 1];
		} else {
			fSource = s[s.length - 1];
		}
		String t[] = target.split("/");
		if (t.length > 2) {
			fTarget = t[2] + "_" + t[t.length - 1];
		} else {
			fTarget = t[t.length - 1];
		}
		// final String fileName = OUTPUT_DIR + "/" + fSource + "---" + fTarget +
		// "_Sim.tsv";
		final String fileName = fSource + "---" + fTarget + "_Sim.tsv";
		propsSource.addAll(getProps(source, fSource));
		propsTarget.addAll(getProps(target, fTarget));

		for (Set<String> done : mapExactMatch.values()) {
			propsSource.removeAll(done);
			propsTarget.removeAll(done);
		}

		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapSim;
		}

		// PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		JaccardSimilarity sim = new JaccardSimilarity();
		propsSource.parallelStream().forEach(pSource -> {
		//for(String pSource : propsSource) {
			propsTarget.parallelStream().forEach(pTarget -> {
			//for(String pTarget : propsTarget) {
				String p1 = Util.getURLName(pSource);
				String p2 = Util.getURLName(pTarget);
				double dSim = sim.apply(p1, p2);
				if (dSim >= threshold) {
					propsMatched.put(pSource, pTarget);
				}
			});
			//}	
		});
		//}
		mapSim.put(fileName, propsMatched);
		// writer.close();
		return mapSim;
	}

	private static void printMap(Map<String, Set<String>> mapExactMatch, Map<String, Map<String, String>> mapSim,
			String fileName) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		PrintWriter writerExact = new PrintWriter(fileName.replaceAll(".tsv", "_Exact.tsv"), "UTF-8");
		PrintWriter writerSim = new PrintWriter(fileName.replaceAll(".tsv", "_Sim.tsv"), "UTF-8");
		writer.println("Dataset_Source\tDataset_Target\t#ExactMatch\t#sim>0.9\t#PropDs\t#PropDt");
		writerExact.println("Property\tSource\tTarget");
		writerSim.println("PropertyS\tPropertyT\tSource\tTarget");
		Set<String> setAlreadyIncluded = new LinkedHashSet<String>();
		for (Entry<String, Set<String>> entry : mapExactMatch.entrySet()) {
			String fNameExact = entry.getKey();
			String fNameSim = fNameExact.replaceAll("_Exact.txt", "_Sim.tsv");
			Set<String> propExact = entry.getValue();
			String[] str = fNameExact.replaceAll("_Exact.txt", "").split("---");
			String source = str[0].replaceAll(OUTPUT_DIR + "/", "");
			String target = str[1].replaceAll("_Exact.txt", "");
			String s = source.replaceAll("andre_", "");
			String t = target.replaceAll("andre_", "");
			//Approach do Edgard aqui.
			// source = getURIDomainEdgard(source)
			// target = getURIDomainEdgard(target)
			if (setAlreadyIncluded.contains(s + "---" + t) || setAlreadyIncluded.contains(t + "---" + s)) {
				continue;
			}
			setAlreadyIncluded.add(s + "---" + t);
			setAlreadyIncluded.add(t + "---" + s);
			if ((mapSim.size() > 0) && (mapSim.get(fNameSim) != null) && (mapSim.get(fNameSim).size() > 0)) {
				writer.println(s + "\t" + t + "\t" + propExact.size() + "\t" + mapSim.get(fNameSim).size() + "\t"
						+ mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
				for (Entry<String, String> p : mapSim.get(fNameSim).entrySet()) {
					String pS = p.getKey();
					String pT = p.getValue();
					writerSim.println(pS + "\t" + pT + "\t" + s + "\t" + t);
				}
				for (String pExact : propExact) {
					writerExact.println(pExact + "\t" + s + "\t" + t);
				}
			} else {
				writer.println(s + "\t" + t + "\t" + propExact.size() + "\t" + 0 + "\t"
						+ mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
				for (String pExact : propExact) {
					writerExact.println(pExact + "\t" + s + "\t" + t);
				}
			}
		}
		writer.close();
		writerExact.close();
		writerSim.close();
	}

	private static boolean alreadyCompared(String source, String target) {
		if (mAlreadyCompared.get(source) != null) {
			if (mAlreadyCompared.get(source).equals(target)) {
				return true;
			}
		}

		return false;
	}

	public static Map<String, String> getSampleQueries(File file) {
		Map<String, String> ret = new LinkedHashMap<String, String>();
		try {
			List<String> lstLines = FileUtils.readLines(file, "UTF-8");
			String query = "";
			for (String line : lstLines) {
				// if(!line.equals("§")){
				if (!line.startsWith("#-")) {
					query += line + "\n";
				} else {
					ret.put(line.replaceAll("#-", ""), query);
					// ret.add(query);
					query = "";
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}

	private static Map<String, Set<String>> getExactMatches(String source, String target)
			throws FileNotFoundException, UnsupportedEncodingException {
		final Set<String> propsSource = new LinkedHashSet<String>();
		final Set<String> propsTarget = new LinkedHashSet<String>();
		final Set<String> propsMatched = new LinkedHashSet<String>();
		final Map<String, Set<String>> mapExactMatch = new LinkedHashMap<String, Set<String>>();
		String s[] = source.split("/");
		String fSource = null;
		String fTarget = null;
		if (s.length > 2) {
			fSource = s[2] + "_" + s[s.length - 1];
		} else {
			fSource = s[s.length - 1];
		}
		String t[] = target.split("/");
		if (t.length > 2) {
			fTarget = t[2] + "_" + t[t.length - 1];
		} else {
			fTarget = t[t.length - 1];
		}
		// final String fileName = OUTPUT_DIR + "/" + fSource + "---" + fTarget +
		// "_Exact.txt";
		final String fileName = fSource + "---" + fTarget + "_Exact.txt";
		propsSource.addAll(getProps(source, fSource));
		propsTarget.addAll(getProps(target, fTarget));
		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapExactMatch;
		}

		propsSource.parallelStream().forEach(pSource -> {
			propsTarget.parallelStream().forEach(pTarget -> {
				if (pSource.equalsIgnoreCase(pTarget)) {
					propsMatched.add(pSource);
				}
			});
		});
		mapExactMatch.put(fileName, propsMatched);
		// writer.close();
		return mapExactMatch;
	}

	private static Set<String> getProps(String source, String fName) {
		//Colocar o approach do Claus aqui...
		//instead of execute the SPARQL at the Dataset, we query the Dataset Catalog from Claus to obtain a list of properties and classes.
		//This should be faster then query the dataset, because there are some datasets/Endpoints extremely slow, more than 3 minutes.
		//return getPropsClaus(source)
		String cSparqlP = "Select DISTINCT ?p where {?s ?p ?o}";
		String cSparqlC = "select distinct ?p where {[] a ?p}";
		Set<String> ret = new LinkedHashSet<String>();
		if (!IN_MEMORY) {
			ret.addAll(execSparql(cSparqlP, source));
			ret.addAll(execSparql(cSparqlC, source));
			return ret;
		}

		if (mapDatasetProperties.containsKey(fName)) {
			return mapDatasetProperties.get(fName);
		}
		ret.addAll(execSparql(cSparqlP, source));
		ret.addAll(execSparql(cSparqlC, source));
		mapDatasetProperties.put(fName, ret);
		return mapDatasetProperties.get(fName);
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();
		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(900000); // 15 minutes
			Runnable block = new Runnable() {
				public void run() {
					// ret.addAll(Util.execQueryEndPoint(cSparql, source));
					if (Util.isEndPoint(source)) {
						ret.addAll(Util.execQueryEndPoint(cSparql, source));
					} else {
						ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.err.println("TIME-OUT-ERROR - dataset/source: " + source);
		}
		System.out.println("#Props: " + ret.size());
		if (ret.size() < 1) {
			mapDsError.put(source, "Unable to retrieve properties via Java.");
		}
		return ret;
	}
}
