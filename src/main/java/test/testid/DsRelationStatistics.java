package test.testid;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.text.similarity.JaccardSimilarity;

public class DsRelationStatistics {

	public static final Map<String, String> mAlreadyCompared = new LinkedHashMap<String, String>();
	public static final Map<String, Set<String>> mapDatasetProperties = new LinkedHashMap<String, Set<String>>();
	public static final boolean IN_MEMORY = true;
	public static final String OUTPUT_DIR = "out_tests2";
	public static Map<String, String> mapDsError = new LinkedHashMap<String, String>();
	public static void main(String[] args) throws Exception {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		File fDir = new File(OUTPUT_DIR);
		if(!fDir.exists()) {
			fDir.mkdir();
		}
		System.out.println("IN_MEMORY: " + IN_MEMORY);
		Map<String, String> mapQuerySource = getSampleQueries(new File("queryDsInfo.txt"));
		ExperimentNN exp = new ExperimentNN();
		Set<String> ds = exp.getDatasets(new File("dirHDT"), 10000);
		ds.addAll(getEndpoints(new File("endpoints.txt")));
		//ds.addAll(mapQuerySource.keySet());
		Set<String> dt = new LinkedHashSet<String>();
		//dt.addAll(mapQuerySource.keySet());
		dt.addAll(ds);
		final Map<String, Set<String>> mapExactMatch = new LinkedHashMap<String, Set<String>>();
		final Map<String, Map<String, String>> mapSim = new LinkedHashMap<String, Map<String, String>>();
		
		//for (String source : mapQuerySource.keySet()) {
		int totalComparisons = ds.size() * dt.size();
		int count = 0;
		for (String source : ds) {
			for (String target : dt) {
				System.out.println("Starting comparison: " + count + " from " + totalComparisons);
				count++;
				if (source.equals(target))
					continue;
				if (alreadyCompared(source, target))
					continue;
				mapExactMatch.putAll(getExactMatches(source, target));
				mapSim.putAll(getSimMatches(source, target, 0.9, mapExactMatch));
				//printMap(mapExactMatch, mapSim, OUTPUT_DIR + "/tableMatches.tsv");
				mAlreadyCompared.put(source, target);
				mAlreadyCompared.put(target, source);
			}
		}
		String sFile = OUTPUT_DIR + "/tableMatches.tsv"; 
		System.out.println("Printing file: " + sFile);
		printMap(mapExactMatch, mapSim, sFile);
		System.out.println("Printing errors...");
		printErrors(mapDsError);
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
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

	private static void printErrors(Map<String, String> mapDsError2) throws FileNotFoundException, UnsupportedEncodingException {
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

	private static Map<String, Map<String, String>> getSimMatches(String source, String target, double threshold, Map<String, Set<String>> mapExactMatch) 
			throws FileNotFoundException, UnsupportedEncodingException {
		final Set<String> propsSource = new LinkedHashSet<String>();
		final Set<String> propsTarget = new LinkedHashSet<String>();
		final Map<String, String> propsMatched = new LinkedHashMap<String, String>();
		final Map<String, Map<String, String>> mapSim = new LinkedHashMap<String, Map<String, String>>();
		
		String s[] = source.split("/");
		String fSource = s[2] + "_" + s[s.length - 1];
		String t[] = target.split("/");
		String fTarget = t[2] + "_" + t[t.length - 1];
		final String fileName = OUTPUT_DIR + "/" + fSource + "---" + fTarget + "_Sim.tsv";
		
		propsSource.addAll(getProps(source, fSource));
		propsTarget.addAll(getProps(target, fTarget));
		
		for (Set<String> done : mapExactMatch.values()) {
			propsSource.removeAll(done);
			propsTarget.removeAll(done);
		}
		
		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapSim;
		}
		
		//PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		JaccardSimilarity sim = new JaccardSimilarity();
		for (String pSource : propsSource) {
			for (String pTarget : propsTarget) {
				//if (pSource.equalsIgnoreCase(pTarget)) {
				String p1 = Util.getURLName(pSource);
				String p2 = Util.getURLName(pTarget);
				double dSim = sim.apply(p1, p2);
				if (dSim >= threshold) {
					propsMatched.put(pSource, pTarget);
					//writer.println(pSource + "\t" + pTarget);
				}
			}
		}
		mapSim.put(fileName, propsMatched);
		//writer.close();
		return mapSim;
	}

	private static void printMap(Map<String, Set<String>> mapExactMatch, Map<String, Map<String, String>> mapSim, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
			PrintWriter writer = new PrintWriter(fileName, "UTF-8");
			PrintWriter writerExact = new PrintWriter(fileName.replaceAll(".tsv", "_Exact.tsv"), "UTF-8");
			PrintWriter writerSim = new PrintWriter(fileName.replaceAll(".tsv", "_Sim.tsv"), "UTF-8");
			writer.println("FileName\t#ExactMatch\t#sim>0.9\t#PropDs\t#PropDt");
			writerExact.println("Property\tSource\tTarget");
			writerSim.println("PropertyS\tPropertyT\tSource\tTarget");
			for (Entry<String, Set<String>> entry : mapExactMatch.entrySet()) {
				String fNameExact = entry.getKey();
				String fNameSim = fNameExact.replaceAll("_Exact.txt", "_Sim.tsv");
				Set<String> propExact = entry.getValue();
				String [] str = fNameExact.replaceAll("_Exact.txt", "").split("---");
				String source = str[0].replaceAll(OUTPUT_DIR + "/", "");
				String target = str[1].replaceAll("_Exact.txt", "");
				String s = source.replaceAll("andre_", "");
				String t = target.replaceAll("andre_", "");
				if((mapSim.size() > 0) && (mapSim.get(fNameSim) != null)) {
					writer.println(fNameExact + "\t" + propExact.size() + "\t" + mapSim.get(fNameSim).size()+ "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
					for (Entry<String, String> p : mapSim.get(fNameSim).entrySet()) {
						String pS = p.getKey();
						String pT = p.getValue();
						writerSim.println(pS + "\t" + pT + "\t" + s + "\t" + t);
					}
				} else {
					writer.println(fNameExact + "\t" + propExact.size() + "\t" + 0 + "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
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
		String fSource = s[2] + "_" + s[s.length - 1];
		String t[] = target.split("/");
		String fTarget = t[2] + "_" + t[t.length - 1];
		final String fileName = OUTPUT_DIR + "/" + fSource + "---" + fTarget + "_Exact.txt";
		propsSource.addAll(getProps(source, fSource));
		propsTarget.addAll(getProps(target, fTarget));
		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapExactMatch;
		}
		
		//PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		for (String pSource : propsSource) {
			for (String pTarget : propsTarget) {
				if (pSource.equalsIgnoreCase(pTarget)) {
					propsMatched.add(pSource);
					//writer.println(pSource);
				}
			}
		}
		mapExactMatch.put(fileName, propsMatched);
		//writer.close();
		return mapExactMatch;
	}

	private static Set<String> getProps(String source, String fName) {
		if(!IN_MEMORY) {
			String cSparql = "Select DISTINCT ?p where {?s ?p ?o}";
			return execSparql(cSparql, source);
		}
		
		if(mapDatasetProperties.containsKey(fName)) {
			return mapDatasetProperties.get(fName);
		}
		String cSparql = "Select DISTINCT ?p where {?s ?p ?o}";
		mapDatasetProperties.put(fName, execSparql(cSparql, source));
		return mapDatasetProperties.get(fName);
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();

		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(600000); // 10 minutes
			Runnable block = new Runnable() {
				public void run() {
			//		ret.addAll(Util.execQueryEndPoint(cSparql, source));
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
		if(ret.size() < 1) {
			mapDsError.put(source, "Unable to retrieve properties via Java.");
		}
		return ret;
	}
}
