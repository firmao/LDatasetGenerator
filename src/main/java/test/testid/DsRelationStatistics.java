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
import java.util.Set;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.time.StopWatch;

public class DsRelationStatistics {

	public static final Map<String, String> mAlreadyCompared = new LinkedHashMap<String, String>();
	public static final Map<String, Set<String>> mapDatasetProperties = new LinkedHashMap<String, Set<String>>();
	public static final boolean IN_MEMORY = true;
	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		System.out.println("IN_MEMORY: " + IN_MEMORY);
		Map<String, String> mapQuerySource = getSampleQueries(new File("queryDsInfo.txt"));
		Set<String> dt = new LinkedHashSet<String>();
		dt.addAll(mapQuerySource.keySet());
		final Map<String, Set<String>> mapExactMatch = new LinkedHashMap<String, Set<String>>();
		final Map<String, Set<String>> mapSim = new LinkedHashMap<String, Set<String>>();
		for (String source : mapQuerySource.keySet()) {
			for (String target : dt) {
				if (source.equals(target))
					continue;
				if (alreadyCompared(source, target))
					continue;
				mapExactMatch.putAll(getExactMatches(source, target));
				// mapSim.putAll(getSimMatches(source, target));
				mAlreadyCompared.put(source, target);
			}
		}
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
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
		propsSource.addAll(getProps(source));
		propsTarget.addAll(getProps(target));
		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapExactMatch;
		}
		String s[] = source.split("/");
		String fSource = s[2] + "_" + s[s.length - 1];
		String t[] = target.split("/");
		String fTarget = t[2] + "_" + t[t.length - 1];
		final String fileName = "outMatches/" + fSource + "---" + fTarget + "_Exact.txt";
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		for (String pSource : propsSource) {
			for (String pTarget : propsTarget) {
				if (pSource.equalsIgnoreCase(pTarget)) {
					propsMatched.add(pSource);
					writer.println(pSource);
				}
			}
		}
		mapExactMatch.put(fileName, propsMatched);
		writer.close();
		return mapExactMatch;
	}

	private static Set<String> getProps(String source) {
		if(!IN_MEMORY) {
			String cSparql = "Select DISTINCT ?p where {?s ?p ?o}";
			return execSparql(cSparql, source);
		}
		
		if(mapDatasetProperties.containsKey(source)) {
			return mapDatasetProperties.get(source);
		}
		String cSparql = "Select DISTINCT ?p where {?s ?p ?o}";
		mapDatasetProperties.put(source, execSparql(cSparql, source));
		return mapDatasetProperties.get(source);
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();

		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(600000); // 10 minutes
			Runnable block = new Runnable() {
				public void run() {
					ret.addAll(Util.execQueryEndPoint(cSparql, source));
			//		if (Util.isEndPoint(source)) {
			//			ret.addAll(Util.execQueryEndPoint(cSparql, source));
			//		} else {
			//			ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
			//		}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.err.println("TIME-OUT-ERROR - dataset/source: " + source);
		}
		return ret;
	}
}