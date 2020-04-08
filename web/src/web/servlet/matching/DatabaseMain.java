package web.servlet.matching;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.UnsupportedEncodingException;
import java.net.URL;
import java.net.URLEncoder;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;

import com.google.gson.Gson;

public class DatabaseMain {
	public static final String DIR_DB = readConfFile("/home/andre/dsconfig.txt");
	public static String TABLEMATCHING_PATH = DIR_DB + "tableMatches.tsv";
	public static String TABLEMATCHES_EXACT = DIR_DB + "tableMatches_Exact.tsv";
	public static String TABLEMATCHES_SIM = DIR_DB + "tableMatches_Sim.tsv";
	public static Map<String, String> mUriDataset = new HashMap<String, String>();
	
	public static void main(String[] args) throws IOException, InterruptedException {
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String cSparql = "select distinct (count(?p) as ?c) where {?s ?p ?o}";
		String endPoint = "https://dbpedia.org/sparql";
		Set<String> sTest = MatchingServlet.execQueryEndPoint(cSparql, endPoint);
		System.out.println(sTest);
		System.exit(0);
		String dataset = "SELECT  ?o2\n" + 
				"WHERE\n" + 
				"  { <http://dbpedia.org/resource/Germany> <http://dbpedia.org/ontology/capital> ?o .\n" + 
				"    ?o <http://dbpedia.org/ontology/populationTotal> ?o2\n" + 
				"  }";
//		String dataset = "6e9e97fa4f1c9ba28ae0dd3786c2de41.hdt";
		Set<String> ret = searchDB(dataset);
		System.out.println("Dataset: " + dataset);
		ret.forEach(System.out::println);

//		Set<String> properties = new LinkedHashSet<String>();
//		properties.add("http://purl.org/dc/terms/date");
//		properties.add("http://crime.rkbexplorer.com/id/location");
//		properties.add("http://purl.org/dc/terms/subject");
//		Set<String> retProp = searchDB(properties);
//		retProp.forEach(System.out::println);
		System.out.println("Statistics:");
		printStatistics(getStatistics());
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}


	private static void printStatistics(ObjStatistics stat) {
		System.out.println("total number of properties matched: " + stat.getPropertiesMatched().size());
		System.out.println("Total number of datasets: " + stat.getTotalNumberDatasets());
	}


	private static String readConfFile(String pathFile) {
		String ret = null;
		List<String> lstLines = null;
		try {
			lstLines = Files.readAllLines(Paths.get(pathFile));
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		for (String string : lstLines) {
			ret=string;
		}
		
		return ret;
	}


	public static Set<String> getResults(String dataset) throws IOException, InterruptedException {
		Set<String> results = searchDB(dataset);
		if (results == null) {
			results = new LinkedHashSet<String>();
			results.add("Dataset not found in our index, please add to the index queue");
		}
		return results;
	}

	public static boolean isSparql(String cSparql) {
		return cSparql.toLowerCase().contains("select");
	}
	
	public static Set<String> extractProperties(String cSparql) throws UnsupportedEncodingException {
		Set<String> ret = new LinkedHashSet<String>();
		String fixSparql = replacePrefixes(cSparql);

		String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
		Matcher urlMatcher = pattern.matcher(fixSparql);

		while (urlMatcher.find()) {
			String key = fixSparql.substring(urlMatcher.start(0), urlMatcher.end(0));
			ret.add(key);
		}

		return ret;
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
	
	public static Set<String> searchDB(String dataset) throws IOException, InterruptedException {
		if (isSparql(dataset)) {
			Set<String> properties = extractProperties(dataset);
			return searchDB(properties);
		}
		Set<String> ret1 = new LinkedHashSet<String>();
		try (Stream<String> lines = Files.lines(Paths.get(TABLEMATCHING_PATH))) {
			ret1.addAll(lines.filter(line -> line.contains(dataset.toLowerCase())).map(String::toLowerCase).collect(Collectors.toSet()));
		}
		Set<String> ret = new LinkedHashSet<String>();
		Set<String> sources = new LinkedHashSet<String>();
		ret.add("Dataset\t#ExactMatch\t#SimMatch");		
		for (String line : ret1) {
			String s1 = line.replaceAll("OUT_TESTS/", "");
			s1 = s1.replaceAll("ANDRE_", "");
			s1 = s1.replaceAll("_EXACT.TXT", "");
			String split1[] = s1.split("\t");
			String split2[] = split1[0].split("---");
			for (String st : split2) {
				if (!st.contains(dataset.toUpperCase())) {
					int numExact = Integer.parseInt(split1[2].trim());
					int numSim = Integer.parseInt(split1[3].trim());
					if ((numExact > 0) || (numSim > 0)) {
						String sRet = st + "\t" + numExact + "\t" + numSim;
						if(sources.add(st)) {
							ret.add(sRet);
						}
					}
				}
			}
		}

		return ret;
	}

//	public static Set<String> getResults(Set<String> properties) {
//		Set<String> results = searchDB(properties);
//		if (results == null) {
//			results = new LinkedHashSet<String>();
//			results.add(
//					"Properties not found in our index, please add a dataset with thosse properties to the index queue");
//		}
//		return results;
//	}

	public static Set<String> searchDB(Set<String> properties) throws IOException, InterruptedException {
		Set<String> ret = new LinkedHashSet<String>();

		Set<String> propExact = new LinkedHashSet<String>();
		Set<String> propSim = new LinkedHashSet<String>();
		for (String prop : properties) {
			try (Stream<String> lines = Files
					.lines(Paths.get(TABLEMATCHES_EXACT))) {

				propExact.addAll(
						lines.filter(line -> line.contains(prop.toLowerCase())).map(String::toLowerCase).collect(Collectors.toSet()));
			}
		}
		
		for (String prop : properties) {
			try (Stream<String> lines = Files
					.lines(Paths.get(TABLEMATCHES_SIM))) {
				propSim.addAll(
						lines.filter(line -> line.contains(prop.toLowerCase())).map(String::toLowerCase).collect(Collectors.toSet()));
			}
		}

		Set<String> datasetsExact = new LinkedHashSet<String>();
		Map<String, Set<String>> mExact = new LinkedHashMap<String, Set<String>>();
		for (String pExact : propExact) {
			String s [] = pExact.split("\t");
			String d1 = s[1].replaceAll("ANDRE_", "");
			String d2 = s[2].replaceAll("ANDRE_", "");
			datasetsExact.add(d1);
			datasetsExact.add(d2);
			//datasetsExact.addAll(getDsWIMUs(d1));
			//datasetsExact.addAll(getDsWIMUs(d2));
			if(mExact.containsKey(s[0].trim())) {
				mExact.get(s[0]).addAll(datasetsExact);
			} else {
				mExact.put(s[0], datasetsExact);
			}
		}
		System.out.println("datasetsExact: " + datasetsExact.size());
//		Set<String> datasetsSim = new LinkedHashSet<String>();
//		Map<Map<String, String>, Map<String, String>> mExact = new LinkedHashMapMap<Map<String, String>, Map<String, String>>();
//		for (String pSim : propSim) {
//			String s [] = pSim.split("\t");
//			mExact.p
//		}
		ret.add(mExact.toString());
		ret.addAll(propSim);
		return ret;
	}
	
	public static Set<String> getDsWIMUs(String pUri) throws InterruptedException, IOException {
		Set<String> sRet = new HashSet<String>();
		String uri = pUri;
		if(uri.contains("sparql") && !uri.startsWith("http")) {
			uri = "http://" + uri;
		}
		
		uri = uri.toLowerCase().replaceAll("_sparql", "/sparql");
		
//		if(uri.endsWith(".hdt")) {
//			uri = getURIFromHDT(uri);
//		}
		
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


	/*
	 * Get URI from the header or from the first subject.
	 
	private static String getURIFromHDT(String uri) {
		
		return NEED TO IMPLMENT;
	}*/


	public static ObjStatistics getStatistics() throws IOException {
		ObjStatistics ret = new ObjStatistics();
		ret.setProperties(new LinkedHashSet<String>());
		ret.setPropertiesMatched(new LinkedHashSet<String>());
		ret.setTop10DatasetsMoreSimilar(new LinkedHashSet<String>());
		Set<String> datasets = new LinkedHashSet<String>();
		
		
		try (Stream<String> lines = Files
				.lines(Paths.get(TABLEMATCHES_EXACT))) {
			
			lines.parallel().forEach(line -> {
				String s[] = line.toLowerCase().split("\t");
				String p = s[0].trim();
				String ds = s[1].trim();
				String dt = s[2].trim();
				datasets.add(ds);
				datasets.add(dt);
				ret.getPropertiesMatched().add(p);
			});
		}
		
		
		try (Stream<String> lines = Files
				.lines(Paths.get(TABLEMATCHES_SIM))) {
			
			lines.parallel().forEach(line -> {
				String s[] = line.toLowerCase().split("\t");
				String ps = s[0].trim();
				String pt = s[1].trim();
				String ds = s[2].trim();
				String dt = s[3].trim();
				datasets.add(ds);
				datasets.add(dt);
				ret.getPropertiesMatched().add(ps);
				ret.getPropertiesMatched().add(pt);
			});
		}

		ret.setTotalNumberDatasets(datasets.size());
		return ret;
	}

}
