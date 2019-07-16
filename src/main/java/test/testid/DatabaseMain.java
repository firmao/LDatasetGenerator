package test.testid;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;

public class DatabaseMain {
	public static String TABLEMATCHING_PATH = "/media/andre/DATA/Dropbox/ws1/LDatasetGenerator/out_tests2/tableMatches.tsv";
	public static String TABLEMATCHES_EXACT = "/media/andre/DATA/Dropbox/ws1/LDatasetGenerator/out_tests2/tableMatches_Exact.tsv";
	public static String TABLEMATCHES_SIM = "/media/andre/DATA/Dropbox/ws1/LDatasetGenerator/out_tests2/tableMatches_Sim.tsv";
	
	public static void main(String[] args) throws IOException {
		String dataset = "684434e8ddfd4590cd188a3178f0b9ae.hdt";
		Set<String> ret = searchDB(dataset);
		System.out.println("Dataset: " + dataset);
		ret.forEach(System.out::println);

		// String dataset = "SELECT ?s WHERE { ?s a <http://dbpedia.org/ontology/City>
		// }";
//		Set<String> properties = new LinkedHashSet<String>();
//		properties.add("http://purl.org/dc/terms/date");
//		properties.add("http://crime.rkbexplorer.com/id/location");
//		properties.add("http://purl.org/dc/terms/subject");
//		Set<String> retProp = searchDB(properties);
//		retProp.forEach(System.out::println);
	}

	public static void add(Map<String, Set<String>> mapExactMatch, Map<String, Map<String, String>> mapSim) {
		for (Entry<String, Set<String>> entry : mapExactMatch.entrySet()) {
			String fNameExact = entry.getKey();
			Set<String> props = entry.getValue();
			String fNameSim = fNameExact.replaceAll("_Exact.txt", "_Sim.tsv");
			String[] str = fNameExact.replaceAll("_Exact.txt", "").split("---");
			String source = str[0].replaceAll(IndexCreator.OUTPUT_DIR + "/", "");
			String target = str[1].replaceAll("_Exact.txt", "");
			if (!addDB(source, target, props.size(), mapSim.get(fNameSim).size())) {
				System.err.println("Problem source: " + source + " target: " + target);
			}

//			if((mapSim.size() > 0) && (mapSim.get(fNameSim) != null)) {
//				writer.println(fNameExact + "\t" + props.size() + "\t" + mapSim.get(fNameSim).size()+ "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
//			} else {
//				writer.println(fNameExact + "\t" + props.size() + "\t" + 0 + "\t" + mapDatasetProperties.get(source).size() + "\t" + mapDatasetProperties.get(target).size());
//			}
		}
	}

	public static boolean addDB(String source, String target, int sizeExact, int sizeSim) {
		boolean ret = false;

		return ret;
	}

	public static Set<String> getResults(String dataset) throws IOException {
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
	
	public static Set<String> searchDB(String dataset) throws IOException {
		if (isSparql(dataset)) {
			Set<String> properties = extractProperties(dataset);
			return searchDB(properties);
		}
		Set<String> ret1 = new LinkedHashSet<String>();
		try (Stream<String> lines = Files.lines(Paths.get(TABLEMATCHING_PATH))) {
			ret1 = lines.filter(line -> line.contains(dataset)).map(String::toUpperCase).collect(Collectors.toSet());
		}
		Set<String> ret = new LinkedHashSet<String>();
		ret.add("Dataset\t#ExactMatch\t#SimMatch");
		for (String line : ret1) {
			String s1 = line.replaceAll("OUT_TESTS/", "");
			s1 = s1.replaceAll("ANDRE_", "");
			s1 = s1.replaceAll("_EXACT.TXT", "");
			String split1[] = s1.split("\t");
			String split2[] = split1[0].split("---");
			for (String st : split2) {
				if (!st.contains(dataset.toUpperCase())) {
					int numExact = Integer.parseInt(split1[1].trim());
					int numSim = Integer.parseInt(split1[2].trim());
					if ((numExact > 0) || (numSim > 0)) {
						String sRet = st + "\t" + split1[1] + "\t" + split1[2];
						ret.add(sRet);
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

	public static Set<String> searchDB(Set<String> properties) throws IOException {
		Set<String> ret = new LinkedHashSet<String>();

		Set<String> propExact = new LinkedHashSet<String>();
		for (String prop : properties) {
			try (Stream<String> lines = Files
					.lines(Paths.get(TABLEMATCHES_EXACT))) {

				propExact.addAll(
						lines.filter(line -> line.contains(prop)).map(String::toUpperCase).collect(Collectors.toSet()));
			}
		}
		Set<String> propSim = new LinkedHashSet<String>();
		for (String prop : properties) {
			try (Stream<String> lines = Files
					.lines(Paths.get(TABLEMATCHES_SIM))) {
				propSim.addAll(
						lines.filter(line -> line.contains(prop)).map(String::toUpperCase).collect(Collectors.toSet()));
			}
		}

		Set<String> datasetsExact = new LinkedHashSet<String>();
		Map<String, Set<String>> mExact = new LinkedHashMap<String, Set<String>>();
		for (String pExact : propExact) {
			String s [] = pExact.split("\t");
			datasetsExact.add(s[1].replaceAll("ANDRE_", ""));
			datasetsExact.add(s[2].replaceAll("ANDRE_", ""));
			if(mExact.containsKey(s[0].trim())) {
				mExact.get(s[0]).addAll(datasetsExact);
			} else {
				mExact.put(s[0], datasetsExact);
			}
		}
		
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

}
