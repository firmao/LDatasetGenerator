package test.testid;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.sparql.core.Prologue;

public class ExperimentSparql {
	private String sparqlQuery, ds;
	private Set<String> manyDt;

	public Set<String> getManyDt() {
		return manyDt;
	}

	public void setManyDt(Set<String> manyDt) {
		this.manyDt = manyDt;
	}

	public String getSparqlQuery() {
		return sparqlQuery;
	}

	public void setSparqlQuery(String sparqlQuery) {
		this.sparqlQuery = sparqlQuery;
	}

	public String getDs() {
		return ds;
	}

	public void setDs(String ds) {
		this.ds = ds;
	}

	public void execute() throws InterruptedException, IOException {
		final Map<String, String> mapProps = new HashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
		final Map<String, Map<String, String>> newMapPropsDs = new HashMap<String, Map<String, String>>();

		PrintWriter writer = new PrintWriter("SparqlMatching.txt", "UTF-8");
		long numberTriples = 0;
		writer.println(getSparqlQuery());
		Set<String> resultsSPARQL = new LinkedHashSet<String>();

		Set<String> datasets = null;
		if(getManyDt() != null) {
			datasets = getManyDt();
		}else {
			datasets = getDatasetsFromWimu(getSparqlQuery());
		}

		mapProps.putAll(extractURIS(getSparqlQuery()));
		mapPropsDs.put(getDs(), mapProps);
		for (String dt : datasets) {
			Map<String, String> props = null;
			try {
				props = getProps(mapProps, getDs(), dt);
				if (mapPropsDs.get(dt) != null) {
					mapPropsDs.get(dt).putAll(props);
				} else {
					mapPropsDs.put(dt, props);
				}
			} catch (Exception e) {
				if (e.getMessage().contains("Adjacency list bitmap")) {
					writer.println("Empty dataset: " + dt);
					continue;
				}
			}
			if (SparqlMatching.mapDsNumTriples.get(dt) != null) {
				numberTriples = SparqlMatching.mapDsNumTriples.get(dt);
				SparqlMatching.totalNumTriples += numberTriples;
			}
			Set<String> retBefore = SparqlMatching.execSparql(getSparqlQuery(), getDs());
			Set<String> retAfter = new LinkedHashSet<String>();
			if ((props == null) || (props.size() < 1)) {
				writer.println("No matches with dataset target: " + dt);
				// continue;
			} else {
				String nSparql = SparqlMatching.replaceURIs(getSparqlQuery(), mapPropsDs.get(dt));
				retAfter.addAll(SparqlMatching.execSparql(nSparql, dt));
				resultsSPARQL.addAll(retAfter);
			}
			resultsSPARQL.addAll(retBefore);
			writer.println("***************");
			writer.println("Dataset Source: " + getDs());
			if(props != null) {
				writer.println("Properties matched: " + props);
				newMapPropsDs.put(dt, props);
			}
			writer.println("Number results before:" + retBefore.size());
			writer.println("Number results after:" + retAfter.size());
			writer.println("Different ontology/property/attribute_name: YES");
			writer.println("Number of triples: " + numberTriples);
			writer.println("***************");
		}

		writer.println("Total number of triples: " + SparqlMatching.totalNumTriples);
		writer.println("Number of results Total after the schema aligment: " + resultsSPARQL.size());
		writer.close();
		// writeDsPropsMatched(newMapPropsDs);
		// writeHDTResult(resultsSPARQL);
	}

	/*
	 * Return a map with the equivalent URIs prop in the dataset Target.
	 */
	private static Map<String, String> getProps(Map<String, String> mProps, String dsS, String dsT) throws IOException {
		Map<String, String> mRet = new HashMap<String, String>();

		for (Entry<String, String> entry : mProps.entrySet()) {
			String prop = entry.getKey().trim();
			Set<String> equivProps = PropertyMatching.getEquivProp(prop, dsS, dsT);
			if (equivProps != null) {
				for (String equivProp : equivProps) {
					mRet.put(prop, equivProp);
				}
			}
		}

		return mRet;
	}

	private static Map<String, String> extractURIS(String cSparql) throws UnsupportedEncodingException {
		Map<String, String> containedUrls = new HashMap<String, String>();

		String fixSparql = replacePrefixes(cSparql);

		String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
		Matcher urlMatcher = pattern.matcher(fixSparql);

		while (urlMatcher.find()) {
			String key = fixSparql.substring(urlMatcher.start(0), urlMatcher.end(0));
			containedUrls.put(key, null);
		}

		return containedUrls;
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

	private Set<String> getDatasetsFromWimu(String cSparql) throws InterruptedException, IOException {
		Set<String> setDatasets = new LinkedHashSet<String>();
		Set<String> setURIs = obtainURIs(cSparql);

		for (String uri : setURIs) {
			setDatasets.addAll(WimuUtil.getDsWIMUs(uri));
		}
		return setDatasets;
	}

	public Set<String> obtainURIs(String query) throws UnsupportedEncodingException {
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

		Set<String> containedUrls = new LinkedHashSet<String>();

		String fixSparql = q.serialize();

		String urlRegex = "((https?|ftp|gopher|telnet|file):((//)|(\\\\))+[\\w\\d:#@%/;$()~_?\\+-=\\\\\\.&]*)";
		Pattern pattern = Pattern.compile(urlRegex, Pattern.CASE_INSENSITIVE);
		Matcher urlMatcher = pattern.matcher(fixSparql);

		while (urlMatcher.find()) {
			String key = fixSparql.substring(urlMatcher.start(0), urlMatcher.end(0));
			containedUrls.add(key);
		}
		return containedUrls;
	}

	public Set<String> getDatasets(File file, int limit) {
		Set<String> ret = new LinkedHashSet<String>();
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			int count = 0;
			for (File source : files) {
				if (source.isFile()) {
					if (count >= limit)
						break;
					if (source.getName().endsWith(".hdt")) {
						ret.add(source.getAbsolutePath());
						count++;
					}
				}
			}
		} else {
			System.err.println(file.getName() + " is not a directory !");
		}

		return ret;
	}

}
