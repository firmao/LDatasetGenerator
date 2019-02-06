package test.testid;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.TimeUnit;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
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
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdtjena.HDTGraph;
import org.tukaani.xz.XZInputStream;

public class Util {

	/*
	 * Save the resources in a file
	 */
	public static void writeFile(Set<String> resources, File fResources) throws IOException {
		if (!fResources.exists())
			fResources.createNewFile();
		PrintWriter writer = new PrintWriter(fResources.getName(), "UTF-8");
		for (String resource : resources) {
			writer.println(resource.replaceAll("\n", "").trim());
		}
		writer.close();
	}

	public static void writeFile(String resource, Map<String, Set<String>> mapPropValue)
			throws FileNotFoundException, UnsupportedEncodingException {
		String s[] = resource.split("/");
		String fileName = "out/" + s[2] + "_" + s[s.length - 1] + ".tsv";
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#---Properties and values from: " + resource);
		writer.println("Property\tValue");
		for (Entry<String, Set<String>> entry : mapPropValue.entrySet()) {
			String prop = entry.getKey();
			Set<String> values = entry.getValue();
			if(values != null) {
				for (String value : values) {
					if(value.contains("@")){ 
						if(value.contains("@en")) {
							writer.println(prop + "\t" + value);
						}
					} else {
						writer.println(prop + "\t" + value);
					}
				}
			}
		}
		writer.close();
	}

	public static String getURLFileName(URL pURL) {
		String name = null;
		try {
			String[] str = pURL.getFile().split("/");
			name = str[str.length - 1];
			name = name.replaceAll("=", ".");
		} catch (Exception e) {
			System.err.println("Problem with URL: " + pURL);
		}
		return name;
	}

	public static File unconpress(File file) {
		File ret = file;
		try {
			File fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new File(file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new File(file.getName().replaceAll(".xz", ""));
			else if (file.getName().endsWith(".zip"))
				fUnzip = new File(file.getName().replaceAll(".zip", ""));
			else if (file.getName().endsWith(".tar.gz"))
				fUnzip = new File(file.getName().replaceAll(".tar.gz", ""));
			else if (file.getName().endsWith(".gz"))
				fUnzip = new File(file.getName().replaceAll(".gz", ""));
			else
				return file;

			if (fUnzip.exists()) {
				return fUnzip;
			}
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				synchronized (bz2In) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = bz2In.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					bz2In.close();
				}
			} else if (file.getName().endsWith(".xz")) {
				XZInputStream xzIn = new XZInputStream(in);
				synchronized (xzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = xzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					xzIn.close();
				}
			} else if (file.getName().endsWith(".zip")) {
				ZipArchiveInputStream zipIn = new ZipArchiveInputStream(in);
				synchronized (zipIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = zipIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					zipIn.close();
				}
			} else if (file.getName().endsWith(".tar.gz") || file.getName().endsWith(".gz")) {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				synchronized (gzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = gzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					gzIn.close();
				}
			}

			// file.delete();

			if (fUnzip != null)
				ret = fUnzip;
		} catch (Exception ex) {
			ret = file;
		}
		return ret;
	}

	/**
	 * Read the IRI content type by opening an HTTP connection. We set the value
	 * 'application/rdf+xml' to the ACCEPT request property for handling
	 * dereferenceable IRIs.
	 */
	public static String getContentType(String iri) {
		String contentType = "";
		try {
			URL url = new URL(iri);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("HEAD");
			connection.setRequestProperty("ACCEPT", "application/rdf+xml");
			connection.connect();
			contentType = connection.getContentType();
			if (contentType == null) {
				contentType = "";
			}
		} catch (MalformedURLException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return contentType;
	}

	public static Map<String, Set<String>> processUnzipRDF(File fUnzip, String resource) {
		Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		try {
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public void triple(Triple triple) {
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
			};
			RDFParserBuilder a = RDFParserBuilder.create();

			if (fUnzip.getName().endsWith(".tql")) {
				a.forceLang(Lang.NQUADS);
			} else if (fUnzip.getName().endsWith(".ttl")) {
				a.forceLang(Lang.TTL);
			} else {
				a.forceLang(Lang.RDFXML);
			}
			Scanner in = null;
			try {
				in = new Scanner(fUnzip);
				while (in.hasNextLine()) {
					a.source(new StringReader(in.nextLine()));
					try {
						a.parse(reader);
					} catch (Exception e) {
						// e.printStackTrace();
					}
				}
				in.close();
			} catch (FileNotFoundException e) {
				// e.printStackTrace();
			}
			fUnzip.delete();
		} catch (Exception ex) {
			System.err.println("Error: " + resource + ": " + ex.getMessage());
			return mPropValue;
		}
		return mPropValue;
	}

	public static void writeFileStatistics(Set<String> resources, String fName)
			throws FileNotFoundException, UnsupportedEncodingException, URISyntaxException {
		// Statistics
		List<String> list = new ArrayList<String>();
		for (String resource : resources) {
			String sourceDomain = getUrlDomain(resource);
			list.add(sourceDomain);
		}

		PrintWriter wStatistics = new PrintWriter(fName, "UTF-8");
		Set<String> unique = new HashSet<String>(list);
		for (String key : unique) {
			wStatistics.println(key + "\t" + Collections.frequency(list, key));
		}
		wStatistics.close();
	}

	public static String getUrlDomain(String url) {
		try {
			URI uri = new URI(url);
			String domain = uri.getHost();
			String[] domainArray = domain.split("\\.");
			if (domainArray.length == 1) {
				return domainArray[0];
			} else {
				return domainArray[domainArray.length - 2] + "." + domainArray[domainArray.length - 1];
			}
		} catch (Exception e) {
			return getUrlDomainSimple(url);
		}
	}

	private static String getUrlDomainSimple(String url) {
		String s[] = url.split("//");
		if (s.length > 0) {
			String domain = s[1].substring(0, s[1].indexOf("/"));
			return domain.trim();
		}
		return "noDomain";
	}

	public static Set<String> getNotDeref(Set<String> resources) {
		Set<String> ret = new HashSet<String>();
		for (String url : resources) {

			try {
				TimeOutBlock timeoutBlock = new TimeOutBlock(60000); // 1 minute
				Runnable block = new Runnable() {
					public void run() {
						if (!isGoodURL(url)) {
							ret.add(url);
						}
					}
				};
				timeoutBlock.addBlock(block);// execute the runnable block
			} catch (Throwable e) {
				System.out.println("TIME-OUT-ERROR - verifying URL: " + url);
			}

		}
		return ret;
	}

	private static boolean isGoodURL(String url) {
		try {
			HttpURLConnection connection = (HttpURLConnection) new URL(url).openConnection();
			connection.setRequestMethod("HEAD");
			int responseCode = connection.getResponseCode();
			if ((responseCode == 200) || (responseCode == 400)) {
				return true;
			} else
				return false;
		} catch (Exception e) {
			return false;
		}
	}

	public static boolean isEndPoint(String source) {
		return source.toLowerCase().contains("sparql");
	}

	public static Set<String> execQueryRDFRes(String cSparql, String dataset, int maxEntities) {
		final Set<String> ret = new HashSet<String>();
		File file = null;
		try {
			if (dataset.startsWith("http")) {
				URL url = new URL(dataset);
				file = new File(Util.getURLFileName(url));
				if (!file.exists()) {
					FileUtils.copyURLToFile(url, file);
				}
			} else {
				file = new File(dataset);
			}

			file = unconpress(file);
			// long limSize = 10000000; // 10 MB
			// if (file.length() > limSize) {
			// System.err.println("File: " + file.getAbsolutePath() + " is bigger than " +
			// limSize + " bytes");
			// ret.addAll(Util.execQueryEndPoint(cSparql, "http://dbpedia.org/sparql",
			// true));
			// return ret;
			// }
			if (file.getName().endsWith("hdt")) {
				return execQueryHDTRes(cSparql, file.getAbsolutePath(), maxEntities);
			}

			long start = System.currentTimeMillis();
			org.apache.jena.rdf.model.Model model = org.apache.jena.rdf.model.ModelFactory.createDefaultModel();
			org.apache.jena.sparql.engine.QueryExecutionBase qe = null;
			org.apache.jena.query.ResultSet resultSet = null;
			/* First check the IRI file extension */
			if (file.getName().toLowerCase().endsWith(".ntriples") || file.getName().toLowerCase().endsWith(".nt")) {
				System.out.println("# Reading a N-Triples file...");
				model.read(file.getAbsolutePath(), "N-TRIPLE");
			} else if (file.getName().toLowerCase().endsWith(".n3")) {
				System.out.println("# Reading a Notation3 (N3) file...");
				model.read(file.getAbsolutePath());
			} else if (file.getName().toLowerCase().endsWith(".json") || file.getName().toLowerCase().endsWith(".jsod")
					|| file.getName().toLowerCase().endsWith(".jsonld")) {
				System.out.println("# Trying to read a 'json-ld' file...");
				model.read(file.getAbsolutePath(), "JSON-LD");
			} else {
				String contentType = getContentType(dataset); // get the IRI
																// content type
				System.out.println("# IRI Content Type: " + contentType);
				if (contentType.contains("application/ld+json") || contentType.contains("application/json")
						|| contentType.contains("application/json+ld")) {
					System.out.println("# Trying to read a 'json-ld' file...");
					model.read(file.getAbsolutePath(), "JSON-LD");
				} else if (contentType.contains("application/n-triples")) {
					System.out.println("# Reading a N-Triples file...");
					model.read(file.getAbsolutePath(), "N-TRIPLE");
				} else if (contentType.contains("text/n3")) {
					System.out.println("# Reading a Notation3 (N3) file...");
					model.read(file.getAbsolutePath());
				} else {
					model.read(file.getAbsolutePath());
				}
			}

			qe = (org.apache.jena.sparql.engine.QueryExecutionBase) org.apache.jena.query.QueryExecutionFactory
					.create(cSparql, model);
			resultSet = qe.execSelect();
			if (resultSet != null) {
				List<QuerySolution> lQuerySolution = ResultSetFormatter.toList(resultSet);
				int count = 0;
				for (QuerySolution qSolution : lQuerySolution) {
					final StringBuffer sb = new StringBuffer();
					for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
						final String varName = varNames.next();
						
						if(qSolution.get(varName).isLiteral()) {
							String s = qSolution.get(varName).asLiteral().getString();
							sb.append(s);
						} else {
							sb.append(qSolution.get(varName).toString() + " ");
						}
						//sb.append(qSolution.get(varName).toString() + " ");
					}
					ret.add(sb.toString());
					count++;
					if((maxEntities != -1) && (count >= maxEntities)) {
						break;
					}
				}
			}
			long total = System.currentTimeMillis() - start;
			System.out.println("Time to query dataset: " + total + "ms");
			// file.delete();
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		if (ret.size() > 0) {
			Main.goodSources.add(dataset);
		}
		
		return ret;
	}

	public static Set<String> execQueryHDTRes(String cSparql, String dataset, int maxEntities) throws IOException {
		final Set<String> ret = new HashSet<String>();
		File file = null;
		HDT hdt = null;
		try {
			System.out.println("Dataset: " + dataset);
			long start = System.currentTimeMillis();
			if (dataset.startsWith("http")) {
				URL url = new URL(dataset);
				file = new File(Util.getURLFileName(url));
				if (!file.exists()) {
					FileUtils.copyURLToFile(url, file);
				}
			} else {
				file = new File(dataset);
			}
			file = unconpress(file);
//			long limSize = 10000000; // 10 MB
//			if (file.length() > limSize) {
//				System.err.println("File: " + file.getAbsolutePath() + " is bigger than " + limSize + " bytes");
//				ret.addAll(Util.execQueryEndPoint(cSparql, "http://dbpedia.org/sparql", true));
//				return ret;
//			}
			long total = System.currentTimeMillis() - start;
			System.out.println("Time to download dataset: " + total + "ms");
			start = System.currentTimeMillis();
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			List<QuerySolution> lQuerySolution = ResultSetFormatter.toList(results);
			int count = 0;
			for (QuerySolution qSolution : lQuerySolution) {
				final StringBuffer sb = new StringBuffer();
				for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
					final String varName = varNames.next();
					
					if(qSolution.get(varName).isLiteral()) {
						String s = qSolution.get(varName).asLiteral().getString();
						sb.append(s);
					} else {
						sb.append(qSolution.get(varName).toString() + " ");
					}
					//sb.append(qSolution.get(varName).toString() + " ");
				}
				ret.add(sb.toString());
				count++;
				if((maxEntities != -1) && (count >= maxEntities)) {
					break;
				}
			}

			total = System.currentTimeMillis() - start;
			System.out.println("Time to query dataset: " + total + "ms");
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + dataset + " Error: " + e.getMessage());
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
		
		if (ret.size() > 0) {
			Main.goodSources.add(dataset);
		}

		return ret;
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
						String res = qSolution.get(varName).toString();
						if (res.contains("http")) {
							ret.add(qSolution.get(varName).toString());
						}
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

		if (ret.size() > 0) {
			Main.goodSources.add(endPoint);
		}
		return ret;
	}

	public static Set<String> execQueryEndPoint(String cSparql, String endPoint, boolean noOffset, int maxEntities) {
		System.out.println("Query endPoint: " + endPoint);
		final Set<String> ret = new HashSet<String>();
		long start = System.currentTimeMillis();

		Query query = QueryFactory.create(cSparql);
		QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
		// QueryEngineHTTP qexec = new QueryEngineHTTP(endPoint, cSparql);
		try {

			ResultSet results = qexec.execSelect();
			List<QuerySolution> lst = ResultSetFormatter.toList(results);
			int count =0;
			for (QuerySolution qSolution : lst) {
				final StringBuffer sb = new StringBuffer();
				for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
					final String varName = varNames.next();
					String res = qSolution.get(varName).toString();
					if (res.contains("http")) {
						if(qSolution.get(varName).isLiteral()) {
							String s = qSolution.get(varName).asLiteral().getString();
							ret.add(s);
						} else {
							ret.add(qSolution.get(varName).toString());
						}
					}
				}
				count++;
				if((maxEntities != -1) && (count >= maxEntities)) {
					break;
				}
			}
		} catch (Exception e) {
			System.err.println("ErrorEndPoint: " + endPoint);
		} finally {
			qexec.close();
		}

		long total = System.currentTimeMillis() - start;
		long seconds = TimeUnit.MILLISECONDS.toSeconds(total);
		System.out.println("Time to get all resources(seconds): " + seconds);

		if (ret.size() > 0) {
			Main.goodSources.add(endPoint);
		}
		return ret;
	}

	public static String getMax(Map<String, Integer> mBestDs) {
		int max = 0;
		String ds = null;
		for (Entry<String, Integer> entry : mBestDs.entrySet()) {
			int n = entry.getValue();
			if(n > max) {
				max = n;
				ds = entry.getKey();
			}
		}
		return ds;
	}

	public static void generateStatistics(List<String> lstDs, String fileName) throws FileNotFoundException, UnsupportedEncodingException {
		Set<String> datasets = new HashSet<String>(lstDs);
		String cSparql = "Select ?date where{\n" + 
				"?s <http://purl.org/dc/terms/modified> ?date\n" + 
				"} order by DESC(?date) limit 1";
		Map<String, String> mDsTimeStamp = new HashMap<String, String>();
		Set<String> ret = new HashSet<String>();
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		
		for (String source : datasets) {
		//lstSources.parallelStream().forEach( source -> {
			try {
				TimeOutBlock timeoutBlock = new TimeOutBlock(300000); // 3 minutes
				Runnable block = new Runnable() {
					public void run() {
						
						if (Util.isEndPoint(source)) {
							//ret.addAll(execQueryEndPoint(cSparql, source));
							ret.addAll(Util.execQueryEndPoint(cSparql, source, true, -1));
						} else {
							ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
						}
						for (String timeStamp : ret) {
							mDsTimeStamp.put(source, timeStamp);
							writer.println(source + "\t" + timeStamp);
						}
						ret.clear();
					}
				};
				timeoutBlock.addBlock(block);// execute the runnable block
			} catch (Throwable e) {
				System.out.println("TIME-OUT-ERROR - dataset/source: " + source);
			}
		}
		writer.close();
		System.out.println("File Generated: " + fileName);
	}
}
