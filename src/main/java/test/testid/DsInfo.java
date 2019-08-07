package test.testid;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

public class DsInfo {

	public static void main(String[] args) throws IOException {
		Map<String, String> mapQuerySource = getSampleQueries(new File("queryDsInfo.txt"));
		System.out.println("#Datasets to process: " + mapQuerySource.size());
		for (Entry<String, String> entry : mapQuerySource.entrySet()) {
			String source = entry.getKey();
			String cSparql = entry.getValue();
			try {
				String s[] = source.split("/");
				String fileNameCSV = "out10/" + s[2] + "_" + s[s.length - 1] + ".csv";
			if (Util.isEndPoint(source)) {
				execQueryEndPoint(cSparql, source, fileNameCSV);
			} else {
				execQueryDump(cSparql, source, fileNameCSV);
			}
			}catch (Exception e) {
				System.err.println("DsFail: " + source);
			}
		}
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
					//ret.add(query);
					query = "";
					continue;
				}
			}
		} catch (IOException e) {
			e.printStackTrace();
		}

		return ret;
	}
	
	public static void execQueryEndPoint(String cSparql, String endPoint, String fileName) throws FileNotFoundException, IOException {
		System.out.println("Query endPoint: " + endPoint);
		final long offsetSize = 9999;
		long offset = 0;
		String sSparql = null;
		ByteArrayOutputStream b_outputStream = new ByteArrayOutputStream();
		int count = 0;
		long start = System.currentTimeMillis();
		do {
			sSparql = cSparql;
			// int indOffset = cSparql.toLowerCase().indexOf("offset");
			// int indLimit = cSparql.toLowerCase().indexOf("limit");
			// if((indLimit < 0) && (indOffset < 0)) {
			// sSparql = cSparql + " offset " + offset + " limit " + offsetSize;
			sSparql = cSparql + " offset " + offset + " Limit 9999";
			// }
			// System.out.println(sSparql);
			Query query = QueryFactory.create(sSparql);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			// QueryEngineHTTP qexec = new QueryEngineHTTP(endPoint, cSparql);
			try {

				ResultSet results = qexec.execSelect();
				ResultSetFormatter.outputAsCSV(b_outputStream,results);
				count++;
				if(count > 4) break;
				
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

		try(OutputStream outputStream = new FileOutputStream(fileName)) {
			b_outputStream.writeTo(outputStream);
		}
	}
	
	public static void execQueryDump(String cSparql, String dataset, String fileNameCSV) throws IOException {
		ByteArrayOutputStream b_outputStream = new ByteArrayOutputStream();
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

			file = Util.unconpress(file);

			if (file.getName().endsWith("hdt")) {
				execQueryHDT(cSparql, file.getAbsolutePath(), fileNameCSV);
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
				String contentType = Util.getContentType(dataset); // get the IRI
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
				ResultSetFormatter.outputAsCSV(b_outputStream,resultSet);
			}
		} catch (Exception ex) {
			ex.printStackTrace();
		}
		
		
		try(OutputStream outputStream = new FileOutputStream(fileNameCSV)) {
			b_outputStream.writeTo(outputStream);
		}
	}

	private static void execQueryHDT(String cSparql, String dataset, String fileNameCSV) throws IOException {
		File file = null;
		HDT hdt = null;
		ByteArrayOutputStream b_outputStream = new ByteArrayOutputStream();
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
			file = Util.unconpress(file);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			if (SparqlMatching.mapDsNumTriples.get(dataset) == null) {
				Header header = hdt.getHeader();
				IteratorTripleString it = header.search("", "http://rdfs.org/ns/void#triples", "");
				String numberTriples = null;
				while (it.hasNext()) {
					TripleString ts = it.next();
					numberTriples = ts.getObject().toString();
				}
				long numTriples = Long.parseLong(numberTriples.trim().replaceAll("\"", ""));
				SparqlMatching.mapDsNumTriples.put(dataset, numTriples);
				System.out.println("Dataset: " + dataset + "\nTriples: " + numberTriples);
			}

			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			// model.write(out, Lang.NTRIPLES); // convert hdt to .nt
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			ResultSetFormatter.outputAsCSV(b_outputStream,results);
			
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + dataset + " Error: " + e.getMessage());
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
		try(OutputStream outputStream = new FileOutputStream(fileNameCSV)) {
			b_outputStream.writeTo(outputStream);
		}
	}
}
