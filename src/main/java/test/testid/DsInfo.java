package test.testid;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.util.HashSet;
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
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;

public class DsInfo {

	public static void main(String[] args) throws IOException {
		Map<String, String> mapQuerySource = getSampleQueries(new File("queryDsInfo.txt"));
		final Set<String> ret = new LinkedHashSet<String>();
		System.out.println("#Datasets to process: " + mapQuerySource.size());
		for (Entry<String, String> entry : mapQuerySource.entrySet()) {
			String source = entry.getKey();
			String cSparql = entry.getValue();
			try {
				String s[] = source.split("/");
				String fileName = "out10/" + s[2] + "_" + s[s.length - 1] + ".csv";
//			if (Util.isEndPoint(source)) {
				execQueryEndPoint(cSparql, source, fileName);
//			} else {
//				ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
//			}
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
	
	public static Set<String> execQueryEndPoint(String cSparql, String endPoint, String fileName) throws IOException {
		System.out.println("Query endPoint: " + endPoint);
		final Set<String> ret = new HashSet<String>();
		final long offsetSize = 9999;
		long offset = 0;
		String sSparql = null;
		long start = System.currentTimeMillis();
		ByteArrayOutputStream b_outputStream = new ByteArrayOutputStream();
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
				ResultSetFormatter.outputAsCSV(b_outputStream,results);
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
		
		return ret;
	}
}
