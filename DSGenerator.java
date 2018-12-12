import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

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
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.apache.jena.sparql.engine.http.QueryEngineHTTP;
import org.apache.jena.util.FileManager;

public class DSGenerator {

	public static void main(String[] args) throws FileNotFoundException, UnsupportedEncodingException {
		Set<String> resources = new HashSet<String>();
		//resources.add("http://sws.geonames.org/78428/");
		//resources.add("http://dbpedia.org/resource/Leipzig");
		resources.addAll(getResources());
		System.out.println("Number of resources: " + resources.size());
		for (String resource : resources) {
			generateFilePropertiesValues(resource);
		}
	}
	
	private static Set<String> getResources() {
		Set<String> ret = new HashSet<String>();
		List<String> lstQueries = getSampleQueries(new File("queries.txt"));
		String endPoint = "http://dbpedia.org/sparql";
		
		for (String cSparql : lstQueries) {
			ret.addAll(execQueryEndPoint(cSparql, endPoint));
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

	public static void generateFilePropertiesValues(String resource) throws FileNotFoundException, UnsupportedEncodingException{
		Model model = null;
		try {
			//model = FileManager.get().loadModel(resource);
			
			model = ModelFactory.createDefaultModel();
			InputStream file = FileManager.get().open(resource);
			model.read(file,null);
		}catch(Exception e) {
			System.err.println("ErrorResource: " + resource + " Message: " + e.getMessage());
			return;
		}		
		StmtIterator stmts = model.listStatements();
		//String fileName = resource.substring(resource.lastIndexOf("/") + 1, resource.length());
		String s[] = resource.split("/");
		String fileName = s[2] + "_" + s[s.length-1] + ".tsv";
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#---Properties and values from: " + resource);
		writer.println("Property\tValue");
		while (stmts.hasNext()) {
			Statement stmt = stmts.next();
			if(resource.equals(stmt.getSubject().toString())) {
				writer.println(stmt.getPredicate() + "\t" + stmt.getObject());
			} else {
				writer.println(stmt.getPredicate() + "\t" + stmt.getSubject());
			}
		}
		
		writer.close();
		System.out.println("File generated: " + fileName);
	}
	
	public static Set<String> execQueryEndPoint(String cSparql, String endPoint) {
		System.out.println("Query endPoint: " + endPoint);
		final Set<String> ret = new HashSet<String>();
		final long offsetSize = 9999;
		long offset = 0;
		do {
			String sSparql = cSparql;
			int indOffset = cSparql.toLowerCase().indexOf("offset");
			int indLimit = cSparql.toLowerCase().indexOf("limit");
			if((indLimit < 0) && (indOffset < 0)) {
				sSparql = cSparql += " offset " + offset + " limit " + offsetSize;
			}
			//System.out.println(cSparql);
			Query query = QueryFactory.create(sSparql);
			QueryExecution qexec = QueryExecutionFactory.sparqlService(endPoint, query);
			//QueryEngineHTTP qexec = new QueryEngineHTTP(endPoint, cSparql);
			try {

				ResultSet results = qexec.execSelect();
				List<QuerySolution> lst = ResultSetFormatter.toList(results);
				for (QuerySolution qSolution : lst) {
					final StringBuffer sb = new StringBuffer();
					for ( final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext(); ) {
		                final String varName = varNames.next();
		                sb.append(qSolution.get(varName).toString() + " ");
		            }
					ret.add(sb.toString() + "\n");
				}
			} catch (Exception e) {
				e.printStackTrace();
				break;
			} finally {
				qexec.close();
			}
			offset += offsetSize;
		} while (true);
		
		return ret;
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
