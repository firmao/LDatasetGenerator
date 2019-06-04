package test.testid;

import java.io.File;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Triple;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ReadWrite;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.apache.jena.riot.out.NodeFormatter;
import org.apache.jena.sparql.util.NodeFactoryExtra;
import org.apache.jena.tdb.TDBFactory;
import org.rdfhdt.hdt.enums.RDFNotation;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.exceptions.ParserException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.options.HDTSpecification;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

public class App {

	public static void main(String[] args) throws IOException, NotFoundException, ParserException {
//		Set<String> datasets = new LinkedHashSet<String>();
//		datasets.add("dirHDT/3_ds_tests/hdt/d1.hdt");
//		datasets.add("dirHDT/3_ds_tests/hdt/d2.hdt");
//		datasets.add("dirHDT/3_ds_tests/hdt/d3.hdt");
//		String cSparql = "Select * where {?s ?p ?o ."
//				+ "Filter(?s=<http://csarven.ca/> || "
//				+ "?s=<http://dbpedia.org/resource/Lucius_Caesar>)}";
//		
//		String pathEmptyHDT = "dirHDT/3_ds_tests/hdt/d1.hdt";
//		experimentHdtFederated(datasets, cSparql, pathEmptyHDT);
		
		//createHDT();
		//System.exit(0);
		// String hdtFile = "dirHDT/dbpedia2015.hdt";
		//String hdtFile = "dirHDT/3_ds_tests/hdt/d2.hdt";
		// String cSparql = "PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX xsd:
		// <http://www.w3.org/2001/XMLSchema#> PREFIX res:
		// <http://dbpedia.org/resource/> PREFIX xsd:
		// <http://www.w3.org/2001/XMLSchema#> SELECT ?years WHERE { res:Ford_Model_T
		// dbo:productionEndYear ?end ; dbo:productionStartYear ?start. BIND ( (
		// year(xsd:date(?end)) - year(xsd:date(?start)) ) AS ?years) }";
//		String cSparql = "Select * where {" + "?s ?p ?o ." + "FILTER(?s=<http://csarven.ca/> "
//				+ "&& ?p=<http://creativecommons.org/ns#license> "
//				+ "&& ?o=<http://creativecommons.org/licenses/by-sa/4.0/>)} " + "limit 10";
		
//		String ds = "dirHDT/0b02ffc7e6f645ad5e91b330bf4e0431.hdt";
//		String cSparql = "SELECT DISTINCT * WHERE { <http://dbpedia.org/ontology/areaCode> ?p ?o } limit 10";
//		System.out.println(execHDTString(ds, cSparql));
//		System.exit(0);
		
		Set<String> datasets = new LinkedHashSet<String>();
		int numberOfDs = 10000;
		//datasets.add("UnionResultsSchemaMatching.hdt");
		//datasets.addAll(getDatasets(new File("dirHDT"), numberOfDs));
		datasets.add("dirHDT/69e7c7ccdc8f0b373325d5acf3c27b26.hdt");
		String cSparql = "SELECT * WHERE {?s ?p ?o}";
		PrintWriter writer = new PrintWriter("sparqlOut.txt", "UTF-8");
		int count = 0;
		for (String ds : datasets) {
			String ret = execHDTString(ds, cSparql);
			writer.println(ret);
			if((ret != null) && !ret.contains("http")) {
				count++;
			}
		}
		writer.println("Datasets without owl:equivalentProperty: " + count);
		writer.println("Total datasets analised: " + datasets.size());
		writer.close();
	}

	private static Set<String> getDatasets(File file, int limit) {
		Set<String> ret = new LinkedHashSet<String>();
		if (file.isDirectory()) {
			File[] files = file.listFiles();
			int count = 0;
			for (File source : files) {
				if (source.isFile()) {
					if (count >= limit)
						break;
					if(source.getName().endsWith(".hdt")) {
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
	private static void createHDT() throws IOException, ParserException {
		// Configuration variables
		String baseURI = "http://example.com/mydataset";
		String rdfInput = "dirHDT/3_ds_tests/data/personne2_vldb.nt";
		String inputType = "ntriples";
		String hdtOutput = "dirHDT/3_ds_tests/hdt/personne2_vldb.hdt";

		// Create HDT from RDF file
		HDT hdt = HDTManager.generateHDT(rdfInput, // Input RDF File
				baseURI, // Base URI
				RDFNotation.parse(inputType), // Input Type
				new HDTSpecification(), // HDT Options
				null // Progress Listener
		);

		// OPTIONAL: Add additional domain-specific properties to the header:
		// Header header = hdt.getHeader();
		// header.insert("myResource1", "property" , "value");

		// Save generated HDT to a file
		hdt.saveToHDT(hdtOutput, null);
	}

	private static void execHDT(String ds, String cSparql) throws IOException {
		File file = null;
		HDT hdt = null;
		try {
			file = new File(ds);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();
			System.out.println(ResultSetFormatter.asText(results));
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + ds + " Error: " + e.getMessage());
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
	}
	
	private static String execHDTString(String ds, String cSparql) throws IOException {
		String ret = null;
		File file = null;
		HDT hdt = null;
		try {
			file = new File(ds);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();
			ret = ResultSetFormatter.asText(results);
			qe.close();
		} catch (Exception e) {
			System.out.println("FAIL: " + ds + " Error: " + e.getMessage());
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
		return ret;
	}

	/*
	 * Empty s, p, o means get everything ALLES.
	 */
	private static void traverseHDT(String hdtFile, String s, String p, String o)
			throws NotFoundException, IOException {
		// Load HDT file NOTE: Use loadIndexedHDT() if you are doing ?P?, ?PO, ??O
		// queries
		HDT hdt = HDTManager.loadHDT(hdtFile, null);

		// Use mapHDT/mapIndexedHDT to save memory.
		// It will load the parts on demand (possibly slower querying).
//				HDT hdt = HDTManager.mapHDT("data/example.hdt", null);

		// Enumerate all triples. Empty string means "any"
		IteratorTripleString it = hdt.search("", "", "");
		System.out.println("Estimated number of results: " + it.estimatedNumResults());
		while (it.hasNext()) {
			TripleString ts = it.next();
			if (ts.getObject().toString().toLowerCase().contains(o)) {
				System.out.println(ts);
			}
		}

//		// List all predicates
//		System.out.println("Dataset contains " + hdt.getDictionary().getNpredicates() + " predicates:");
//		Iterator<? extends CharSequence> itPred = hdt.getDictionary().getPredicates().getSortedEntries();
//		while (itPred.hasNext()) {
//			CharSequence str = itPred.next();
//			System.out.println(str);
//		}
	}

	private static void experimentHdtFederated(Set<String> datasets, String cSparql, String emptyHDT) {
		HDT bigHDT = null;
		Dataset dataset = null;
		try {
//			bigHDT = HDTManager.loadHDT(emptyHDT, null);
//			HDTGraph bigHDTGraph = new HDTGraph(bigHDT);
//			Model model = new ModelCom(bigHDTGraph);
			
			dataset = TDBFactory.createDataset("dirTDB");
			dataset.begin(ReadWrite.WRITE);
			Model model = dataset.getDefaultModel();
			for (String ds : datasets) {
				HDT hdt = HDTManager.loadHDT(ds, null);
				HDTGraph hdtGraph = new HDTGraph(hdt);
				Model modelLess = new ModelCom(hdtGraph);
				
				
				model.add(modelLess);
				//model.union(modelLess);
				//model.intersection(modelLess);
				//model.commit();
				hdt.close();
			}
			//model.write(System.out, "RDF/XML-ABBREV");
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();
			System.out.println(ResultSetFormatter.asText(results));
			qe.close();
			dataset.commit();
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			dataset.close();
			if (bigHDT != null) {
				try {
					bigHDT.close();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}

}
