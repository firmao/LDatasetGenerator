package test.testid;

import java.io.File;
import java.io.IOException;
import java.util.List;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
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
		
//		Set<String> hdtFiles = new HashSet<String>();
//		hdtFiles.add("dirHDT/f84568054530681185224349d6d9ce48.hdt");
//		hdtFiles.add("dirHDT/f490676b9c0a8d6726cade30295fdbae.hdt");
//		hdtFiles.add("dirHDT/f94731fd64e5d21ddf3d9b056c33f7b8.hdt");
//		hdtFiles.parallelStream().forEach(hdtFile ->{ // this line distribute the process in parallel, one hdtFile per core processor.
//			try {
//				traverseHDT(hdtFile, "","", "birth");
//			} catch (NotFoundException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			} catch (IOException e) {
//				// TODO Auto-generated catch block
//				e.printStackTrace();
//			}
//		});
		createHDT();
		System.exit(0);
		//String hdtFile = "dirHDT/dbpedia2015.hdt";
		String hdtFile = "dirHDT/85d5a476b56fde200e770cefa0e5033c.hdt";
		//String cSparql = "PREFIX dbo: <http://dbpedia.org/ontology/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> PREFIX res: <http://dbpedia.org/resource/> PREFIX xsd: <http://www.w3.org/2001/XMLSchema#> SELECT ?years WHERE { res:Ford_Model_T dbo:productionEndYear ?end ; dbo:productionStartYear ?start. BIND ( ( year(xsd:date(?end)) - year(xsd:date(?start)) ) AS ?years) }";
		String cSparql = "Select * where {"
				+ "?s ?p ?o ."
				+ "FILTER(?s=<http://csarven.ca/> "
				+ "&& ?p=<http://creativecommons.org/ns#license> "
				+ "&& ?o=<http://creativecommons.org/licenses/by-sa/4.0/>)} "
				+ "limit 10";
		execHDT(hdtFile, cSparql);
		
	}

	private static void createHDT() throws IOException, ParserException {
		 // Configuration variables
        String baseURI = "http://example.com/mydataset";
        String rdfInput = "dirHDT/3_ds_tests/data/d3.nt";
        String inputType = "ntriples";
        String hdtOutput = "dirHDT/3_ds_tests/hdt/d3.hdt";
 
        // Create HDT from RDF file
        HDT hdt = HDTManager.generateHDT(
                            rdfInput,         // Input RDF File
                            baseURI,          // Base URI
                            RDFNotation.parse(inputType), // Input Type
                            new HDTSpecification(),   // HDT Options
                            null              // Progress Listener
                );
 
        // OPTIONAL: Add additional domain-specific properties to the header:
        //Header header = hdt.getHeader();
        //header.insert("myResource1", "property" , "value");
 
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

	/*
	 * Empty s, p, o means get everything ALLES.
	 */
	private static void traverseHDT(String hdtFile, String s, String p, String o) throws NotFoundException, IOException {
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
			if(ts.getObject().toString().toLowerCase().contains(o)){
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

}
