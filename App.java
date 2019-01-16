package test.testid;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

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
import org.rdfhdt.hdtjena.HDTGraph;

/**
 * Hello world!
 *
 */
public class App 
{
    public static void main( String[] args ) throws IOException
    {
    	String cSparql = "select distinct ?x {\n" + 
    			"?x <http://www.w3.org/2000/01/rdf-schema#label> \"Tim Berners-Lee\"\n" + 
    			"}";
    	String dataset = "dirHDT/f490676b9c0a8d6726cade30295fdbae.hdt";
        Set<String> sRet = execQueryHDTRes(cSparql, dataset);
    	System.out.println(sRet);
    }
    
    public static Set<String> execQueryHDTRes(String cSparql, String dataset) throws IOException {
		final Set<String> ret = new HashSet<String>();
		File file = null;
		HDT hdt = null;
		try {
			System.out.println("Dataset: " + dataset);
			
			file = new File(dataset);
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			
			System.out.println("BaseURI: " + hdt.getBaseURI());
			System.out.println("Dictionary.type: " + hdt.getDictionary().getType());
			System.out.println("Header.NumberElements: " + hdt.getHeader().getNumberOfElements());
			
			HDTGraph graph = new HDTGraph(hdt);
			
			Model model = new ModelCom(graph);
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			List<QuerySolution> lQuerySolution = ResultSetFormatter.toList(results);
			for (QuerySolution qSolution : lQuerySolution) {
				final StringBuffer sb = new StringBuffer();
				for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
					final String varName = varNames.next();
					sb.append(qSolution.get(varName).toString() + " ");
				}
				ret.add(sb.toString());
			}

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
}
