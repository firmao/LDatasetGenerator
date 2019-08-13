package test.testid;

import java.io.File;
import java.io.IOException;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

import org.apache.jena.query.Query;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.ResultSetFormatter;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.impl.ModelCom;
import org.rdfhdt.hdt.exceptions.NotFoundException;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;
import org.rdfhdt.hdtjena.HDTGraph;

public class LODUtil {

	public static long getNumClasses(String ds, HDT hdt) {
		String cSparql = "SELECT DISTINCT (count(?type) as ?c) WHERE {?s a ?type.}";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getAvgInOutDegree(String ds, HDT hdt) {
//		String cSparql = "";
//		if(hdt != null) {
//			return execQueryHDT(hdt, cSparql);
//		}
//		Set<String> ret = execSparql(cSparql,ds);
//		for (String r : ret) {
//			if(Util.isNumeric(r)) {
//				return Integer.parseInt(r);
//			}
//		}
		return 0;
	}

	public static long getNumPredicates(String ds, HDT hdt) throws IOException, NotFoundException {
		String cSparql = "Select distinct (count(?p) as ?c) where{?s ?p ?o}";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getNumProperties(String ds, HDT hdt) {
		// String cSparql = "SELECT DISTINCT (count(?p) as ?c) WHERE { ?p
		// <http://www.w3.org/2000/01/rdf-schema#domain> ?class .}";
		String cSparql = "SELECT DISTINCT (count(?p) as ?c) WHERE { ?p a <http://www.w3.org/1999/02/22-rdf-syntax-ns#Property> }";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getNumSameAs(String ds, HDT hdt) {
		String cSparql = "SELECT distinct (count(?s) as ?c) WHERE { ?s <http://www.w3.org/2002/07/owl#sameAs> ?o . }";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}

		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Integer.parseInt(r);
			}
		}
		return 0;
	}

	private static long execQueryHDT(HDT hdt, String cSparql) {
		long ret = 0;
		try {
			HDTGraph graph = new HDTGraph(hdt);
			Model model = new ModelCom(graph);
			// model.write(out, Lang.NTRIPLES); // convert hdt to .nt
			Query query = QueryFactory.create(cSparql);
			QueryExecution qe = QueryExecutionFactory.create(query, model);
			ResultSet results = qe.execSelect();

			List<QuerySolution> lQuerySolution = ResultSetFormatter.toList(results);
			for (QuerySolution qSolution : lQuerySolution) {
				final StringBuffer sb = new StringBuffer();
				for (final Iterator<String> varNames = qSolution.varNames(); varNames.hasNext();) {
					final String varName = varNames.next();

					if (qSolution.get(varName).isLiteral()) {
						String s = qSolution.get(varName).asLiteral().getString();
						sb.append(s);
					} else {
						sb.append(qSolution.get(varName).toString() + " ");
					}
				}
				if (Util.isNumeric(sb.toString().trim())) {
					ret = Integer.parseInt(sb.toString().trim());
					break;
				}
			}
			qe.close();
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	public static long getNumSubjects(String ds, HDT hdt) throws IOException, NotFoundException {
		String cSparql = "Select distinct (count(?s) as ?c) where{?s ?p ?o}";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getNumTriples(String ds, HDT hdt) throws IOException, NotFoundException {
		String cSparql = "SELECT DISTINCT (count(*) as ?c) WHERE {?s ?p ?o }";
		if (hdt != null) {
			Header header = hdt.getHeader();
			IteratorTripleString it = header.search("", "http://rdfs.org/ns/void#triples", "");
			String numberTriples = null;
			while (it.hasNext()) {
				TripleString ts = it.next();
				numberTriples = ts.getObject().toString();
			}
			long numTriples = Long.parseLong(numberTriples.trim().replaceAll("\"", ""));
			if (numTriples > 0) {
				return numTriples;
			}
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getNumDuplicateInstances(String ds, HDT hdt) {
//		String cSparql = "";
//		if(hdt != null) {
//			return execQueryHDT(hdt, cSparql);
//		}
//		Set<String> ret = execSparql(cSparql,ds);
//		for (String r : ret) {
//			if(Util.isNumeric(r)) {
//				return Long.parseLong(r);
//			}
//		}
		return 0;
	}

	public static long getNumLoops(String ds, HDT hdt) {
//		String cSparql = "";
//		if(hdt != null) {
//			return execQueryHDT(hdt, cSparql);
//		}
//		Set<String> ret = execSparql(cSparql,ds);
//		for (String r : ret) {
//			if(Util.isNumeric(r)) {
//				return Long.parseLong(r);
//			}
//		}
		return 0;
	}

	public static int getNumSimilarDatasets(String ds, HDT hdt) {
		Set<String> rDs = null;
		try {
			if (Util.isEndPoint(ds)) {
				rDs = DatabaseMain.searchDB(ds);
			} else {
				File f = new File(ds);
				rDs = DatabaseMain.searchDB(f.getName());
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return rDs.size();
	}

	public static long getMaxInOutDegree(String ds, HDT hdt) {
		String cSparql = "SELECT ?cent ((?indegree+?outdegree) AS ?degree) WHERE {\n"
				+ "  {SELECT (?s AS ?cent) (COUNT(*) AS ?outdegree)\n" + "    { ?s ?p ?o }\n" + "    GROUP BY ?s\n"
				+ "    ORDER BY DESC(?outdegree)\n" + "  }\n" + "  {SELECT (?o AS ?cent) (COUNT(*) AS ?indegree)\n"
				+ "    { ?s ?p ?o }\n" + "    GROUP BY ?o\n" + "    ORDER BY DESC(?indegree)\n" + "  }\n" + "} limit 1";
		if (hdt != null) {
			try {
				Header header = hdt.getHeader();
				IteratorTripleString it = header.search("", "http://purl.org/HDT/hdt#dictionarynumSharedSubjectObject", "");
				String numberTriples = null;
				while (it.hasNext()) {
					TripleString ts = it.next();
					numberTriples = ts.getObject().toString();
				}
				long ret = Long.parseLong(numberTriples.trim().replaceAll("\"", ""));
				if (ret > 0) {
					return ret;
				}
			} catch (Exception e) {
				return execQueryHDT(hdt, cSparql);
			}
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	public static long getNumObjects(String ds, HDT hdt) throws IOException, NotFoundException {
		String cSparql = "Select distinct (count(?o) as ?c) where{?s ?p ?o}";
		if (hdt != null) {
			return execQueryHDT(hdt, cSparql);
		}
		Set<String> ret = execSparql(cSparql, ds);
		for (String r : ret) {
			if (Util.isNumeric(r)) {
				return Long.parseLong(r);
			}
		}
		return 0;
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();

		try {
			TimeOutBlock timeoutBlock = new TimeOutBlock(900000); // 15 minutes
			Runnable block = new Runnable() {
				public void run() {
					// ret.addAll(Util.execQueryEndPoint(cSparql, source));
					if (Util.isEndPoint(source)) {
						ret.addAll(Util.execQueryEndPoint(cSparql, source));
					} else {
						ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
					}
				}
			};
			timeoutBlock.addBlock(block);// execute the runnable block
		} catch (Throwable e) {
			System.err.println("TIME-OUT-ERROR - dataset/source: " + source);
		}

		return ret;
	}

	public static long getHeaderInfo(Header header, String type) throws NotFoundException {
		IteratorTripleString it = null;
		if (type.toLowerCase().contains("triples")) {
			it = header.search("", "http://rdfs.org/ns/void#triples", "");
		} else if (type.toLowerCase().contains("propert")) {
			it = header.search("", "http://rdfs.org/ns/void#properties", "");
		} else if (type.toLowerCase().contains("predicat")) {
			it = header.search("", "http://rdfs.org/ns/void#properties", "");
		} else if (type.toLowerCase().contains("subject")) {
			it = header.search("", "http://rdfs.org/ns/void#distinctSubjects", "");
		} else if (type.toLowerCase().contains("object")) {
			it = header.search("", "http://rdfs.org/ns/void#distinctObjects", "");
		}
		String info = null;
		if (it == null) {
			return 0;
		}
		while (it.hasNext()) {
			TripleString ts = it.next();
			info = ts.getObject().toString();
		}
		long ret = Long.parseLong(info.trim().replaceAll("\"", ""));
		return ret;
	}

}