package web.servlet.matching;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FileUtils;
import org.rdfhdt.hdt.dictionary.Dictionary;
import org.rdfhdt.hdt.hdt.HDT;
import org.rdfhdt.hdt.hdt.HDTManager;
import org.rdfhdt.hdt.header.Header;
import org.rdfhdt.hdt.triples.IteratorTripleString;
import org.rdfhdt.hdt.triples.TripleString;

/**
 * Servlet implementation class MatchingServlet
 */
@WebServlet("/MatchingServlet")
public class MatchingServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * @see HttpServlet#HttpServlet()
	 */
	public MatchingServlet() {
		super();
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		/*if(request.getParameter("hdtinsert") != null) {
			String data = request.getParameter("hdtinsert").trim();
			String retInsert = insertHDT(data);
			response.getOutputStream().println(retInsert);
		} else*/ if (request.getParameter("query") != null) {
			Set<String> ret = new LinkedHashSet<String>();
			Set<String> dsService = new HashSet<String>();
			String dataset = request.getParameter("query").trim();
			String origQuery = request.getParameter("query").trim();
			boolean bDataset = ((request.getParameter("opt") != null)
					&& ((request.getParameter("opt").contentEquals("dataset"))));
			boolean bProperties = ((request.getParameter("opt") != null)
					&& ((request.getParameter("opt").contentEquals("properties"))));
			boolean bSparql = ((request.getParameter("opt") != null)
					&& ((request.getParameter("opt").contentEquals("sparql"))));
			boolean bService = ((request.getParameter("service") != null)
					&& ((request.getParameter("service").contentEquals("service"))));

			String str[] = dataset.split(",");
			if (str.length > 1) {
				Set<String> props = new LinkedHashSet<String>();
				if (DatabaseMain.isSparql(dataset)) {
					try {
						ret = DatabaseMain.searchDB(dataset);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else {
					for (String p : str) {
						props.add(p.trim());
					}
					try {
						ret = DatabaseMain.searchDB(props);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			} else {
				if (bDataset) {
					dataset = dataset.replaceAll("http://", "");
					dataset = dataset.replaceAll("https://", "");
					dataset = dataset.replaceAll("/sparql", "_sparql");
					try {
						ret = DatabaseMain.searchDB(dataset);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (bProperties) {
					Set<String> props = new LinkedHashSet<String>();
					props.add(dataset);
					try {
						ret = DatabaseMain.searchDB(props);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				} else if (DatabaseMain.isSparql(dataset)) {
					try {
						ret = DatabaseMain.searchDB(dataset);
					} catch (InterruptedException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
				}
			}
			StringBuffer bRet = new StringBuffer();
			if ((ret != null) && (ret.size() > 0)) {
				Set<String> datasets = new LinkedHashSet<String>();
				Map<String, Set<String>> mapEdges = new LinkedHashMap<String, Set<String>>();
				bRet.append("<!DOCTYPE html>\n" + "<html>\n" + "<head>\n" + "<style>\n" + "table {\n"
						+ "  font-family: arial, sans-serif;\n" + "  border-collapse: collapse;\n" + "  width: 100%;\n"
						+ "}\n" + "\n" + "td, th {\n" + "  border: 1px solid #dddddd;\n" + "  text-align: left;\n"
						+ "  padding: 8px;\n" + "}\n" + "\n" + "tr:nth-child(even) {\n"
						+ "  background-color: #dddddd;\n" + "}\n" + "</style>\n" + "</head>\n" + "<body>"
						+ "<script src=\"https://www.kryogenix.org/code/browser/sorttable/sorttable.js\"></script>");
				bRet.append("<h1>Input: </h1><textArea cols=\"100\" rows=\"3\">" + origQuery + "</textArea>");
				bRet.append(
						"<p><button onClick=\"window.location.href='vis.jsp';\">Visualize dataset relations</button></p>");
				bRet.append("<h1>Output:</h1>");
				int count = 0;
				for (String element : ret) {
					String r = element.toLowerCase();
					r = r.replaceAll("_sparql", "/sparql");
					r = r.replaceAll("out_tests2/", "");
					r = r.replaceAll("andre_", "");
					r = r.replaceAll("_exact.txt", "");
					if (r.contains("{")) {
						bRet.append("<h3>Exact Match</h3>" + "<br>Json file containing: Property->DatasetsMatch");
						mapEdges.putAll(getEdges(r));
						String s1 = r.replaceAll(",", "\",\"");
						bRet.append("<textArea rows='8' cols='80'>" + s1 + "</textArea>");

						// response.getOutputStream().println(r.replaceAll(sR[0], "<b>"+sR[0]+"</b>="));
						bRet.append("<h3>Similar Match</h3>");
						bRet.append("<table class=\"sortable\">\n" + "  <tr>\n" + "    <th>Property_Source</th>\n"
								+ "    <th>Property_Target</th>\n" + "    <th>Dataset_Source</th>\n"
								+ "    <th>Dataset_Target</th>\n" + "  </tr>");
					} else {
						String sR = r.replaceAll("ANDRE_", "");
						String s[] = sR.split("\t");
						if (s.length > 3) {
							String pS = "<a href='" + s[0] + "'>" + s[0] + "</a>";
							String pT = "<a href='" + s[1] + "'>" + s[1] + "</a>";
							String dS = getLinkHref(s[2]);
							String dT = getLinkHref(s[3]);
							bRet.append("<tr>\n" + "    <td>" + pS + "</td>\n" + "    <td>" + pT + "</td>\n"
									+ "    <td>" + dS + "</td>\n" + "    <td>" + dT + "</td>\n" + "  </tr>");
						} else {
							if (count == 0) {
								// bRet.append("<table><tr><th>" + s[0] + "</th><th>" + s[1] + "</th><th>" +
								// s[2] + "</th></tr>");
								bRet.append(
										"<table class=\"sortable\"><tr><th>Dataset</th><th>#Exact Match</th><th>#Similarity > 0.9</th></tr>");
							} else {
								String ds = s[0].replaceAll("andre_", "");
								ds = ds.replaceAll("_exact.txt", "");
								double score = getScore(dataset, ds, s[1]);
								// if (score > 0.04) {
								
								// }
								if (datasets.add(ds + ";" + score) && (!dataset.replaceAll("_", "/").equalsIgnoreCase(ds))) {
									if (bService) {
										dsService.add(getDsLink(ds) + "\n");
									} else {
										ds = getLinkHref(s[0]);
										bRet.append("<tr>\n" + "    <td>" + ds + "</td>\n" + "    <td>" + s[1] + "</td>\n"
												+ "<td>" + s[2] + "</td>\n" + "<td><button onclick=alert('Shared Properties:????')>details</button></td>\n" + "</tr>");
									}
								}

							}
							count++;
						}

					}
				}
				bRet.append("</table></body></html>");
				if (bDataset) {
					dataset = dataset.replaceAll("_sparql", "/sparql");
					mapEdges.put(dataset, datasets);
					request.getSession().setAttribute("datasets", datasets);
				} else {
					Set<String> nodes = new LinkedHashSet<String>();
					nodes.addAll(mapEdges.keySet());
					for (Entry<String, Set<String>> entry : mapEdges.entrySet()) {
						for(String ds : entry.getValue()) {
							nodes.add(ds);
						}
					}
					request.getSession().setAttribute("datasets", nodes);
				}
				request.getSession().setAttribute("edges", mapEdges);
				// ret.forEach(response.getWriter()::println);
				request.getSession().setAttribute("origQuery", origQuery);
				if (bService) {
					response.getOutputStream().println(dsService.toString());
				} else {
					response.getOutputStream().println(bRet.toString());
				}
			}

			// response.getWriter().append("Served at:
			// ").append(request.getParameter("query"));
		}
	}

	public static Map<String, Set<String>> getEdges(String relPropDataset) {
		Map<String, Set<String>> mapEdges = new LinkedHashMap<String, Set<String>>();
		String s[] = relPropDataset.split(",");
		String prop = null;
		Set<String> datasets = null;
		for (String elem : s) {
			String dataset = elem.trim().replaceAll("\\[|\\]", "");
			//dataset = dataset.replaceAll("]", "");
			dataset = dataset.replaceAll("\\{", "");
			dataset = dataset.replaceAll("\\}", "");
			if (dataset.contains("=")) {
				String s1[] = dataset.split("=");
				prop = s1[0];
				datasets = new LinkedHashSet<String>();
				datasets.add(s1[1]);
				mapEdges.put(prop, datasets);
			} else {
				if (prop != null) {
					if (mapEdges.containsKey(prop)) {
						mapEdges.get(prop).add(dataset);
					} else {
						datasets = new LinkedHashSet<String>();
						datasets.add(dataset);
						mapEdges.put(prop, datasets);
					}
				}
			}

		}

		return mapEdges;
	}

	private double getScore(String dSource, String dTarget, String exactMatch) throws IOException {
		double ret = 0.0;
		try {
			int nPropSource = getNumProp(dSource);
			int nPropTarget = getNumProp(dTarget);
			int nExactMatch = Integer.parseInt(exactMatch);
			// Jaccard
			ret = (nExactMatch / ((nPropSource + nPropTarget) - nExactMatch));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}

	private int getNumProp(String ds) throws IOException {
		HDT hdt = null;
		File file = null;
		String nProp = "";
		if (ds.toLowerCase().contains("sparql")) {
			ds = ds.replaceAll("_sparql", "/sparql");
			if (!ds.startsWith("http")) {
				ds = "https://" + ds;
			}
			return getNumPropSPARQL(ds);
		}
		if (!ds.toLowerCase().endsWith(".hdt")) {
			return getNumPropRDF(ds);
		}
		try {
			file = new File(ds);
			if (!file.exists()) {
				// download the file.
				URL url = new URL(getLinkDs(ds));
				FileUtils.copyURLToFile(url, file);
			}
			hdt = HDTManager.mapHDT(file.getAbsolutePath(), null);
			Dictionary dic = hdt.getDictionary();
			Header header = hdt.getHeader();
			IteratorTripleString it = header.search("", "http://rdfs.org/ns/void#properties", "");

			while (it.hasNext()) {
				TripleString ts = it.next();
				nProp = ts.getObject().toString().replaceAll("\"", "");
			}
		} catch (Exception e) {
			if (!e.getMessage().contains("Adjacency list")) {
				// e.printStackTrace();
				// System.gc();
			}
		} finally {
			// file.delete();
			if (hdt != null) {
				hdt.close();
			}
		}
		if (nProp.length() > 0) {
			return Integer.parseInt(nProp);
		} else {
			return 0;
		}
	}

	private int getNumPropRDF(String ds) {
		System.err.println("**** getNumPropRDF() NEED TO IMPLEMENT !!! *****");
		return 0;
	}

	private int getNumPropSPARQL(String ds) {
		String cSparql = "select distinct (count(?p) as ?c) where {?s ?p ?o}";
		// String cSparql = "select * where {?s ?p ?o} limit 10";
		Set<String> ret = execQueryEndPoint(cSparql, ds);
		for (String item : ret) {
			return Integer.parseInt(item);
		}
		return 0;
	}

	public static Set<String> execQueryEndPoint(String cSparql, String endPoint) {
		final Set<String> ret = new LinkedHashSet<String>();
		System.out.println("Query endPoint: " + endPoint);

		if (endPoint.toLowerCase().contains("eventmedia")) {
			ret.add("0");
			return ret;
		}

		if (endPoint.toLowerCase().contains("dbpedia")) {
			ret.add("438336346");
			return ret;
		}

		if (endPoint.toLowerCase().contains("wikidata")) {
			ret.add("438336346");
			return ret;
			// return WikidataQuery.getResult(cSparql);
		}
		ret.add("10000");
		return ret;
		/*
		 * Query query = QueryFactory.create(cSparql); QueryEngineHTTP qexec = new
		 * QueryEngineHTTP(endPoint, cSparql); try { ResultSet results =
		 * qexec.execSelect(); List<QuerySolution> lst =
		 * ResultSetFormatter.toList(results); for (QuerySolution qSolution : lst) {
		 * final StringBuffer sb = new StringBuffer(); for (final Iterator<String>
		 * varNames = qSolution.varNames(); varNames.hasNext();) { final String varName
		 * = varNames.next(); String res = qSolution.get(varName).toString(); if
		 * (res.contains("http")) { ret.add(qSolution.get(varName).toString()); } } } }
		 * catch (Exception e) { e.printStackTrace(); } finally { qexec.close(); }
		 * return ret;
		 */
	}

	private String getDsLink(String ds) {
		if (ds.contains("sparql") && !ds.startsWith("http")) {
			return "http://" + ds;
		} else {
			return "http://141.57.11.86:8082/dirHDTLaundromat/decompressed/" + ds.substring(0, 2) + "/"
					+ ds.replaceAll(".hdt", "/" + ds);
		}
	}

	private String getLinkHref(String ds) {
		if (ds.contains("sparql")) {
			return "<a href='http://" + ds + "'>http://" + ds + "</a>";
		} else {
			return "<a href='http://141.57.11.86:8082/dirHDTLaundromat/decompressed/" + ds.substring(0, 2) + "/"
					+ ds.replaceAll(".hdt", "/") + ds + "'>" + ds + "</a>";
		}
	}

	private String getLinkDs(String ds) {
		if (ds.contains("sparql")) {
			return "http://" + ds;
		} else {
			return "http://141.57.11.86:8082/dirHDTLaundromat/decompressed/" + ds.substring(0, 2) + "/"
					+ ds.replaceAll(".hdt", "/") + ds;
		}
	}

	/**
	 * @see HttpServlet#doPost(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doPost(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		// TODO Auto-generated method stub
		doGet(request, response);
	}

}
