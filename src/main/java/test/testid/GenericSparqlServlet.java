package web.servlet.matching;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

import javax.servlet.ServletException;
import javax.servlet.annotation.WebServlet;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

/**
 * Servlet implementation class SimilarityServlet
 */
@WebServlet("/SimilarityServlet")
public class GenericSparqlServlet extends HttpServlet {
	private static final long serialVersionUID = 1L;

	/**
	 * Default constructor.
	 */
	public GenericSparqlServlet() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @see HttpServlet#doGet(HttpServletRequest request, HttpServletResponse
	 *      response)
	 */
	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws ServletException, IOException {
		if (request.getParameter("dataset") != null) {
			String endPoint = request.getParameter("dataset");
			String query = request.getParameter("query");
			response.getWriter().append(Util.execQueryRDFRes(query, endPoint, -1).toString());
			response.getWriter().append("\n\nDataset: ").append(request.getParameter("dataset"));
			response.getWriter().append("\nQuery: ").append(request.getParameter("query"));
		} else if (request.getParameter("datasets") != null) {
			Set<String> ret = new LinkedHashSet<String>();
			String datasets = request.getParameter("datasets");
			String str[] = datasets.split(",");
			if (str.length > 1) {
				Set<String> props = new LinkedHashSet<String>();
				for (String p : str) {
					props.add(p.trim());
				}
				try {
					ret = generateDatasetSimilarity(props);
					response.getWriter().append("Datasets: ").append(datasets).append("\n");
					response.getWriter().append("Number of properties and classes they share: ")
							.append("" + ret.size());
					response.getWriter().append("\nMatches: ").append(ret.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
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

	public static Set<String> generateDatasetSimilarity(Set<String> datasets) {
		Set<String> ret = new HashSet<String>();
		String[] array = datasets.stream().toArray(String[]::new);
		for (int i = 0; i < array.length; i++) {
			for (int j = i; j < array.length; j++) {
				try {
					if (array[i].equalsIgnoreCase(array[j]))
						continue;
					Map<String, Set<String>> exactMatches = getExactMatches(array[i], array[j]);
					ret.add(exactMatches.toString());
				} catch (Exception e) {
					e.printStackTrace();
				}
			}

		}
		return ret;
	}

	private static Map<String, Set<String>> getExactMatches(String source, String target)
			throws FileNotFoundException, UnsupportedEncodingException {
		final Set<String> propsSource = new LinkedHashSet<String>();
		final Set<String> propsTarget = new LinkedHashSet<String>();
		final Set<String> propsMatched = new LinkedHashSet<String>();
		final Map<String, Set<String>> mapExactMatch = new LinkedHashMap<String, Set<String>>();
		String s[] = source.split("/");
		String fSource = null;
		String fTarget = null;
		if (s.length > 2) {
			fSource = s[2] + "_" + s[s.length - 1];
		} else {
			fSource = s[s.length - 1];
		}
		String t[] = target.split("/");
		if (t.length > 2) {
			fTarget = t[2] + "_" + t[t.length - 1];
		} else {
			fTarget = t[t.length - 1];
		}
		// final String fileName = OUTPUT_DIR + "/" + fSource + "---" + fTarget +
		// "_Exact.txt";
		final String fileName = fSource + "---" + fTarget;
		propsSource.addAll(getProps(source, fSource));
		propsTarget.addAll(getProps(target, fTarget));
		if ((propsSource.size() < 1) || (propsTarget.size() < 1)) {
			return mapExactMatch;
		}

		propsSource.parallelStream().forEach(pSource -> {
			propsTarget.parallelStream().forEach(pTarget -> {
				if (pSource.equalsIgnoreCase(pTarget)) {
					propsMatched.add(pSource);
				}
			});
		});
		mapExactMatch.put(fileName, propsMatched);
		// writer.close();
		return mapExactMatch;
	}

	private static Set<String> getProps(String source, String fName) {
		// Put Claus approach here...
		// instead of execute the SPARQL at the Dataset, we query the Dataset Catalog
		// from Claus to obtain a list of properties and classes.
		// This should be faster then query the dataset, because there are some
		// datasets/Endpoints extremely slow, more than 3 minutes.
		// return getPropsClaus(source)
		String cSparqlP = "Select ?p where {?s ?p ?o}";
		String cSparqlC = "select distinct ?p where {[] a ?p}";
		Set<String> ret = new LinkedHashSet<String>();
		ret.addAll(execSparql(cSparqlP, source));
		ret.addAll(execSparql(cSparqlC, source));
		return ret;
	}

	private static Set<String> execSparql(String cSparql, String source) {
		final Set<String> ret = new LinkedHashSet<String>();
		try {
			ret.addAll(Util.execQueryRDFRes(cSparql, source, -1));
		} catch (Exception e) {
			e.printStackTrace();
		}
		return ret;
	}
}
