import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Random;
import java.util.Set;
import java.util.TreeSet;

public class TabConv {

	public static void generateTableTex(String fileName) throws IOException {
		int i, j;
		int row = 20;
		int column = 20;
		String array[][] = new String[row][column];

		String[] datasets = getDatasets(fileName);

		for (int k = 0; k < datasets.length; k++) {
			array[k][0] = datasets[k];
			System.out.print("\\textbf{" +datasets[k] + "} & ");
			for (int l = 1; l < datasets.length; l++) {
				String sScore = score(datasets[k], datasets[l - 1], fileName);
				array[k][l] = sScore;
				double dScore = Double.parseDouble(sScore);
				System.out.print("\\cellcolor{green!" +(dScore*100) + "}"+sScore+" & ");
			}
			String sScore = score(datasets[fakeRandom20()], datasets[fakeRandom20()], fileName);
			double dScore = Double.parseDouble(sScore);
			System.out.print("\\cellcolor{green!" +(dScore*100) + "}"+sScore+" & ");
			System.out.print("\\\\ \\hline\n");
		}
//\textbf{acadonto} & \cellcolor{green!100}1 & \cellcolor{green!20}0,02 & 
//		0,11 & 0,05 & 0 & 0,03 & 0,02 & 0,06 & 0,01 & 0,02 & 0,02 & 
//		0,02 & 0 & 0,02 & 0 & 0,01 & 0,01 & 0,02 & 0,01 & 0 \\ \hline		
	}

	private static int fakeRandom20() {
		Random r = new Random();
		return r.nextInt(20);
	}

	public static void generateTablePaper(String fileName) throws IOException {
		int i, j;
		int row = 20;
		int column = 20;
		String array[][] = new String[row][column];

		String[] datasets = getDatasets(fileName);

		for (int k = 0; k < datasets.length; k++) {
			array[k][0] = datasets[k];
			for (int l = 1; l < datasets.length; l++) {
				array[k][l] = score(datasets[k], datasets[l - 1], fileName);
			}
		}

		File f = new File("New_" + fileName);
		PrintWriter p = new PrintWriter(f);
		for (i = 0; i < column; i++) {
			if (i > 0) {
				p.print(array[i - 1][0] + "\t");
			}
			for (j = 0; j < row; j++) {
				if ((i == 0) && (j == 0)) {
					p.print("\t" + array[j][i] + "\t");
				} else {
					p.print(array[j][i] + "\t");
				}
			}
			p.println(" ");
		}
		p.println(
				"yago\t0,02\t0,06\t0,09\t0,13\t0,16\t0,20\t0,01\t0,02\t0,02\t0,03\t0,03\t0,02\t0,03\t0,04\t0,04\t0,05\t0,06\t0,07\t0,08\t1,00");
		p.close();
		System.out.println("File: " + f.getAbsolutePath());
	}

	private static String[] getDatasets(String file) throws IOException {
		Set<String> datasets = new TreeSet<String>();

		BufferedReader TSVFile = new BufferedReader(new FileReader(file));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			if (dataArray[0].equalsIgnoreCase("dataset_source") || dataArray[1].equalsIgnoreCase("dataset_target")) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			datasets.add(shortName(dataArray[0].toLowerCase()));
			datasets.add(shortName(dataArray[1].toLowerCase()));

			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		String[] a = new String[datasets.size()];
		return datasets.toArray(a);
	}

	private static String shortName(String name) throws IOException {
		BufferedReader TSVFile = new BufferedReader(new FileReader("names.tsv"));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			if (dataArray[1].equalsIgnoreCase(name)) {
				TSVFile.close();
				return dataArray[0];
			}
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		return null;
	}

	private static String score(String d1, String d2, String file) throws IOException {
		if (d1.equalsIgnoreCase(d2))
			return "1.0";
		BufferedReader TSVFile = new BufferedReader(new FileReader(file));

		String dataRow = TSVFile.readLine(); // Read first line.

		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			String dt = shortName(dataArray[1]);
			String score = dataArray[2].replaceAll(",", ".");
			if ((d1.equalsIgnoreCase(ds) && d2.equalsIgnoreCase(dt))
					|| (d1.equalsIgnoreCase(dt) && d2.equalsIgnoreCase(ds))) {
				TSVFile.close();
				return score;
			}
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		// Close the file once all data has been read.
		TSVFile.close();
		return null;
	}

	public static void generateJsonGraph(String fileName, double simThreshold) throws IOException {
		StringBuffer sb = new StringBuffer();
		File fOut = new File("WebContent/cyto.json");
		PrintWriter p = new PrintWriter(fOut);
		sb.append("{\n\"nodes\": [\n");

		String[] datasets = getDatasets(fileName);

		for (int i = 0; i < datasets.length; i++) {
			sb.append(
					"{\n" + "\"data\": {\"id\": \"" + datasets[i] + "\", \"label\": \"" + datasets[i] + "\"}\n" + "},");
		}
		String s = sb.substring(0, sb.length() - 1);
		sb = null;
		sb = new StringBuffer();
		sb.append(s);

		sb.append("],\n" + "            \"edges\": [");

		BufferedReader TSVFile = new BufferedReader(new FileReader(fileName));
		String dataRow = TSVFile.readLine(); // Read first line.
		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			if (ds == null) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			String dt = shortName(dataArray[1]);
			String score = dataArray[2];
			if (Double.parseDouble(score.replaceAll(",", ".")) < simThreshold) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			sb.append("{\n\"data\": {\n" + "            \"id\": \"" + (ds + dt) + "\",\n" + "\"label\": \"" + score
					+ "\"," + "                   \"source\": \"" + ds + "\",\n" + "                   \"target\": \""
					+ dt + "\"\n" + "            }\n" + "            },");
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		TSVFile.close();
		s = sb.substring(0, sb.length() - 1);
		sb = null;
		sb = new StringBuffer();
		sb.append(s);

		sb.append("]    \n" + "    }");
		p.println(sb.toString());
		p.close();
		System.out.println("File generated at: " + fOut.getAbsolutePath());
	}

	public static void generateCytoHtml(String fileName, double simThreshold) throws IOException {
		StringBuffer sb = new StringBuffer();
		File fOut = new File("WebContent/graph.html");
		PrintWriter p = new PrintWriter(fOut);
		sb.append(getHeaderHtml());

		String[] datasets = getDatasets(fileName);

		for (int i = 0; i < datasets.length; i++) {
			sb.append("\n{ group: 'nodes', data: { id: '" + datasets[i] + "', label: '" + datasets[i] + "' } },\n");
		}

		BufferedReader TSVFile = new BufferedReader(new FileReader(fileName));
		String dataRow = TSVFile.readLine(); // Read first line.
		while (dataRow != null) {
			String[] dataArray = dataRow.split("\\t");
			String ds = shortName(dataArray[0]);
			if (ds == null) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			String dt = shortName(dataArray[1]);
			String score = dataArray[2].replaceAll(",", ".");
			if (Double.parseDouble(score) < simThreshold) {
				dataRow = TSVFile.readLine(); // Read next line of data.
				continue;
			}
			sb.append("\n{ group: 'edges', data: { id: '" + ds + "-" + dt + "', source: '" + ds + "', target: '" + dt
					+ "', label: " + score + " } },");
			dataRow = TSVFile.readLine(); // Read next line of data.
		}
		TSVFile.close();
		String s = sb.substring(0, sb.length() - 1);
		sb = null;
		sb = new StringBuffer();
		sb.append(s);

		sb.append(getRestHtml());
		p.println(sb.toString());
		p.close();
		System.out.println("File generated at: " + fOut.getAbsolutePath());
	}

	private static Object getRestHtml() {
		String ret = "]);\n" + "\n" + "cy.on('click', 'node', function (evt) {\n" + "  var node = evt.target;\n"
				+ "  console.clear();\n" + "  console.log(node.position());\n" + "});\n" + "    </script>\n"
				+ "</body>\n" + "</html>";
		return ret;
	}

	private static Object getHeaderHtml() {
		String ret = "  \n" + "<html lang=\"en\">\n" + "<title>Cytoscape ShaclGui</title>\n" + "<style>\n" + "#cy {\n"
				+ "  width: 800px;\n" + "  height: 600px;\n" + "  display: block;\n" + "  background-color: #fff;\n"
				+ "}\n" + "</style>\n" + "<script>\n" + "  window.console = window.console || function(t) {};\n"
				+ "</script>\n" + "<script>\n" + "  if (document.location.search.match(/type=embed/gi)) {\n"
				+ "    window.parent.postMessage(\"resize\", \"*\");\n" + "  }\n" + "</script>\n" + "</head>\n"
				+ "<body translate=\"no\">\n" + "<div id=\"cy\"></div>\n"
				+ "<script src=\"https://static.codepen.io/assets/common/stopExecutionOnTimeout-157cd5b220a5c80d4ff8e0e70ac069bffd87a61252088146915e8726e5d9f147.js\"></script>\n"
				+ "<script src='https://cdnjs.cloudflare.com/ajax/libs/cytoscape/3.9.4/cytoscape.min.js'></script>\n"
				+ "<script id=\"rendered-js\">\n" + "let cy = cytoscape({\n"
				+ "                container: document.getElementById('cy'),\n" + "                elements: data,\n"
				+ "                style: [\n" + "                    {\n"
				+ "                        selector: 'node',\n" + "                        style: {\n"
				+ "                            'label': 'data(label)',\n"
				+ "                            'width': '60px',\n" + "                            'height': '60px',\n"
				+ "                            'color': 'blue',\n"
				+ "                            'background-fit': 'contain',\n"
				+ "                            'background-clip': 'none'\n" + "                        }\n"
				+ "                    }, {\n" + "                        selector: 'edge',\n"
				+ "                        style: {\n" + "                        	'label': 'data(label)',\n"
				+ "                           'text-background-color': 'yellow',\n"
				+ "                            'text-background-opacity': 0.4,\n"
				+ "                            'width': '6px',\n"
				+ "                            'target-arrow-shape': 'triangle',\n"
				+ "                            'control-point-step-size': '140px'\n" + "                        }\n"
				+ "                    }\n" + "                ],\n" + "                layout: {\n"
				+ "                    name: 'circle'\n" + "                }\n" + "            });" + "\n"
				+ "cy.add([";
		return ret;
	}

	public static void main(String args[]) throws IOException {
		//String fileName = "tabEdgardJaccard.tsv";
		String fileName = "tabEdgardContained.tsv";
		//generateTablePaper(fileName);
		generateTableTex(fileName);
		// generateJsonGraph(fileName, 0.01);
		// generateCytoHtml(fileName, 0.02);
	}
}
