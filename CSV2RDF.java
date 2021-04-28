package test.testid;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.Reader;
import java.io.UnsupportedEncodingException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.UnaryOperator;
import java.util.stream.Collectors;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.jena.rdf.model.Model;
import org.apache.jena.rdf.model.ModelFactory;
import org.apache.jena.rdf.model.Property;
import org.apache.jena.rdf.model.Resource;
import org.apache.jena.shared.PrefixMapping;
import org.apache.jena.vocabulary.OWL;
import org.apache.jena.vocabulary.*;

public class CSV2RDF {

	public static void main(String[] args) {
		// String tsv = "Sepal length\tSepal width\tPetal length\tPetal
		// width\tSpecies\n"
		// + "5.1\t3.5\t1.4\t0.2\tI. setosa\n" + "4.9\t3.0\t1.4\t0.2\tI. setosa";
		StopWatch stopWatch = new StopWatch();
		stopWatch.start();
		String DIR_TSV = "out_tests2";
		//String DIR_TSV = "index_relod";
		File files[] = (new File(DIR_TSV)).listFiles();
		for (File fileTSV : files) {
			long limSize = 10000000; // 10 MB
			if (fileTSV.length() > (limSize * 9)) {
				try {
					Set<File> filesSplited = splitFile(fileTSV);
					for (File fileSplit : filesSplited) {
						String newFileRDF = fileSplit.getAbsolutePath().replaceAll(".tsv", ".nt");
						convTSV(fileSplit, newFileRDF);
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
			} else {
				String newFileRDF = fileTSV.getAbsolutePath().replaceAll(".tsv", ".nt");
				convTSV(fileTSV, newFileRDF);
			}
		}
		stopWatch.stop();
		System.out.println("Stopwatch time: " + stopWatch);
	}

	private static Set<File> splitFile(File fileTSV) throws IOException {
		Set<File> sRet = new HashSet<File>();
		LineIterator it = FileUtils.lineIterator(fileTSV, "UTF-8");
		try {
			int countLines = 0;
			int cFiles = 0;
			int limLines = 754000;
			Set<String> sLines = new HashSet<String>();
			while (it.hasNext()) {
				String line = it.nextLine();
				sLines.add(line);
				countLines++;
				if(countLines > limLines) {
					String newName = fileTSV.getAbsolutePath().replaceAll(".tsv", "") + "_" + (++cFiles) + ".tsv";
					sRet.add(createNewFile(sLines, newName));
					sLines = new HashSet<String>();
					countLines = 0;
				}
			}
		} finally {
			LineIterator.closeQuietly(it);
		}
		return sRet;
	}

	private static File createNewFile(Set<String> sLines, String newName) throws FileNotFoundException, UnsupportedEncodingException {
		PrintWriter writer = new PrintWriter(newName, "UTF-8");
		for(String line: sLines) {
			writer.println(line);
		}
		writer.close();
		return new File(newName);
	}

	private static void convTSV(File fileTSV, String newFileRDF) {
		try {
			Charset ch = StandardCharsets.UTF_8;
			String separator = "\t";
			String ns = "http://relod.org";
			UnaryOperator<String> nameToURI = s -> ns + s.toLowerCase().replace(" ", "_");

			Model m = ModelFactory.createDefaultModel().setNsPrefixes(PrefixMapping.Standard).setNsPrefix("ex", ns);
			Resource clazz = m.createResource(ns + "MyClass", OWL.Class);

			// try (InputStream is = new ByteArrayInputStream(tsv.getBytes(ch));
			try (InputStream is = new ByteArrayInputStream(FileUtils.readFileToByteArray(fileTSV));
					Reader r = new InputStreamReader(is, ch);
					BufferedReader br = new BufferedReader(r)) {
				String first = br.lines().findFirst().orElseThrow(IllegalArgumentException::new);
				List<Property> props = Arrays.stream(first.split(separator))
						.map(s -> m.createResource(nameToURI.apply(s), OWL.DatatypeProperty).addProperty(RDFS.label, s)
								.as(Property.class))
						.collect(Collectors.toList());
				br.lines().forEach(line -> {
					String[] data = line.split(separator);
					if (data.length != props.size())
						throw new IllegalArgumentException();
					Resource individual = m.createResource(clazz);
					for (int i = 0; i < data.length; i++) {
						individual.addProperty(props.get(i), data[i]);
					}
				});
			}
			FileWriter out = new FileWriter(newFileRDF);
			m.write(out, "nt");
			// m.write(System.out, "ttl");
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

}
