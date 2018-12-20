package test.testid;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringReader;
import java.io.UnsupportedEncodingException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Scanner;
import java.util.Set;

import org.apache.commons.compress.archivers.zip.ZipArchiveInputStream;
import org.apache.commons.compress.compressors.bzip2.BZip2CompressorInputStream;
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream;
import org.apache.jena.graph.Triple;
import org.apache.jena.riot.Lang;
import org.apache.jena.riot.RDFParserBuilder;
import org.apache.jena.riot.system.StreamRDF;
import org.apache.jena.sparql.core.Quad;
import org.tukaani.xz.XZInputStream;

public class Util {

	/*
	 * Save the resources in a file
	 */
	public static void writeFile(Set<String> resources, File fResources) throws IOException {
		if(!fResources.exists()) fResources.createNewFile();
		PrintWriter writer = new PrintWriter(fResources.getName(), "UTF-8");
		for (String resource : resources) {
			writer.println(resource.replaceAll("\n", ""));
		}
		writer.close();
	}

	public static void writeFile(String resource, Map<String, Set<String>> mapPropValue) throws FileNotFoundException, UnsupportedEncodingException {
		String s[] = resource.split("/");
		String fileName = "out/" + s[2] + "_" + s[s.length - 1] + ".tsv";
		PrintWriter writer = new PrintWriter(fileName, "UTF-8");
		writer.println("#---Properties and values from: " + resource);
		writer.println("Property\tValue");
		for (Entry<String, Set<String>> entry : mapPropValue.entrySet()) {
			String prop = entry.getKey();
			Set<String> values = entry.getValue();
			for (String value : values) {
				writer.println(prop + "\t" + value);
			}
		}
		writer.close();
	}
	
	public static String getURLFileName(URL pURL) {
		String name = null;
		try {
			String[] str = pURL.getFile().split("/");
			name = str[str.length - 1];
			name = name.replaceAll("=", ".");
		} catch (Exception e) {
			System.err.println("Problem with URL: " + pURL);
		}
		return name;
	}
	
	public static File unconpress(File file) {
		File ret = file;
		try {
			File fUnzip = null;
			if (file.getName().endsWith(".bz2"))
				fUnzip = new File(file.getName().replaceAll(".bz2", ""));
			else if (file.getName().endsWith(".xz"))
				fUnzip = new File(file.getName().replaceAll(".xz", ""));
			else if (file.getName().endsWith(".zip"))
				fUnzip = new File(file.getName().replaceAll(".zip", ""));
			else if (file.getName().endsWith(".tar.gz"))
				fUnzip = new File(file.getName().replaceAll(".tar.gz", ""));
			else if (file.getName().endsWith(".gz"))
				fUnzip = new File(file.getName().replaceAll(".gz", ""));
			else
				return file;

			if (fUnzip.exists()) {
				return fUnzip;
			}
			BufferedInputStream in = new BufferedInputStream(new FileInputStream(file));
			FileOutputStream out = new FileOutputStream(fUnzip);

			if (file.getName().endsWith(".bz2")) {
				BZip2CompressorInputStream bz2In = new BZip2CompressorInputStream(in);
				synchronized (bz2In) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = bz2In.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					bz2In.close();
				}
			} else if (file.getName().endsWith(".xz")) {
				XZInputStream xzIn = new XZInputStream(in);
				synchronized (xzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = xzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					xzIn.close();
				}
			} else if (file.getName().endsWith(".zip")) {
				ZipArchiveInputStream zipIn = new ZipArchiveInputStream(in);
				synchronized (zipIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = zipIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					zipIn.close();
				}
			} else if (file.getName().endsWith(".tar.gz") || file.getName().endsWith(".gz")) {
				GzipCompressorInputStream gzIn = new GzipCompressorInputStream(in);
				synchronized (gzIn) {
					final byte[] buffer = new byte[8192];
					int n = 0;
					while (-1 != (n = gzIn.read(buffer))) {
						out.write(buffer, 0, n);
					}
					out.close();
					gzIn.close();
				}
			}

			// file.delete();

			if (fUnzip != null)
				ret = fUnzip;
		} catch (Exception ex) {
			ret = file;
		}
		return ret;
	}
	
	/**
	 * Read the IRI content type by opening an HTTP connection. We set the value
	 * 'application/rdf+xml' to the ACCEPT request property for handling
	 * dereferenceable IRIs.
	 */
	public static String getContentType(String iri) {
		String contentType = "";
		try {
			URL url = new URL(iri);
			HttpURLConnection connection = (HttpURLConnection) url.openConnection();
			connection.setRequestMethod("HEAD");
			connection.setRequestProperty("ACCEPT", "application/rdf+xml");
			connection.connect();
			contentType = connection.getContentType();
			if (contentType == null) {
				contentType = "";
			}
		} catch (MalformedURLException ex) {
			ex.printStackTrace();
		} catch (IOException ex) {
			ex.printStackTrace();
		}
		return contentType;
	}
	
	public static Map<String, Set<String>> processUnzipRDF(File fUnzip, String resource) {
		Map<String, Set<String>> mPropValue = new HashMap<String, Set<String>>();
		try {
			StreamRDF reader = new StreamRDF() {

				@Override
				public void base(String arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void finish() {
					// TODO Auto-generated method stub

				}

				@Override
				public void prefix(String arg0, String arg1) {
					// TODO Auto-generated method stub

				}

				@Override
				public void quad(Quad arg0) {
					// TODO Auto-generated method stub

				}

				@Override
				public void start() {
					// TODO Auto-generated method stub

				}

				@Override
				public void triple(Triple triple) {
					if(resource.equals(triple.getSubject().toString())) {
						if(mPropValue.containsKey(triple.getPredicate().toString().trim())) {
							mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getObject().toString());
						} else {
							Set<String> value = new HashSet<String>();
							value.add(triple.getObject().toString());
							mPropValue.put(triple.getPredicate().toString(), value);
						}
					} else {
						if(mPropValue.containsKey(triple.getPredicate().toString().trim())) {
							mPropValue.get(triple.getPredicate().toString().trim()).add(triple.getSubject().toString());
						} else {
							Set<String> value = new HashSet<String>();
							value.add(triple.getSubject().toString());
							mPropValue.put(triple.getPredicate().toString(), value);
						}
						//mPropValue.put(triple.getPredicate().toString(), triple.getSubject().toString());
					}
				}
			};
			RDFParserBuilder a = RDFParserBuilder.create();
			
			if (fUnzip.getName().endsWith(".tql")) {
				a.forceLang(Lang.NQUADS);
			} else if (fUnzip.getName().endsWith(".ttl")) {
				a.forceLang(Lang.TTL);
			} else {
				a.forceLang(Lang.RDFXML);
			}
			Scanner in = null;
			try {
				in = new Scanner(fUnzip);
				while(in.hasNextLine()) {
					a.source(new StringReader(in.nextLine()));
					try {
						a.parse(reader);
					} catch (Exception e) {
						//e.printStackTrace();
					}
				}
				in.close();
			} catch (FileNotFoundException e) {
				//e.printStackTrace();
			}
			fUnzip.delete();
		} catch (Exception ex) {
			System.err.println("Error: " + resource + ": " + ex.getMessage());
			return mPropValue;
		}
		return mPropValue;
	}
}
