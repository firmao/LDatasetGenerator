package test.testid;

import java.io.IOException;
import java.io.InputStreamReader;
import java.net.URL;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.google.gson.Gson;

public class WimuUtil {
	public static Set<String> getDsWIMUs(String uri) throws InterruptedException, IOException {
		Set<String> sRet = new HashSet<String>();

		URL urlSearch = new URL("http://139.18.8.58:8080/LinkLion2_WServ/Find?uri=" + uri);
		InputStreamReader reader = null;
		try {
			reader = new InputStreamReader(urlSearch.openStream());
		} catch (Exception e) {
			Thread.sleep(5000);
			reader = new InputStreamReader(urlSearch.openStream());
		}
		try {
			WIMUDataset[] wData = new Gson().fromJson(reader, WIMUDataset[].class);
			for (WIMUDataset wDs : wData) {
				if (sRet.size() > 10)
					break;
				sRet.add(wDs.getDataset());
				if (sRet.size() < 1) {
					sRet.add(wDs.getHdt());
				}
			}
		} catch (Exception e) {
			System.err.println("No dataset for the URI: " + uri);
		}
		System.out.println("Resource: " + uri + " NumberDatasets: " + sRet.size());
		return sRet;
	}

	public static Map<String, String> getFromWIMUq(String resource) throws InterruptedException, IOException {
		System.out.println("getFromWIMU()  - IMPLEMENT !!!!!");
		final Map<String, String> mPropValue = new HashMap<String, String>();
		Set<String> dsWIMUs = getDsWIMUs(resource);

		for (final String ds : dsWIMUs) {
			if ((ds != null) && ds.contains("dbpedia")) {
				mPropValue.putAll(getEndPoint(ds));
			}
			if ((ds != null) && ds.contains("https://hdt.lod.labs.vu.nl")) {
				mPropValue.putAll(getLODalot(ds));
			}

			try {
				mPropValue.putAll(getRes(ds));
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
		return mPropValue;
	}

	private static Map<String, String> getLODalot(String ds) {
		final Map<String, String> mPropValue = new HashMap<String, String>();
		
		return mPropValue;
	}

	private static Map<String, String>getEndPoint(String ds) {
		final Map<String, String> mPropValue = new HashMap<String, String>();
		
		return mPropValue;
	}

	public static Map<String, String> getRes(String ds) {
		final Map<String, String> mPropValue = new HashMap<String, String>();
		
		return mPropValue;
	}

}
