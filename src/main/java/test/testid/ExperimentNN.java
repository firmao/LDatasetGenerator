package test.testid;

import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;

public class ExperimentNN extends Experiment {
	private Set<String> manyDs;

	public Set<String> getManyDs() {
		return manyDs;
	}

	public void setManyDs(Set<String> pManyDs) {
		Set<String> nManyDs = new LinkedHashSet<String>();
		for (String d : pManyDs) {
			if (!d.endsWith(".hdt")) {
				try {
					nManyDs.add(convertHDT(d));
				} catch (Exception e) {
					System.err.println("Problem converting file to HDT: " + d);
				}
			} else {
				nManyDs.add(d);
			}
		}

		this.manyDs = nManyDs;
	}

	public void compareNN() throws IOException {
		final Map<String, String> mapMatches = new LinkedHashMap<String, String>();
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();

		Set<String> mDs = getManyDs();
		Set<String> mDt = getManyDt();
		for (String ds : mDs) {
			for (String dt : mDt) {
				try {
					mapMatches.putAll(PropertyMatchingNN.schemaMatching(ds, dt));
					if(mapMatches.size() > 0) {
						mapPropsDs.put(dt, mapMatches);
					}	
				} catch (IOException e) {
					e.printStackTrace();
				}
			}
		}
		writeDsPropsMatched(mapPropsDs);
		evaluateGoldDir(mapPropsDs);
	}
	
	public void compare1N() throws IOException {
		final Map<String, Map<String, String>> mapPropsDs = new HashMap<String, Map<String, String>>();
		Set<String> dts = getManyDt();
		
		for (String dt : dts) {
		//dts.parallelStream().forEach(dt -> {
			try {
				mapPropsDs.put(dt, PropertyMatchingNN.schemaMatching(getDs(), dt));
			} catch (IOException e) {
				e.printStackTrace();
			}
		//});
		}
		writeDsPropsMatched(mapPropsDs);
		evaluateGoldDir(mapPropsDs);
	}
}
