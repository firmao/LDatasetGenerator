package test.testid;

import org.apache.commons.text.similarity.JaroWinklerDistance;
import org.apache.jena.sparql.expr.NodeValue;

public class JaroWinkler {
	public NodeValue exec(NodeValue value1, NodeValue value2) {
		JaroWinklerDistance jaro = new JaroWinklerDistance();
		double dSim = jaro.apply(value1.asString(), value2.asString());
		return NodeValue.makeDouble(dSim);
	}
}
