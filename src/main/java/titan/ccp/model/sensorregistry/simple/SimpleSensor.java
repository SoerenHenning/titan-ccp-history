package titan.ccp.model.sensorregistry.simple;

import java.util.ArrayList;
import java.util.List;

//TODO remove
public class SimpleSensor {

	private String name;

	private List<SimpleSensor> children = new ArrayList<>();

	public SimpleSensor() {
	}

	public String getName() {
		return this.name;
	}

	public void setName(final String name) {
		this.name = name;
	}

	public List<SimpleSensor> getChildren() {
		return this.children;
	}

	public void setChildren(final List<SimpleSensor> children) {
		this.children = children;
	}

}
