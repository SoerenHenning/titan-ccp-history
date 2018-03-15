package titan.ccp.model.sensorregistry.simple;

import java.util.ArrayList;
import java.util.List;

public class SimpleSensor {

	private String name;
	
	private List<SimpleSensor> children = new ArrayList<>();

	public SimpleSensor() {}
	
	
	
	public String getName() {
		return name;
	}



	public void setName(String name) {
		this.name = name;
	}



	public List<SimpleSensor> getChildren() {
		return children;
	}

	public void setChildren(List<SimpleSensor> children) {
		this.children = children;
	}
	
	
}
