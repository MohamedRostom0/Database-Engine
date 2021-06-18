package GridIndex;

import java.io.Serializable;

public class Range implements Serializable {
	private Object lowerLimit;
	private Object upperLimit;
	
	public Range(Object l, Object u) {
		lowerLimit = l;
		upperLimit = u;
	}
	
	public String toString() {
		return lowerLimit + " -> " + upperLimit;
	}

	public Object getLowerLimit() {
		return lowerLimit;
	}

	public Object getUpperLimit() {
		return upperLimit;
	}
}
