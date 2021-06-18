package GridIndex;

import java.io.Serializable;
import java.util.Date;

public class recordReference implements Comparable <recordReference>, Serializable{
	private String pageName;
	private int position;
//	private String value;
	private Object value;
	
	public recordReference(String pageName, int pos, Object value) {
		this.pageName = pageName;
		this.position = pos;
		this.value = value;
	}
	
	
	public String getPageName() {
		return pageName;
	}

	
	public int getPosition() {
		return position;
	}
	
	public Object getValue() {
		return this.value;
	}
	
	@Override
	public int compareTo(recordReference other) {
		if(this.getValue() instanceof Integer) {
			return (Integer)this.getValue() - (Integer)other.getValue();
		}
		else if(this.getValue() instanceof Double) {
			double x = ((Double)this.getValue() - (Double)other.getValue());
			if(x < 0)
				return -1;
			if(x == 0)
				return 0;
			else
				return 1;
//			return (int) ((Double)this.getValue() - (Double)other.getValue());
		}
		else if(this.getValue() instanceof Date) {
			return ((Date)this.getValue()).compareTo((Date) other.getValue());
		}
		else if(this.getValue() instanceof String) {
			return ((String)this.getValue()).compareTo((String)other.getValue());
		}
		return 0;
	}
	
	public String toString() {
		return value + " row = " + position + " page name: " + pageName;
	}
	
	public static void main(String[] args) {

	}
}
