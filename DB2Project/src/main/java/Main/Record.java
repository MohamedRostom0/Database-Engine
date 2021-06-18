package Main;
import java.io.Serializable;
import java.util.Date;
import java.util.Hashtable;

public class Record implements Comparable <Record>, Serializable {
	
	private Hashtable <String, Object> record;
	private Object ckValue ;
	
	public Record(String ckColName, Hashtable <String, Object> r) {
		record = r;
		ckValue = record.get(ckColName);
	}

	@Override
	public int compareTo(Record r) {
		if(ckValue instanceof Integer) {
			return Integer.compare((Integer)ckValue, (Integer)r.getCkValue());
		}
		else if(ckValue instanceof String) {
			return ((String)ckValue).compareTo((String)r.getCkValue());
		}
		else if(ckValue instanceof Double) {
			return Double.compare((Double)ckValue, (Double)r.getCkValue());
		}
		else if(ckValue instanceof Date) {
			return ((Date)ckValue).compareTo((Date)r.getCkValue());
		}
		return 0;
	}

	public Hashtable<String, Object> getRecord() {
		return record;
	}

	public void setRecord(Hashtable<String, Object> record) {
		this.record = record;
	}

	public Object getCkValue() {
		return ckValue;
	}

	public void setCkValue(Object ckValue) {
		this.ckValue = ckValue;
	}
	
	public String toString() {
		return this.record.toString();
	}

}
