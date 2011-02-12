
public class Update implements Comparable<Update>{
	public String contents;
	public int version;
	public int source;
	
	public Update(String contents, int version, int source){
		this.contents = contents;
		this.version = version;
		this.source = source;
	}
	
	public int compareTo(Update other){
		if(this.version > other.version)
			return -1;
		else if(this.version < other.version)
			return 1;
		else
			return 0;
	}
}
