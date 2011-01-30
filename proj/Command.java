
public class Command extends Queueable{
	public static final int CREATE = 0;
	public static final int GET = 1;
	public static final int PUT = 2;
	public static final int APPEND = 3;
	public static final int DELETE = 4;
	private static final String[] toS = {"Create", "Get", "Put", "Append", "Delete"};
	
	private int type;
	private String fileName;
	private int dest;
	private String contents;
	
	public Command(int dest, int type, String fileName) throws IllegalArgumentException{
		this(dest, type, fileName, null);
	}
	
	public Command(int dest, int type, String fileName, String contents) throws IllegalArgumentException{
		if(!this.isValidType(type)){
			throw new IllegalArgumentException("Invalid Command Type: " + type);
		}
		this.type = type;
		this.fileName = fileName;
		this.dest = dest;
		if(contents != null){
			if(contents.charAt(0) == '"' && contents.charAt(contents.length() - 1) == '"')
				contents = contents.substring(1, contents.length() - 1);
			contents = contents.replaceAll("\\\\n", "\n");
		}
		this.contents = contents;
	}
	
	public int getType(){
		return this.type;
	}
	
	public String getFileName(){
		return this.fileName;
	}
	
	public int getDest(){
		return this.dest;
	}
	
	public String getContents(){
		return this.contents;
	}
	
	public boolean isValidType(int t){
		return t == CREATE || t == GET || t == PUT || t == APPEND || t == DELETE;
	}
	
	public String toString(){
		return toS[this.type];
	}
}
