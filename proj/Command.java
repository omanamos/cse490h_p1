
public class Command extends Queueable{
	public static final int CREATE = 0;
	public static final int GET = 1;
	public static final int PUT = 2;
	public static final int APPEND = 3;
	public static final int DELETE = 4;
	public static final int UPDATE = 5;
	private static final String[] toS = {"Create", "Get", "Put", "Append", "Delete", "Update"};
	
	private int type;
	private File f;
	private int dest;
	private String contents;
	
	public Command(int dest, int type, File f) throws IllegalArgumentException{
		this(dest, type, f, null);
	}
	
	public Command(int dest, int type, File f, String contents) throws IllegalArgumentException{
		if(!this.isValidType(type)){
			throw new IllegalArgumentException("Invalid Command Type: " + type);
		}
		this.type = type;
		this.f = f;
		this.dest = dest;
		if(contents != null && contents.length() > 1){
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
		return this.f.getName();
	}
	
	public File getFile(){
		return this.f;
	}
	
	public int getDest(){
		return this.dest;
	}
	
	public String getContents(){
		return this.contents;
	}
	
	public boolean isValidType(int t){
		return t == CREATE || t == GET || t == PUT || t == APPEND || t == DELETE || t == UPDATE;
	}
	
	public byte[] buildCommit(){
		byte[] rtn = new byte[0];
		//TODO: encode type and filename
		return rtn;
	}
	
	public String toString(){
		return toS[this.type];
	}
}
