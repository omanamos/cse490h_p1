import java.util.Map;

import edu.washington.cs.cse490h.lib.Utility;


public class Command extends Queueable{
	public static final int CREATE = 0;
	public static final int GET = 1;
	public static final int PUT = 2;
	public static final int APPEND = 3;
	public static final int DELETE = 4;
	public static final int UPDATE = 5;
	public static final String[] toS = {"Create", "Get", "Put", "Append", "Delete", "Update"};
	
	private int type;
	private File f;
	private int dest;
	private String contents;
	private String fileName;
	
	public Command(int dest, int type, File f) throws IllegalArgumentException{
		this(dest, type, f, "");
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
	
	public void setContents(String contents){
		if(!contents.isEmpty() && contents.charAt(0) == '"' && contents.charAt(contents.length() - 1) == '"')
			contents = contents.substring(1, contents.length() - 1);
		contents = contents.replaceAll("\\\\n", "\n");
		this.contents = contents;
	}
	
	public int getType(){
		return this.type;
	}
	
	public String getFileName(){
		return this.f == null ? this.fileName : this.f.getName();
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
	
	/**
	 * Each field in the Command object is separated by a space.
	 * The contents of the string are first converted to a byte
	 * array and then put in the string so that no conflicts with
	 * delimeters occur.
	 * @return String representation of a Command
	 */
	public String buildCommit(){
		byte[] tmp = Utility.stringToByteArray(this.contents);
		String contents = "[";
		for(byte b : tmp){
			contents += b + ",";
		}
		contents = contents.length() == 1 ? contents + "]" : contents.substring(0, contents.length() - 1) + "]";
		
		return this.type + " " + this.f.getName() + " " + contents;
	}
	
	/**
	 * @param str string built by Command.buildCommit
	 * @param cache
	 * @return Command parsed from given string
	 */
	public static Command fromByteArray(String str, Map<String, File> cache){
		String[] command = str.split(" ");
		int type = Integer.parseInt(command[0]);
		
		String fileName = command[1];
		if(!cache.containsKey(fileName))
			cache.put(fileName, new MasterFile(File.RW, fileName));
		
		String bytes = command[2].substring(1, command[2].length() - 1);
		if(!bytes.isEmpty()){
			String[] bsa = bytes.split(",");
			byte[] ba = new byte[bsa.length];
			for(int i = 0; i < bsa.length; i++){
				ba[i] = Byte.parseByte(bsa[i]);
			}
			return new Command(TransactionLayer.MASTER_NODE, type, cache.get(fileName), Utility.byteArrayToString(ba));
		}
		return new Command(TransactionLayer.MASTER_NODE, type, cache.get(fileName));
	}
	
	public String toString(){
		if(this.type == PUT || this.type == APPEND)
			return toS[this.type] + " " + this.getFileName() + " " + this.contents.replaceAll("\n", "\\n");
		else
			return toS[this.type] + " " + this.getFileName();
	}
	
	public static String toString(int type){
		try{
			return Command.toS[type];
		}catch(Exception e){
			return TXNProtocol.protocolToString(type);
		}
	}
}
