import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.regex.Pattern;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.PersistentStorageWriter;
import edu.washington.cs.cse490h.lib.Utility;

public class DistNode extends RIONode {
	
	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		String data = Utility.byteArrayToString(msg);
		
		switch(protocol){
			case RPCProtocol.APPEND: case RPCProtocol.PUT:
				int indexOfSpace = data.indexOf(' ');
				String fileName = data.substring(0, indexOfSpace);
				String content = data.substring(indexOfSpace + 1);
				if(content.charAt(0) == '"')
					content = content.substring(1, content.length() - 1);
				
				try {
					//Unescape newline character
					content = content.replaceAll("\\\\n", "\n");
					if(fileExists(fileName))
						if(protocol == RPCProtocol.PUT)
							putFile(fileName, content);
						else{
							this.getWriter(fileName, true).write(content);
						}
					else
						throw new IOException();
				} catch (IOException e) {
					returnError(from, protocol, fileName, Error.ERR_10);
				}
				break;
			case RPCProtocol.CREATE:
				try {
					if(!fileExists(data))
						this.getWriter(data, false);
					else
						returnError(from, protocol, data, Error.ERR_11);
					
				} catch (IOException e) {
					e.printStackTrace();
				}
				break;
			case RPCProtocol.DELETE:
				try{
					if(fileExists(data))
						this.getWriter(data, true).delete();
					else
						throw new IOException();
				}catch(IOException e){
					returnError(from, protocol, data, Error.ERR_10);
				}
				break;
			case RPCProtocol.GET:
				try {
					returnFile(from, data);
				} catch (Exception e) {
					returnError(from, protocol, data, Error.ERR_10);
				}
				break;
			default:
				return;
		}
	}
	
	private void returnError(int source, int protocol, String fileName, int errCode){
		String error = buildErrorString(this.addr, source, protocol, fileName, errCode);
		this.RIOLayer.returnRIO(source, RTNProtocol.ERROR, Utility.stringToByteArray(error));
	}
	
	private void returnData(int source, byte[] payload){
		//TODO
	}
	
	/**
	 * Handles the special case of replacing a file's contents with PUT
	 * @param fileName The name of the file to overwrite
	 * @param content The content to replace the file with.
	 * @throws IOException 
	 */
	private void putFile(String fileName, String content) throws IOException{
		PersistentStorageReader oldFile = this.getReader(fileName);
		PersistentStorageWriter temp = getWriter(".temp", false);
		
		copyFile(oldFile, temp, fileName + "\n");
		
		PersistentStorageWriter newFile = getWriter(fileName, false);
		newFile.write(content);
		temp.delete();
	}
	
	/**
	 * @param fileName The name of the file to check for
	 * @return true if the file exists, false otherwise
	 */
	private boolean fileExists(String fileName){
		try {
			this.getReader(fileName);
			return true;
		} catch (FileNotFoundException e) {
			return false;
		}
	}
	
	/**
	 * Copies the file in r to the file in w, appending start to the beginning of the file
	 * @param r Buffer to read from - source
	 * @param w Buffer to write to - destination
	 * @param start String to append to the beginning of the file
	 * @throws IOException
	 */
	private void copyFile(PersistentStorageReader r, PersistentStorageWriter w, String start) throws IOException{
		String file = start;
		String line = r.readLine(); 
		while(line != null){
			file += line + "\n";
			line = r.readLine();
		}
		w.write(file);
	}
	
	/**
	 * Prints out an error in the following form Node #{addr}: Error: #{protocol} on server #{server} and file #{fileName} returned error code #{code}"
	 * @param dest server node
	 * @param source client node
	 * @param protocol protocol used
	 * @param fileName name of file in command
	 * @param code error code returned as defined by Error class
	 */
	static String buildErrorString(int dest, int source, int protocol, String fileName, int code){
		return "Node " + source + ": Error: " + RPCProtocol.protocolToString(protocol) + " on server " + 
						dest + " and file " + fileName + " returned error code " + Error.ERROR_STRINGS[code];
	}
	
	@Override
	/**
	 * Starts up the node. Checks for unfinished PUT commands.
	 */
	public void start() {
		if(fileExists(".temp")){
			try{
				PersistentStorageReader tempR = this.getReader(".temp");
				String fileName = tempR.readLine();
				
				if (fileName == null)
					this.getWriter(".temp", false).delete();
				else{
					copyFile(tempR, this.getWriter(fileName, false), "");
					this.getWriter(".temp", false).delete();
				}
			}catch(IOException e){
				e.printStackTrace();
			}
		}
	}

	@Override
	/**
	 * Handles given command. Prints error if command is not properly formed.
	 */
	public void onCommand(String command) {
		command = command.trim();
		if(!Pattern.matches("^((create|get|delete) [^ ]+ [^ ]+|(put|append) [^ ]+ [^ ]+ (\\\".+\\\"|[^ ]+))$", command)){
			System.out.println("Node: " + this.addr + " Error: Invalid command: " + command);
			return;
		}
		
		int indexOfSpace = command.indexOf(' ');
		int lastSpace = indexOfSpace + 1;
		String code = command.substring(0, indexOfSpace);
		
		indexOfSpace = command.indexOf(' ', lastSpace);
		int server = Integer.parseInt(command.substring(lastSpace, indexOfSpace));
		lastSpace = indexOfSpace + 1;
		
		indexOfSpace = command.indexOf(' ', lastSpace);
		if(indexOfSpace < 0)
			indexOfSpace = command.length();
		String fileName = command.substring(lastSpace, indexOfSpace);
		lastSpace = indexOfSpace + 1;
		
		if(code.equals("create")){
			this.create(server, fileName);
		}else if(code.equals("get")){
			this.get(server, fileName);
		}else if(code.equals("put")){
			String content = command.substring(lastSpace);
			this.put(server, fileName, content);
		}else if(code.equals("append")){
			String content = command.substring(lastSpace);
			this.append(server, fileName, content);
		}else if(code.equals("delete")){
			this.delete(server, fileName);
		}
	}
	
	private void create(int server, String filename){
		this.RIOLayer.sendRIO(server, RPCProtocol.CREATE, Utility.stringToByteArray(filename));
	}
	
	private void get(int server, String filename){
		this.RIOLayer.sendRIO(server, RPCProtocol.GET, Utility.stringToByteArray(filename));
	}
	
	private void put(int server, String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename + " " + contents);
		if(payload.length > RIOPacket.MAX_PAYLOAD_SIZE)
			System.out.println(buildErrorString(this.addr, server, RPCProtocol.PUT, filename, Error.ERR_30));
		this.RIOLayer.sendRIO(server, RPCProtocol.PUT, payload);
	}
	
	private void append(int server, String filename, String contents){
		byte[] payload = Utility.stringToByteArray(filename + " " + contents);
		if(payload.length > RIOPacket.MAX_PAYLOAD_SIZE)
			System.out.println(buildErrorString(this.addr, server, RPCProtocol.APPEND, filename, Error.ERR_30));
		this.RIOLayer.sendRIO(server, RPCProtocol.APPEND, payload);
	}
	
	private void delete(int server, String filename){
		this.RIOLayer.sendRIO(server, RPCProtocol.DELETE, Utility.stringToByteArray(filename));
	}
	
	/**
	 * Returns a file to the client
	 * @param r Buffer to return
	 * @throws IOException 
	 */
	private void returnFile(int from, String fileName) throws IOException{
		PersistentStorageReader r = this.getReader(fileName);
		String file = "";
		String line = r.readLine();
		while(line != null){
			file += line;
			line = r.readLine();
		}
		
		byte[] rtn = Utility.stringToByteArray(file);
		if(rtn.length > RIOPacket.MAX_PAYLOAD_SIZE)
			this.returnError(from, RPCProtocol.GET, fileName, Error.ERR_30);
		else
			this.returnData(from, rtn);
	}
	

}


