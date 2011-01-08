/*
			try {
				this.getWriter(parts[2], false);
			} catch (IOException e) {
				e.printStackTrace();
			}
			try {
				printReader(this.getReader(parts[2]));
			} catch (FileNotFoundException e) {
				System.out.println("10 File does not exist");
			}
			try {
				this.getWriter(parts[2], false).write(parts[3]);
			} catch (IOException e) {
				System.out.println("10 File does not exist");
			}
 */
import java.io.IOException;

import edu.washington.cs.cse490h.lib.PersistentStorageReader;
import edu.washington.cs.cse490h.lib.Utility;

public class DistNode extends RIONode {

	@Override
	public void onRIOReceive(Integer from, int protocol, byte[] msg) {
		
	}

	@Override
	public void start() {
		
	}

	@Override
	public void onCommand(String command) {
		String[] parts = command.split(" ");
		int server = Integer.parseInt(parts[1]);
		
		if(parts[0].equals("create")){
			this.RIOLayer.RIOSend(server, Protocol.DATA, Utility.stringToByteArray(command));
		}else if(parts[0].equals("get")){
			
		}else if(parts[0].equals("put")){
			
		}else if(parts[0].equals("append")){
			
		}else if(parts[0].equals("delete")){
			
		}else{
			
		}
	}
	
	private void printReader(PersistentStorageReader r){
		try{
			String line = r.readLine();
			while(line != null){
				System.out.println(line);
				line = r.readLine();
			}
		}catch(IOException e){
			e.printStackTrace();
		}
	}
}
