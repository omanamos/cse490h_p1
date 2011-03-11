package facebook;

import java.util.LinkedList;
import java.util.Queue;
import java.util.HashSet;

import nodes.DistNode;


import transactions.Command;
import transactions.Transaction;

public abstract class FacebookOperation {
	protected Queue<String> cmds;
	protected HashSet<User> users;
	protected User user;
	protected DistNode n;
	
	protected int commandId;
	
	public FacebookOperation(String[] commands, DistNode n){
		this.n = n;
		this.cmds = new LinkedList<String>();
		for(String s : commands)
			this.cmds.add(s);
	}
	
	public int getCommandId() {
		return this.commandId;
	}
	
	public String nextCommand() {
		return this.cmds.poll();
	}
	
	public abstract void onCommandFinish(Command c);
	public abstract void onAbort(Transaction txn);
	public abstract void onCommit(Transaction txn);
	public abstract void onStart(int txId);
	
	public static HashSet<User> loadUsers( String userString ) {
		HashSet<User> userSet = new HashSet<User>();
		
		String[] allUsers = userString.split("\\n");
		for( String userPass : allUsers ) {
			String[] user = userPass.split(" ");
			userSet.add( new User(user[0], user[1]));
		}
		
		return userSet;
	}
	
	public static boolean isUserLoggedIn( User u, String logString ) {
		String[] loggedUsers = logString.split("\\n");
		for( String user : loggedUsers ) {
			if( user.equals( u.getUsername() ) ) {
				return true;
			}
		}
		return false;
	}
	
	public static String replaceField( String command, String fieldName, String replacement ) {
		return command.replaceAll("[" + fieldName + "]", replacement);
	}

	
}
