package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class Logout extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in as given User
												"put logged_in [contents]",
												"txcommit"};
	public Logout(User u, DistNode n){
		super(COMMANDS, n, u);
	}
	
	@Override
	public void onCommandFinish(Command c) {
		
		switch( this.cmds.size() ) {
		case 2:
			if( FacebookOperation.isUserLoggedIn(this.user, this.n.addr, c.getContents())) {
				//not sure what to do here, right now i just remove the user from the file contents
				String newContents = "";
				for( String line : c.getContents().split("\n") ) {
					String tokens[] = line.split(" ");
					//add everything to the newcontents except our username addr combo
					if( !( tokens[0].equals( this.user.getUsername() ) && tokens[1].equals( this.n.addr + "") ) ) {
						newContents += line + "\n";
					}
				}
				String newCommand = FacebookOperation.replaceField( this.nextCommand(), "contents", newContents);
				this.n.onFacebookCommand( newCommand );
			} else {
				this.notLoggedIn();
			}
			break;
		case 1:
			this.n.onFacebookCommand(this.nextCommand());
			break;
		}
		
	}

	@Override
	public void onAbort(Transaction txn) {
		System.out.println("Node " + this.n.addr + ": Error: Cannot execute command: Please try again");
	}

	@Override
	public void onCommit(Transaction txn) {
		this.n.onLogout( this.user );
		System.out.println("Node " + this.n.addr + ": Successfully Logged out user: " + this.user.getUsername());
	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.nextCommand() );
	}

}
