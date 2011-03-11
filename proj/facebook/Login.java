package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class Login extends FacebookOperation{
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in already
												"get users", //check if user exits
												"put logged_in \"[contents]\"",
												"txcommit"};

	public Login(User u, DistNode n){
		super(COMMANDS, n, u);
		this.n.onFacebookCommand( this.nextCommand() );
	}
	
	@Override
	public void onCommandFinish(Command c) {
		
		switch( this.cmds.size() ) {
		case 3:
			//If the user is not logged in then do the next command
			if( !FacebookOperation.isUserLoggedIn(this.user, this.n.addr, c.getContents()) ) {
				//execute next command
				this.n.onFacebookCommand( this.nextCommand() );
			} else {
				this.printError("You are already logged in.");
			}
			break;
		case 2:
			if( FacebookOperation.doesUserExist( this.user, c.getContents() )) {
				String newCommand = FacebookOperation.replaceField(this.nextCommand(), "contents", this.user.getUsername() + " " + this.n.addr );
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("Login information invalid");
			}
			break;
		case 1:
			this.n.onFacebookCommand( this.nextCommand() );
			break;
		}
		
	}

	@Override
	public void onAbort(Transaction txn) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void onCommit(Transaction txn) {
		this.n.onLogin( this.user );
	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.nextCommand() );
		
	}

}
