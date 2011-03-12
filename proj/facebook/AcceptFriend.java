package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class AcceptFriend extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check that curUser is logged in
												"get users", //check that requester exists -> if not, remove from [curUser]_requests file
												"get [curUser]_requests", //check that requester actually requested to be friends
												"append [curUser]_friends \"[requester]\n\"",
												"append [requester]_friends \"[curUser]\n\"",
												"put [curUser]_requests \"[contents]\"",
												"txcommit"};
	
	private String currentRequests;
	private User requester;
	
	public AcceptFriend(User curUser, User requester, DistNode n){
		super(COMMANDS, n, curUser);
		this.requester = requester;
	}
	
	@Override
	public void onCommandFinish(Command c) {
		String newCommand;
		String replaceCurUser;
		String replaceRequestor;
		
		switch( this.cmds.size() ) {
		case 6:
			if( FacebookOperation.isUserLoggedIn(this.user, this.n.addr, c.getContents())) {
				this.n.onFacebookCommand( this.nextCommand() );
			} else {
				this.notLoggedIn();
			}
			break;
		case 5:
			//get our friend requests
			if( FacebookOperation.doesUserExist(this.requester, c.getContents())) {
				newCommand = FacebookOperation.replaceField(this.nextCommand(), "curUser", this.user.getUsername());
				this.n.onFacebookCommand(newCommand);
			} else {
				this.printError("User " + this.requester.getUsername() + " does not exist.");
			}
			break;
		case 4:
			this.currentRequests = c.getContents();
			if( didUserRequestFriendship() ) {
				
				replaceCurUser = FacebookOperation.replaceField(this.nextCommand(), "curUser", this.user.getUsername());
				replaceRequestor = FacebookOperation.replaceField(replaceCurUser, "requester", this.requester.getUsername());
				this.n.onFacebookCommand(replaceRequestor);
			} else {
				this.printError("User " + this.requester.getUsername() + " did not request to be friends.");
			}
			
			break;
		case 3:
			replaceCurUser = FacebookOperation.replaceField(this.nextCommand(), "curUser", this.user.getUsername());
			replaceRequestor = FacebookOperation.replaceField(replaceCurUser, "requester", this.requester.getUsername());
			this.n.onFacebookCommand(replaceRequestor);
			break;
		case 2:
			String newRequestContents = removeRequesterFromRequests();
			String replaceContents = FacebookOperation.replaceField(this.nextCommand(), "contents", newRequestContents);
			newCommand = FacebookOperation.replaceField(replaceContents, "curUser", this.user.getUsername());
			this.n.onFacebookCommand(newCommand);
			break;
		case 1:
			this.n.onFacebookCommand(this.nextCommand());
			break;
		}

	}
	
	private String removeRequesterFromRequests() {
		String newContents = "";
		for( String username : this.currentRequests.split("\n")) {
			if( !this.requester.equals(username)) {
				newContents += username + "\n";
			}
		}
		return newContents;
	}
	
	private boolean didUserRequestFriendship() {
		for( String user : this.currentRequests.split("\n") ) {
			if( this.requester.getUsername().equals( user ) ) {
				return true;
			}
		}
		return false;
	}

	@Override
	public void onAbort(Transaction txn) {
		System.out.println("Node " + this.n.addr + ": Error: Cannot execute command: Please try again");
	}

	@Override
	public void onCommit(Transaction txn) {
		System.out.println("You are now friends with " + this.requester.getUsername() );

	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.nextCommand() );
		
	}

}
