package facebook;

import nodes.DistNode;
import transactions.Command;
import transactions.Transaction;

public class RequestFriend extends FacebookOperation {
	private static final String[] COMMANDS = {	"txstart",
												"get logged_in", //check if logged in as given requester
												"get users", //check friend exists
												"get [friend]_requests", //make sure they haven't already requested us
												"get [username]_requests", //make sure we haven't requested them
												"get [username]_friends", //make sure we aren't already friends with them
												"append [friend]_requests \"[requester]\\n\"",
												"txcommit"};
	
	protected User friend;
	
	public RequestFriend(User requester, User friend, DistNode n){
		super(COMMANDS, n, requester);
		this.friend = friend;
	}
	
	@Override
	public void onCommandFinish(Command c) {
		switch( this.cmds.size()  ) {
		case 6:
			if( FacebookOperation.isUserLoggedIn(this.user, this.n.addr, c.getContents())) {
				this.n.onFacebookCommand( this.nextCommand() );
			} else {
				this.notLoggedIn();
			}
			break;
		case 5:
			if( FacebookOperation.doesUserExist(this.friend, c.getContents())) {
				String newCommand = FacebookOperation.replaceField(this.nextCommand(), "friend", this.friend.getUsername());
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("Error: Node " + this.n.addr + ":User " + this.friend.getUsername() + " does not exist" );
			}
			break;
		case 4:
			if( !doesRequestExistsAlready( c.getContents(), this.friend ) ) {
				String newCommand = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("Error: Node " + this.n.addr + ":User " + this.friend.getUsername() + " has already requested user: " + this.user.getUsername() + " as a friend.");
			}
			break;
		case 3:
			if( !doesRequestExistsAlready( c.getContents(), this.user ) ) {
				String newCommand = FacebookOperation.replaceField(this.nextCommand(), "username", this.user.getUsername());
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("Error: Node " + this.n.addr + ":User " + this.user.getUsername() + " has already requested user: " + this.friend.getUsername() + " as a friend.");
			}
			break;
		case 2:
			if( !doesRequestExistsAlready( c.getContents(), this.user ) ) {
				String replaceFriend = FacebookOperation.replaceField( this.nextCommand(), "friend", this.friend.getUsername() );
				String newCommand = FacebookOperation.replaceField(replaceFriend, "requester", this.user.getUsername());
				this.n.onFacebookCommand( newCommand );
			} else {
				this.printError("Error: Node " + this.n.addr + ":User " + this.user.getUsername() + " is already friends with user: " + this.friend.getUsername() );
			}
			break;
		case 1:
			this.n.onFacebookCommand( this.nextCommand() );
			break;
		}

	}

	private boolean doesRequestExistsAlready( String requestContents, User u ) {
		for( String username : requestContents.split("\n")) {
			if( username.equals( u.getUsername() ) ) {
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
		System.out.println("Success: Node " + this.n.addr + ": User " + this.user.getUsername() + ": Successfully requested friend " + this.friend.getUsername() );

	}

	@Override
	public void onStart(int txId) {
		this.commandId = txId;
		this.n.onFacebookCommand( this.nextCommand() );
		
	}

}
