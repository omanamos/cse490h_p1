package facebook;

public class User {
	
	private String username;
	private String password;
	
	public User(String username){
		this(username, null);
	}
	
	public User(String username, String password){
		this.username = username;
		this.password = password;
	}
	
	public String getUsername(){
		return this.username;
	}
	
	public String getPassword(){
		return this.password;
	}
	
	public static User fromString(String user){
		String[] parts = user.split("|");
		return new User(parts[0], parts[1]);
	}
	
	public String toString(){
		return this.username + ((this.password == null) ? "" : "|" + this.password);
	}
}
