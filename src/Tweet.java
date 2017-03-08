import java.util.Date;

public class Tweet {
	private Date date;
	private String text;
	
	public Tweet() {
		
	}
	
	public Tweet(Date date, String text) {
		this.date = date;
		this.text = text;
	}
	
	public Date getDate() {
		return date;
	}
	
	public String getText() {
		return text;
	}
	
	public String toString() {
		return date + " " + text;
	}
}
