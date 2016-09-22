import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reg
{
	public static void main(String args[])
	{
		String a = "id=\"214730\"";

		Pattern p = Pattern.compile("id=\"([0-9]+)\"");
		
		Matcher m = p.matcher(a);
		
		while(m.find())
		{
            System.out.println(m.group(0));
			System.out.println(m.group(1));
		}
	}
}
