import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class Reg
{
	public static void main(String args[])
	{
		String a = "<doc id=\"214730\" title=\"Henry Hallam\" nonfiltered=\"1\" processed=\"1\" dbindex=\"0\"> Henry Hallam (July 9, 1777 - January 21, 1859) was an English historian. ENDOFARTICLE. </doc><doc id=\"415720\" title=\"Henry Hallam\" nonfiltered=\"1\" processed=\"1\" dbindex=\"0\"> Henry Hgllam (July 9, 1777 - January 21, 1859) wax an English historian. ENDOFARTICLE. </doc>";

		Pattern p = Pattern.compile("<doc id=\"([0-9]+)\" title=\"(.*)\" no.*>(.*)</doc>");
		
		Matcher m = p.matcher(a);
		
		while(m.find())
		{
            System.out.println(m.group(0));
			System.out.println(m.group(1));
			System.out.println(m.group(2));
			System.out.println(m.group(3));
		}
	}
}
