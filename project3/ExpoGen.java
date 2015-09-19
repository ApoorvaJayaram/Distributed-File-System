package project3;
import java.util.Random;

public class ExpoGen {
	    long value;
		public long generateExpo(double mean)
		{
			Random r =new Random();
			
			while(true)
			{
				value = (long)(-mean*Math.log(r.nextDouble()));
				if(value>0)
					break;
			}
			return value;
			//System.out.println(value);
			
		}

}
