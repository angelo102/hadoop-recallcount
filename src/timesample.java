import java.io.FileWriter;
import java.io.IOException;
import java.util.concurrent.TimeUnit;


public class timesample {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		
		System.out.println("Initial Time");
		long init = System.currentTimeMillis();
		System.out.println(init);
		for(int i=0;i<10;i++){
			int x = i+5;
			x=x+1;
			String s ="20121212";
			//String[] causeArray = s.split(":");
			//String[] causeArray2 = causeArray[0].split(",");
			//String cause = causeArray2[0];
			//System.out.println(cause);
			System.out.println(s.substring(0,4));
			//split_size=max(128,min(Long.MAX_VALUE(default),64))
			
			try {
				FileWriter fw;
				fw = new FileWriter("benchmarks.txt",true);
				fw.append(String.valueOf(i) +"\n");
				fw.close();
			} catch (IOException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
		}
		System.out.println("End Time");
		long fin = System.currentTimeMillis();
		System.out.println(fin);
		
		
		System.out.println("Time Elapsed");
		System.out.println(TimeUnit.SECONDS.convert(fin-init, TimeUnit.MILLISECONDS));
		

	}

}
