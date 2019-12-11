import java.io.*;
import java.util.*;
import java.nio.file.Files;

public class Merger {

    public static void mergeFileHelper(int noOfChunks, String filename) {
		try {
			String outputFilePath = filename;
			FileOutputStream fostream = new FileOutputStream(outputFilePath);
			String chunk = null;
			for(int num = 0; num < noOfChunks; num++) {
				chunk = "./temp/" + num;
				File file = new File(chunk);
                FileInputStream fistream = new FileInputStream(chunk);
                int filelength = (int) file.length();
				byte[] buffer = new byte[filelength];
				fistream.read(buffer, 0, filelength);
				fostream.write(buffer);
				fostream.flush();
				fistream.close();
			}
			fostream.close();
			System.out.println("Merge file completed. The file is available at " + outputFilePath);
        } catch (FileNotFoundException e) {  
            System.out.println("Some chunks are not available. Will try merging again later");  
        } catch (IOException e) { 
            System.out.println("Some chunks are not available. Will try merging again later");
        }
    }
    
}