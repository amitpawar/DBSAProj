import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;


public class TypeDataInputStream {
	
	File file = null;
	InputStream inputStream = null;
	DataInputStream dataInput = null;
	
	public void open(String filePath) throws FileNotFoundException
	{
		file = new File(filePath);
		inputStream = new FileInputStream(file);
		dataInput = new DataInputStream(inputStream);
	}
	
	public int read_next() throws IOException
	{
		return dataInput.readInt();
	}
	
	public boolean end_of_stream() throws IOException
	{
		if(dataInput.available() > 0)
			return false;
		
		else
			return true;
	}
	
	public void close() throws IOException
	{
		if(inputStream != null)
			inputStream.close();
		
		if(dataInput != null)
			dataInput.close();
	}

}
