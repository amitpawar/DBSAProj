import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.io.BufferedInputStream;


public class TypeBufferedWithBufferSizeInputStream {
	
	File file = null;
	InputStream inputStream = null;
	BufferedInputStream bufferInput = null;
	DataInputStream dataInput = null;
	int bufferSize = 2048;
	
	
	public void open(String filePath) throws FileNotFoundException
	{
		File file = new File(filePath);
		inputStream = new FileInputStream(file);
		bufferInput = new BufferedInputStream(inputStream,bufferSize);
		dataInput = new DataInputStream(bufferInput);
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
