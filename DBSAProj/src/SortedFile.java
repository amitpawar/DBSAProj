import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedOutputStream;


public class SortedFile {
	
	File file = null;
	OutputStream outputStream = null;
	BufferedOutputStream bufferOutput = null;
	DataOutputStream dataOutput = null;
	InputStream inputStream = null;
	BufferedInputStream bufferInput = null;
	DataInputStream dataInput = null;
	
	
	
	public void create(String filePath) throws FileNotFoundException
	{
		file = new File(filePath);
		outputStream = new FileOutputStream(file);
		bufferOutput=new BufferedOutputStream(outputStream);
		dataOutput = new DataOutputStream(bufferOutput);
		
	}
	
	public void write(int value) throws IOException
	{
		
		dataOutput.writeInt(value);
		dataOutput.flush();
	}
	
	public void closeAfterWrite() throws IOException
	{
		if(outputStream != null)
			outputStream.close();
		
		if(dataOutput != null)
			dataOutput.close();
	}
	
	public void open(String filePath) throws FileNotFoundException
	{
		File file = new File(filePath);
		inputStream = new FileInputStream(file);
		bufferInput = new BufferedInputStream(inputStream);
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
	
	public void closeAfterRead() throws IOException
	{
		if(inputStream != null)
			inputStream.close();
		
		if(dataInput != null)
			dataInput.close();
	}




}
