import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.BufferedOutputStream;


public class TypeBufferedWithBufferSizeOutputStream {
	
	File file = null;
	OutputStream outputStream = null;
	BufferedOutputStream bufferOutput = null;
	DataOutputStream dataOutput = null;
	int bufferSize = 2048;
	
	public void create(String filePath) throws FileNotFoundException
	{
		file = new File(filePath);
		outputStream = new FileOutputStream(file);
		bufferOutput=new BufferedOutputStream(outputStream,bufferSize);
		dataOutput = new DataOutputStream(bufferOutput);
		
	}
	
	public void write(int valueToWrite) throws IOException
	{
		dataOutput.writeInt(valueToWrite);
	}
	
	public void close() throws IOException
	{
		if(dataOutput != null)
		{
			dataOutput.flush();
			dataOutput.close();
		}
		if(outputStream != null)
			outputStream.close();
	
	}



}
