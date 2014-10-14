import java.io.DataOutputStream;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;


public class TypeDataOutputStream {
	
	File file = null;
	OutputStream outputStream = null;
	DataOutputStream dataOutput = null;
	
	public void create(String filePath) throws FileNotFoundException
	{
		file = new File(filePath);
		outputStream = new FileOutputStream(file);
		dataOutput = new DataOutputStream(outputStream);
		
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
