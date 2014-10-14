import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

public class TypeFileChannelOutputStream {

	File file = null;
	MappedByteBuffer buffer= null;
	FileChannel outChannel = null;
	RandomAccessFile fileToWrite = null;
	int bufferPositionCounter = 0;
	int bufferSize = 1024;
	int totalRecords = 0;
	
	public void create(String filePath) throws IOException
	{
		file = new File(filePath);
		fileToWrite = new RandomAccessFile(file, "rw");
		outChannel = fileToWrite.getChannel();
	}
	
	public void write(int minValue, int maxValue, int noOfRecords) throws IOException
	{
		Random rand= new Random();
		this.totalRecords = noOfRecords;
		
		for(int i = 0 ; i < noOfRecords ; i++)
		{	
			if(buffer == null || buffer.remaining() < 4)
			{
				map();
			}
			bufferPositionCounter++;		//counter to remember the position for next mapping
			
			int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
			//System.out.println(randomNum);
			buffer.putInt(randomNum);
		}
		
		buffer.clear();
		
	}
	
	public void map() throws IOException
	{
		buffer = outChannel.map(FileChannel.MapMode.READ_WRITE,bufferPositionCounter*4 ,bufferSize);		
	}
	
	public void close() throws IOException
	{
		if(outChannel != null)
		{
			buffer.clear();
			outChannel.close();
		}
		
	}
	
		
}
