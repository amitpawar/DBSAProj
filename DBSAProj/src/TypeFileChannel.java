import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.Random;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class TypeFileChannel {
	
	File file = null;
	MappedByteBuffer buffer= null;
	RandomAccessFile fileToTest = null;
	int bufferPositionCounter = 0;
	int bufferSize = 128;
	int totalRecords = 0;
	FileChannel channel = null;
	int counter = 0;
	int fileSize = 0;
	
	
	public void create(String filePath) throws IOException
	{
		file = new File(filePath);
		fileToTest = new RandomAccessFile(file, "rw");
		channel = fileToTest.getChannel();
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
			bufferPositionCounter++;
			
			int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
			System.out.println(+randomNum);
			buffer.putInt(randomNum);
		}
	}

	
	public void open(String filePath) throws IOException
	{
		file = new File(filePath);
		fileToTest = new RandomAccessFile(file, "rw");
		channel = fileToTest.getChannel();
		fileSize = (int)file.length()/4;
		bufferPositionCounter = 0;
	}
	
	public int read_next() throws IOException
	{
		if( buffer == null || buffer.remaining() < 4)
		{
			map();
		}
		bufferPositionCounter++;
		counter++;
		int j = buffer.getInt();
		return j;
		
	}
	
	public void map() throws IOException
	{
		buffer = channel.map(FileChannel.MapMode.READ_WRITE,bufferPositionCounter*4,bufferSize);
	}
	
	public boolean end_of_stream() throws IOException
	{
		if(counter >= fileSize)
			return true;
		
		else return false;
	}
	
	public void closeAfterWrite() throws IOException
	{
		fileToTest.close();
	}
	
	public void closeAfterRead() throws IOException
	{
		buffer.rewind();
		buffer.clear();
		fileToTest.close();
	}

	
}
