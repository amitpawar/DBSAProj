import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;


public class TypeFileChannelInputStream {

	File file = null;
	RandomAccessFile fileToRead =  null;
	FileChannel inChannel = null;
	MappedByteBuffer buffer = null;
	int counter = 0;
	int bufferPositionCounter = 0;
	int fileSize = 0;
	int bufferSize = 1024;
	
	public void open(String filePath) throws IOException
	{
		file = new File(filePath);
		fileToRead = new RandomAccessFile(file, "rw");
		inChannel = fileToRead.getChannel();
		fileSize = (int)file.length()/4;
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
		buffer = inChannel.map(FileChannel.MapMode.READ_WRITE,bufferPositionCounter*4,bufferSize);
	}
	
	public boolean end_of_stream() throws IOException
	{
		if(counter >= fileSize)
			return true;
		
		else return false;
	}
	
	public void close() throws IOException
	{
		if(inChannel != null)
		{
			buffer.clear();
			inChannel.close();
		}
	}
	
}
