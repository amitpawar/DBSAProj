
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.util.Arrays;
import java.util.Random;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;




public class MergeSort {
	
	//Test Method
	public void WriteToFileUsingDataStream(String filePath, int minValue, int maxValue, int noOfRecords)
	{
		File file = new File(filePath);
		Random rand= new Random();
		
		try {
			
			if(!file.exists())
				file.createNewFile();
			
			OutputStream outputStream = new FileOutputStream(file);
			DataOutputStream dataOutput=new DataOutputStream(outputStream);
			
			for(int i = 0 ; i < noOfRecords ; i++)
			{
				int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
				dataOutput.writeInt(randomNum);
			}
			
			dataOutput.flush();
			outputStream.close();
			dataOutput.close();
		}
		
		catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	//Test Method
	public void WriteToFileUsingBufferStream(String filePath, int minValue, int maxValue, int noOfRecords)
	{
		File file = new File(filePath);
		Random rand= new Random();
		
		try {
			
			if(!file.exists())
				file.createNewFile();
			
			OutputStream outputStream = new FileOutputStream(file);
			BufferedOutputStream bufferOutput=new BufferedOutputStream(outputStream);
			DataOutputStream dataOutput=new DataOutputStream(bufferOutput);
			
			for(int i = 0 ; i < noOfRecords ; i++)
			{
				int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
				dataOutput.writeInt(randomNum);
			}
			
			dataOutput.flush();
			outputStream.close();
			dataOutput.close();
		}
		
		catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	//Test Method
	public void WriteToFileUsingBufferStreamWithBufferSize(String filePath, int minValue, int maxValue, int noOfRecords,int bufferSize)
	{
		File file = new File(filePath);
		Random rand= new Random();
		
		try {
			
			if(!file.exists())
				file.createNewFile();
			
			OutputStream outputStream = new FileOutputStream(file);
			BufferedOutputStream bufferOutput=new BufferedOutputStream(outputStream,bufferSize);
			DataOutputStream dataOutput=new DataOutputStream(bufferOutput);
			
			for(int i = 0 ; i < noOfRecords ; i++)
			{
				int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
				dataOutput.writeInt(randomNum);
			}
			
			dataOutput.flush();
			outputStream.close();
			dataOutput.close();
		}
		
		catch (Exception e) {
			e.printStackTrace();
		} 
		
	}
	
	//Test Method
	public void WriteToFileUsingFileChannel(String filePath, int minValue, int maxValue, int noOfRecords,int bufferSize)
	{
		File file = new File(filePath);
		Random rand= new Random();
		
		try {
			
			if(!file.exists())
				file.createNewFile();
			
			ByteBuffer buffer=ByteBuffer.allocate(bufferSize);
			buffer.clear();
			
			FileOutputStream outputStream = new FileOutputStream(file);
			FileChannel outChannel = outputStream.getChannel();

			while(noOfRecords >= 0)
			{
				
				for(int i = 0 ; i < buffer.remaining() ; i++)
				{
					int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
					System.out.println(randomNum);
					buffer.putInt(randomNum);
					noOfRecords --;
				}
				outChannel.write(buffer);
				buffer.clear();
				//buffer.flip();
				
			}
			
			/*while(buffer.hasRemaining())
			{
				outChannel.write(buffer);
			}*/
			
			outputStream.close();
			
		}
		
		catch (Exception e) {
			e.printStackTrace();
		} 
		
	}

	//Test Method
	public void ReadFromFileUsingDataStream(String filePath)
	{
		File file = new File(filePath);
		
		
		try
		{
			InputStream inputStream = new FileInputStream(file);
			DataInputStream dataInput = new DataInputStream(inputStream);
				
			while(dataInput.available() > 0)
			{
			    dataInput.readInt();
			}
			
			dataInput.close();
		}
		
		catch(EOFException ex)
		{
				System.out.println("Read complete.");
				
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		
	}
	
	//Test Method
	public void ReadFromFileUsingBufferStream(String filePath)
	{
		File file = new File(filePath);
		
		
		try
		{
			InputStream inputStream = new FileInputStream(file);
			BufferedInputStream bufferInput = new BufferedInputStream(inputStream);
			DataInputStream dataInput = new DataInputStream(bufferInput);
			
			while(dataInput.available() > 0)
			{
			    dataInput.readInt();
			}
			
			dataInput.close();
		}
		
		catch(EOFException ex)
		{
				System.out.println("Read complete.");
				
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}

	//Test Method
	public void ReadFromFileUsingFileChannel(String filePath,int bufferSize)
	{
		File file = new File(filePath);
		
		try
		{
			FileInputStream inputStream = new FileInputStream(file);
			FileChannel inChannel = inputStream.getChannel();
			
			System.out.println("Reading from FileChannel");			
			
			ByteBuffer buffer = ByteBuffer.allocate((int)file.length());
			
			
			while((inChannel.read(buffer))!= -1)
			{
				buffer.flip();
				System.out.println("Capacity "+buffer.capacity());
				
				for(int i=0;i < buffer.capacity()/4;i++)
				{
					int j = buffer.getInt();
					System.out.println(+j);
				}
				buffer.clear();
				
			}
			
			inChannel.close();
			inputStream.close();
		}
		
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	//Test Method 
	public void ReadFromFileUsingBufferStreamWithBufferSize(String filePath,int bufferSize)
	{
		File file = new File(filePath);
		
		
		try
		{
			InputStream inputStream = new FileInputStream(file);
			BufferedInputStream bufferInput = new BufferedInputStream(inputStream,bufferSize);
			DataInputStream dataInput = new DataInputStream(bufferInput);
			
			while(dataInput.available() > 0)
			{
			    dataInput.readInt();
			}
			
			bufferInput.close();
			dataInput.close();
		}
		
		catch(EOFException ex)
		{
				System.out.println("Read complete.");
				
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public long StopWatchStart()
	{
		return System.currentTimeMillis();
	}
	
	public long StopWatchStop()
	{
		return System.currentTimeMillis();
	}
	
	public long TimeTaken(long startTime,long endTime)
	{
		return endTime - startTime;
	}
	
	//DataStream
	//By default -
	//range of integers : 1 - 99
	//number of integers : 1024
	//tested the read on 1gb file by switching N = 1 and k = 1 and opening the appropriated file
	public void TestMethodOne(int N, int k)  throws IOException
	{
		int minValue = 1;
		int maxValue = 99;
		int noOfRecords = 1024;
		Random rand = new Random();
		TypeDataOutputStream[] dataOutput = new TypeDataOutputStream[k];
		TypeDataInputStream[] dataInput = new TypeDataInputStream[k];
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				
				dataOutput[i] = new TypeDataOutputStream();
				dataOutput[i].create("C://Data/DataStreamFile" + i +".txt");
				
				for(int m= 0 ; m < noOfRecords ; m++)
				{
					int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
					dataOutput[i].write(randomNum);
					//System.out.println(randomNum);
				}
			}
		}
		
		for(int i=0;i<k;i++)
		{
			dataOutput[i].close();
		}
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				dataInput[i] = new TypeDataInputStream();
				//System.out.println("Reading");
				dataInput[i].open("C://Data/DataStreamFile" + i +".txt");
				while(!dataInput[i].end_of_stream())
					dataInput[i].read_next();
					//System.out.println(dataInput[i].read_next());
			}
		}
		
		for(int i=0;i<k;i++)
		{
			dataInput[i].close();
		}
		
	}

	//BufferedStream with no mention of size
	//By default -
	//range of integers : 1 - 99
	//number of integers : 1024
	//tested the read on 1gb file by switching N = 1 and k = 1 and opening the appropriated file
	public void TestMethodTwo(int N, int k) throws IOException
	{
		int minValue = 1;
		int maxValue = 99;
		int noOfRecords = 1024;
		Random rand= new Random();
		TypeBufferedOutputStream[] bufferOutput = new TypeBufferedOutputStream[k];
		TypeBufferedInputStream[] bufferInput = new TypeBufferedInputStream[k];
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				bufferOutput[i] = new TypeBufferedOutputStream();
				bufferOutput[i].create("C://Data/BufferStreamFile"+ i +".txt");
			
				for(int m= 0 ; m < noOfRecords ; m++)
				{
					int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
					bufferOutput[i].write(randomNum);
					//System.out.println(randomNum);
				}
			}
		}
		for(int i=0;i<k;i++)
		{
			bufferOutput[i].close();
		}
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				bufferInput[i] = new TypeBufferedInputStream();
				//System.out.println("Reading -");
				bufferInput[i].open("C://Data/BufferStreamFile"+ i +".txt");
				while(!bufferInput[i].end_of_stream())
					bufferInput[i].read_next();
			}
		}
		
		for(int i=0;i<k;i++)
		{
			bufferInput[i].close();
		}
	}
	
	//Buffered Stream with buffer size
	//By default -
	//range of integers : 1 - 99
	//number of integers : 1024
	//tested the read on 1gb file by switching N = 1 and k = 1 and opening the appropriated file
	public void TestMethodThree(int N, int k) throws IOException
	{
		int minValue = 1;
		int maxValue = 99;
		int noOfRecords = 2048;
		Random rand = new Random();
		TypeBufferedWithBufferSizeOutputStream[] bufferOutput = new TypeBufferedWithBufferSizeOutputStream[k];
		TypeBufferedWithBufferSizeInputStream[] bufferInput = new TypeBufferedWithBufferSizeInputStream[k];
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				bufferOutput[i] = new TypeBufferedWithBufferSizeOutputStream();
				bufferOutput[i].create("C://Data/BufferStreamWithBufferSizeFile "+ i +".txt");
				
				for(int m= 0 ; m < noOfRecords ; m++)
				{
					int randomNum = rand.nextInt((maxValue - minValue)+1) + minValue;
					bufferOutput[i].write(randomNum);
					//System.out.println(randomNum);
				}
			}
		}
		
		for(int i=0;i<k;i++)
		{
			bufferOutput[i].close();
		}
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				bufferInput[i]=new TypeBufferedWithBufferSizeInputStream();
				//System.out.println("Reading");
				bufferInput[i].open("C://Data/BufferStreamWithBufferSizeFile "+ i +".txt");
				while(!bufferInput[i].end_of_stream())
					bufferInput[i].read_next();
			}
		}
		
		for(int i=0;i<k;i++)
		{
			bufferInput[i].close();
		}
	}
	
	//File channel implementation
	//By default -
	//range of integers : 1 - 99
	//number of integers : 1024
	//tested the read on 1gb file by switching N = 1 and k = 1
	public void TestMethodFour(int N, int k) throws IOException
	{
		int minValue = 1;
		int maxValue = 99;
		int noOfRecords = 1024;
		TypeFileChannelOutputStream[] outputChannel = new TypeFileChannelOutputStream[k];
		TypeFileChannelInputStream[] inputChannel = new TypeFileChannelInputStream[k];
		
		
		 for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				outputChannel[i] = new TypeFileChannelOutputStream();
				//outputChannel[i].open("C://Data//1gbfile.txt");
				outputChannel[i].create("C://Data//ChannelFile "+ i +".txt");
				outputChannel[i].write(minValue, maxValue, noOfRecords);

			}
		}
		
		for(int i = 0 ;i < k ; i++)
		{
			outputChannel[i].close();
		}
		
		//System.out.println("Reading");
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				inputChannel[i] = new TypeFileChannelInputStream();
				//inputChannel[i].open("C://Data//1gbfile.txt");
				inputChannel[i].open("C://Data//ChannelFile "+ i +".txt");
				while(!inputChannel[i].end_of_stream())
					inputChannel[i].read_next();
				
			}
		}
		
		for(int i = 0 ;i < k ; i++)
		{
			inputChannel[i].close();
		}
	}
	
	//Another implementation of method 4 in a slightly different manner
	//write and read single stream at one go
	public void TestMethodFourVOne(int N, int k) throws IOException
	{
		int minValue = 1;
		int maxValue = 99;
		int noOfRecords = 1024;
		TypeFileChannel[] fcTest = new TypeFileChannel[k];
		
		for(int j=0;j<N;j++)		
		{
			for(int i=0;i<k;i++)
			{
				fcTest[i] = new TypeFileChannel();
				fcTest[i].create("C://Data//ChannelFile "+ i +".txt");
				fcTest[i].write(minValue, maxValue, noOfRecords);
				
				System.out.println("Reading-");
				fcTest[i].open("C://Data//ChannelFile "+ i +".txt");
				while(!fcTest[i].end_of_stream())
					System.out.println(fcTest[i].read_next());
			}
		}
		
		for(int i=0;i<k;i++)
		{
			fcTest[i].closeAfterRead();
			fcTest[i].closeAfterWrite();
		}
	}
	
	public int[] CreateRandomIntValuesAndSort(int min,int max, int noOfRecords)
	{
		int [] valuesToReturn = new int[noOfRecords];
		Random rand= new Random();
		for(int i = 0 ; i < noOfRecords ; i++)
		{
			int randomNum = rand.nextInt((max - min)+1) + min;
			valuesToReturn[i]=randomNum;
		}
		Arrays.sort(valuesToReturn);
		return valuesToReturn;
	}
	
	public String[] CreateSortedFiles(int n) throws IOException
	{
		String[] filePaths = new String[n];
		int minVal = 1;
		int maxVal = 99;
		int numberOfRecords = 1024;
		MergeSort ms = new MergeSort();
		
		for(int i = 0; i < n ; i++)
		{
			SortedFile file = new SortedFile();
			file.create("C://Data//SortedFile "+i+".txt");
			int[] intList = ms.CreateRandomIntValuesAndSort(minVal,maxVal,numberOfRecords);
			for(int j = 0;j < intList.length;j++ )
				file.write(intList[j]);
			file.closeAfterWrite();
			filePaths[i] = "C://Data//SortedFile "+i+".txt";
		}
		
		return filePaths;
	}
	
	public void ReadSortedFiles(String[] filePaths) throws IOException
	{
		for(int i = 0;i < filePaths.length ; i++)
		{
			SortedFile file = new SortedFile();
			 file.open(filePaths[i]);
			  System.out.println("File "+i);
			  while(!file.end_of_stream())
				  file.read_next();
			  file.closeAfterRead();
		}
	}
	
	//Create Sorted Files ( 5 sorted files by default)
	//Merge them by MergeSort
	public void TestMultiWayMergeSort() throws IOException
	{
		  int numberOfInputStreams = 5; 
		  MergeSort ms = new MergeSort();
		  String filePaths[] = ms.CreateSortedFiles(numberOfInputStreams);
		  String outputfilePath = "C://Data//MultiWayMergedAndSortedFile.txt";
		  
		 // ms.ReadSortedFiles(filePaths);
		  System.out.println("Merge and Sort:");
		  MultiWayMergeSort sort = new MultiWayMergeSort();
		  sort.loadMergeSortFiles(filePaths,outputfilePath); 
	}
	
	//By Default -
	//Size of file - 2048 integers
	//Memory available - 512 
	//Streams to merge in one pass -3
	public void TestExternalMultiWayMergeSort() throws IOException
	{
		int N = 2048;
		int M = 512;
		int d = 3;
		Random rand= new Random();
		
		TypeBufferedOutputStream fileToTest = new TypeBufferedOutputStream();
		fileToTest.create("C://Data//fileToTest.txt");
		for(int m= 0 ; m < N ; m++)
		{
			int randomNum = rand.nextInt((99 - 1)+1) + 1;
			fileToTest.write(randomNum);
			//System.out.println(randomNum);
		}
		fileToTest.close();
		
		ExternalMultiwayMergeSort checkSort = new ExternalMultiwayMergeSort();
		
		checkSort.SplitAndSort("C://Data//100mbfile.txt", N, M , d);
		
	}
	
	
	//By default -
	//We have kept N = 1 and k = 1
	//We are testing Stream 1, can test the subsequent methods by uncommenting the method call
	public static void main(String[] args) throws IOException
	{
		int noOfStreams = 1;
		int noOfTimes = 1;
		
		
		MergeSort ms = new MergeSort();
		
		long startTime = ms.StopWatchStart();
		
		
		ms.TestMethodOne(noOfTimes,noOfStreams);
		ms.TestMethodTwo(noOfTimes,noOfStreams);
		ms.TestMethodThree(noOfTimes,noOfStreams);
		ms.TestMethodFour(noOfTimes,noOfStreams);
		ms.TestMethodFourVOne(noOfTimes, noOfStreams);
		
		
		ms.TestMultiWayMergeSort();
		
		//ms.TestExternalMultiWayMergeSort();
		
		long endTime = ms.StopWatchStop();
		System.out.println("Time taken " +ms.TimeTaken(startTime, endTime) + "ms" );
		
	}

}
