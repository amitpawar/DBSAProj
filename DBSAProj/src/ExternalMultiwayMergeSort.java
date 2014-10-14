
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;


public class ExternalMultiwayMergeSort {
	
	boolean extraStream = false;

	//M - memory available
	//d - number of streams to merge in one pass
	//N - input file size
	public void SplitAndSort(String filePath, int N,int M,int d) throws IOException
	{
		int numberOfStreams = N/M;
		
		//in case such as N = 1024, M = 200
		//extraStream will be for those 24 records
		if(numberOfStreams*M < N )
		{
			numberOfStreams = numberOfStreams + 1;
			extraStream = true;
		}
			
		SortedFile[] streamsToOperate =  new SortedFile[numberOfStreams];
		SortedFile inputStream = new SortedFile();
		SortedFile outputStream = new SortedFile();
		int[] sizesForStream = new int[numberOfStreams];		//sizesForStream = fileSize
		
		inputStream.open(filePath);  
		
		//setting the size of streams
		for(int i = 0;i < numberOfStreams; i++)
		{
			streamsToOperate[i] = new SortedFile();
			
			if( i < numberOfStreams - 1 && extraStream)
				sizesForStream[i] = N%M;
			else
				sizesForStream[i] = M;
			
			streamsToOperate[i].create("C://Data//dividedStream "+i+".txt");
		}
		
		//create sorted streams
		List<SortedFile> queue = new ArrayList<SortedFile>();
		
		for(int i = 0; i < numberOfStreams; i++)
		{
			int[] valuesToAdd = new int[sizesForStream[i]];
			
			for(int j = 0; j < sizesForStream[i]; j++)
			{
				int value = inputStream.read_next();
				valuesToAdd[j] =  value;
			}
			Arrays.sort(valuesToAdd);
			
			for(int k = 0; k < sizesForStream[i]; k++)
			{
				streamsToOperate[i].write(valuesToAdd[k]);
			}
			streamsToOperate[i].closeAfterWrite();
			streamsToOperate[i].open("C://Data//dividedStream "+i+".txt");
			queue.add(streamsToOperate[i]);
		}
		
		//pass d streams in one pass
		//continue till only one list that final merged list remains
		//numVal takes care when d > queue size
		int counter = 0;
		while(queue.size() != 1)
		{
			counter++;
			int numVal = ( d < queue.size() ? d : queue.size());
			
			SortedFile[] filesToMerge = new SortedFile[numVal];
			
			for(int i = 0;i < numVal; i++)
			{
				filesToMerge[i] = queue.remove(0);
			}
			
			if(queue.size() != 0 )
			{
				MultiWayMergeSort sort = new MultiWayMergeSort();
				outputStream = new SortedFile();
				outputStream = sort.mergeKList(filesToMerge, "C://Data//Merged "+counter+".txt");
				outputStream.closeAfterWrite();
				outputStream = new SortedFile();
				outputStream.open("C://Data//Merged "+counter+".txt");
				queue.add(outputStream);
			}
			else
			{
				MultiWayMergeSort sort = new MultiWayMergeSort();
				outputStream = new SortedFile();
				outputStream = sort.mergeKList(filesToMerge, "C://Data//FinalMerged.txt"); 
				outputStream.closeAfterWrite();
				outputStream = new SortedFile();
				outputStream.open("C://Data//FinalMerged.txt");
				queue.add(outputStream);
			}
			
		}
		
	}
	
}
