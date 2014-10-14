import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.PriorityQueue;


public class MultiWayMergeSort {
	
	 

	 public SortedFile loadMergeSortFiles(String[] filePaths, String outputFilePath) throws IOException
	 {
		 SortedFile[] filesToBeMerged = new SortedFile[filePaths.length];
		 
		 //open the streams
		 for(int i = 0 ; i < filesToBeMerged.length; i++ )
		 {
			 filesToBeMerged[i] = new SortedFile();
			 filesToBeMerged[i].open(filePaths[i]);	
		 }
		 
		 return mergeKList(filesToBeMerged, outputFilePath);
		 
	 }
	

	 public SortedFile mergeKList(SortedFile[] input, String outputFilePath) throws IOException 
	 {
		  PriorityQueue<Node> queue = new PriorityQueue<Node>(input.length);
		  List<Integer> output = new ArrayList<Integer>();
		  List<SortedFile> refFiles = new ArrayList<>();
		  
		  Node token;
		  
		  //create list of streams
		  for(int i = 0; i < input.length; i++)
		  {
			  refFiles.add(input[i]);
		  }
		  
		  //get the first integer from each stream, create node and add to the queue
		  //adding to the queue will sort these records
		  for(int i = 0; i < input.length; i++)
		  {
			  if(refFiles.get(i).end_of_stream() == false)
			  {
				  int nodeValue = refFiles.get(i).read_next();
				  token = new Node(nodeValue, i);
				  queue.add(token);
			  }
		  }
		  
		  //poll will remove the head which will be the least value
		  //we store the id of this stream
		  //we add the next value to the queue from the above stream 
		  //continue till queue is empty
		  while(queue.isEmpty()==false)
		  {
			  Node newNode = queue.poll();
			  output.add(newNode.getData());
			  
			  int id = newNode.getIndexList();
			  
			  if(refFiles.get(id).end_of_stream() == false)
			  {
				  int nodeVal = refFiles.get(id).read_next();
				  token = new Node(nodeVal,id);
				  queue.add(token);
			  }
		  }
		  
		  return createSortedOutputFile(output, outputFilePath);
	 }
	 
	 public static SortedFile createSortedOutputFile(List<Integer> input,  String outputFilePath) throws IOException
	 {
		 SortedFile outputFile = new SortedFile();
		 outputFile.create(outputFilePath);
		 
		 for (Integer value : input) 
		 {
			outputFile.write(value);
		 }
		 
		 outputFile.closeAfterWrite();
		 
		 return outputFile;
	 }
	 
	 

	
	
}
