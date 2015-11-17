import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.Map.Entry;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.regex.Pattern;

/**
 * WordCount program using a Producer-Consumers pattern.
 * 
 * @author Nicolas Di Tullio
 *
 */
public class MultithreadedWordCount {

	/** Create thread-safe blocking queue which will store up to 1k lines. */
	private static BlockingQueue<String> sharedQueue = new LinkedBlockingQueue<>(1000);

	/** Array to store consumer threads. */
	private static Thread[] consumers;

	/** Concurrent Map containing <word, count> entries. */
	private static Map<String, Integer> wordCounts = new ConcurrentHashMap<String, Integer>();
	
	/** Finished flag. */
	private static boolean readingFinished = false;
	
	/** Pattern to match all non-ascii letters to be removed. */
	private static Pattern specialCharsRemovePattern = Pattern.compile("[^a-zA-Z]");

	/**
	 * Main function. Initializes Producer and Consumers.
	 * Sorts the result before writing it to the output file.
	 * 
	 * @param args
	 * @throws InterruptedException
	 */
	public static void main(String args[]) throws InterruptedException {

		// Parse arguments
		if (args.length != 3) {
			System.out.println("3 arguments needed : input_file, output_file, nb_threads");
			System.exit(1);
		}
		String inputFile = args[0];
		String outputFile = args[1];
		int nbThreads = Integer.parseInt(args[2]);

		// Start timer
		System.out.printf("Execution starting with %d consumer thread(s) ...\n", nbThreads);
		long executionStartTime = System.currentTimeMillis();
		
		// Create array to store the consumer threads
		consumers = new Thread[nbThreads];

		// Create and start Producer thread
		Thread producer = new Thread(new Producer(inputFile));
		producer.start();

		// Create and start Consumer Threads
		for (int i = 0; i < nbThreads; i++) {
			consumers[i] = new Thread(new Consumer());
			consumers[i].start();
		}

		// Wait for all threads to finish
		producer.join();
		for (int i = 0; i < nbThreads; i++) {
			consumers[i].join();
		}

		// Print execution time
		System.out.printf("Word Counting took %d ms.\n", System.currentTimeMillis() - executionStartTime);
		System.out.printf("Now ordering results ...\n");
		
		// Create an ordered map of the results
		Map<String, Integer> ordered = new TreeMap<String, Integer>(wordCounts);
		
		// Print results
		try 
		(
			FileWriter fstream = new FileWriter(outputFile);
			BufferedWriter out = new BufferedWriter(fstream);
		) {
			for (Entry<String, Integer> entry : ordered.entrySet()) {
				out.write(String.format("%s %s\n", entry.getKey(), entry.getValue()));
			}
		} catch (IOException e) {
			e.printStackTrace();
		}
		
		// Print execution time
		System.out.printf("Total Execution took %d ms.\n", System.currentTimeMillis() - executionStartTime);
	}

	/**
	 * Producer. Reads the input file and stores each line in the queue.
	 */
	public static class Producer implements Runnable {

		private String inputFile;

		public Producer(String inputFile) {
			this.inputFile = inputFile;
		}

		@Override
		public void run() {
			File input = new File(inputFile);
			int count = 0;
			try (BufferedReader br = new BufferedReader(new FileReader(input));) {
				String line;
				while ((line = br.readLine()) != null) {
					sharedQueue.put(line);
					count++;
					if (count % 1000000 == 0) {
						System.out.printf("%dM lines read from input. Current Queue size : %d\n", count / 1000000, sharedQueue.size());
					}
				}
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			} catch (IOException e) {
				e.printStackTrace();
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			readingFinished = true;
		}

	}

	/**
	 * Consumer. Fetches a line in the queue and splits it to count words.
	 */
	public static class Consumer implements Runnable {

		@Override
		public void run() {
			
			while (!readingFinished || !sharedQueue.isEmpty()) {

				// Get a line from the queue
				String line = sharedQueue.poll();
				if (line == null) continue;
				
				// Tokenize the line and do some word counting
				String[] words = specialCharsRemovePattern.matcher(line)
						.replaceAll(" ").toLowerCase().split("\\s+");
				
				for (String word : words) {
					if (word.length() >= 50) continue;
					int count = wordCounts.containsKey(word) ? wordCounts.get(word) + 1 : 1;
					wordCounts.put(word, count);
				}
			}
		}

	}

}
