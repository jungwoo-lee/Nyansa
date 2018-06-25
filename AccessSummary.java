import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TimeZone;
import java.util.TreeMap;

/*
Time Complexity = O(N)
*/
public class AccessSummary {

  private Map<Long, Map<String,Integer>> dateToURLHitMap;
  private int batchSize = 100;
	public AccessSummary()
	{
		dateToURLHitMap = new TreeMap<Long, Map<String,Integer>>();
	}

	public AccessSummary(int size)
	{
		  batchSize = size;
			dateToURLHitMap = new TreeMap<Long, Map<String,Integer>>();
	}

  public static long getDateFromEpoch(long epoch)
	{
			epoch = (long)Math.floor(epoch / (24*60*60)); //drop hour/minute/second detailed
			return epoch;
	}

	public static String convertEpochToDate(long epoch)
	{
			epoch = epoch * (24*60*60*1000);
			Date date = new Date(epoch);
			DateFormat dateFormat = new SimpleDateFormat("dd/MM/yyyy");
			dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			String res = dateFormat.format(date);
			return res;
	}

	private void parseAndStore(String[] lines)
	{
			for (String line : lines)
			{
					String[] tokens = line.split("\\|");
					long epoch = Long.parseLong(tokens[0]);
          epoch = getDateFromEpoch(epoch);
					Map<String, Integer> urlToHitMap
            = dateToURLHitMap.get(epoch);
					if(urlToHitMap == null)
					{
						urlToHitMap = new HashMap<String, Integer>();
					}
					Integer hits = urlToHitMap.get(tokens[1]);
					hits = (hits==null)? 1:++hits;
					urlToHitMap.put(tokens[1], hits);
					dateToURLHitMap.put(epoch, urlToHitMap);
			}
	}
  public boolean fetchInputFromFile(String filename)
	{
		File file = new File(filename);
		if(file.exists())
		{
			try
			{
					FileInputStream fstream = new FileInputStream(filename);
					DataInputStream data_input = new DataInputStream(fstream);
					BufferedReader buffer = new BufferedReader(new InputStreamReader(data_input));
          boolean isPatchingDone = false;
					while(!isPatchingDone)
					{
						int linesInBuffer = 0;
						String line;
						List<String> lines = new ArrayList<String>();
						while(linesInBuffer < batchSize && isPatchingDone == false)
						{
							line = buffer.readLine();
							if(line == null)
								isPatchingDone = true;
							else
							{
								line = line.trim();
								if((line.length()!=0))
									lines.add(line);
								linesInBuffer++;
							}
						}
						linesInBuffer = 0;
						parseAndStore(lines.toArray(new String[lines.size()]));
					}
			}
			catch (Exception e)
			{
					System.err.println("Error: " + e.getMessage());
			}
		}
		else
		{
				System.err.println(filename + "was not find in the system.");
				return false;
		}
 		return true;
	}

	public void printURLHitSummary()
	{
			for(Entry<Long, Map<String, Integer>> entry : dateToURLHitMap.entrySet())
			{
				Map<String,Integer> urlToCntMap = entry.getValue();
				Entry<String,Integer>[] hitMapEntries
					= urlToCntMap.entrySet().toArray(new Entry[urlToCntMap.size()]);
				Arrays.sort(hitMapEntries, new Comparator<Entry<String, Integer>>(){
					@Override
					public int compare(Entry<String, Integer> o1, Entry<String, Integer> o2)
					{
						return o2.getValue().compareTo(o1.getValue());
					}
				});

				System.out.println(convertEpochToDate(entry.getKey()) + " GMT");
				for(Entry<String,Integer> hitMapEntry : hitMapEntries)
				{
					System.out.println(hitMapEntry.getKey()+" "+hitMapEntry.getValue());
				}
		}
	}

  public static void main(String[] args)
  {
    if(args.length == 1)
    {
        AccessSummary summary = new AccessSummary();
        if(summary.fetchInputFromFile(args[0]))
        {
          summary.printURLHitSummary();
        }
    }
    else if(args.length ==2)
    {
        int batchSize = 0;
        try
	      {
          batchSize = Integer.parseInt(args[1]);
          if(batchSize < 1)
              throw new NumberFormatException();
        }
        catch(NumberFormatException e) {
           System.out.println("The batch size needs to a positive integer");
           return;
        }
        AccessSummary summary = new AccessSummary(batchSize);
        if(summary.fetchInputFromFile(args[0]))
        {
          summary.printURLHitSummary();
        }
    }
    else
    {
        System.out.println("need a input file path and/or batch size as arguments.");
    }

  }
}
