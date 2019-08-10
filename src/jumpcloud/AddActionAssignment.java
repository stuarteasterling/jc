package jumpcloud;

import java.util.Map;
import java.util.HashMap;
import java.math.BigInteger;
import org.json.JSONObject;
import org.json.JSONArray;

/**
 * Maintains and reports the average time for a set of actions identified by name.
 * 
 * All operations are thread-safe.
 * 
 * Methods take a Design by Contract (DBC) approach: preconditions (i.e. for parameters passed) are clearly stated; 
 * if they are not met by the client code then the operation fails and state of object is undefined.
 */
public class AddActionAssignment
{
	/**
	 * The JSON field name for action name; used for input and output JSON
	 */
	public static final String ACTION_NAME_JSON_FLD = "action";
	
	/**
	 * The JSON field name for the time for a particular action; used for input JSON
	 */
	public static final String TIME_JSON_FLD = "time";
	
	/**
	 * The JSON field name for average time for a particular action; used for output JSON
	 */
	public static final String AVERAGE_TIME_JSON_FLD = "avg";
	
	
	/**
	 * Maps an action name to the data for calculating an average time for that action
	 */
	private Map<String, AverageCalcData> actionsAverageTimeData;
	
	
	/**
	 * Sole contructor
	 */
	public AddActionAssignment ()
	{
		this.actionsAverageTimeData = new HashMap<String, AverageCalcData>();
	}
	
	/**
	 * 
	 * @param jsonStr : a non-null String in valid JSON format 
	 * 					containing two non-empty fields: ACTION_NAME_JSON_FLD and TIME_JSON_FLD
	 */
	public void addAction ( String jsonStr )
	{
		this.addAction( new JSONObject( jsonStr) );
	}
	
	/**
	 * 
	 * @param jsonObj : a non-null JSONObject containing two non-null fields: ACTION_NAME_JSON_FLD and TIME_JSON_FLD
	 */
	public void addAction ( JSONObject jsonObj )
	{
		this.addAction( jsonObj.getString( ACTION_NAME_JSON_FLD ),
						jsonObj.getInt( TIME_JSON_FLD ) );
	}
	
	/**
	 * @param actionName : a non-null name for the action
	 * @param time : the amount of time the action took
	 */
	public void addAction ( String actionName, int time )
	{
		AverageCalcData data = null;
		
		synchronized ( this.actionsAverageTimeData )
		{
			data = this.actionsAverageTimeData.get( actionName );
			
			if ( data == null )
			{
				data = new AverageCalcData();
				this.actionsAverageTimeData.put( actionName, data );
			}
		}
		
		data.addToTotal( time );  // AverageCalcData handles its own synchronization
		
	}
	
	/**
	 * Returns the average time for all actions stored with this object in a JSON array string format
	 * 
	 * @return a JSON array string containing a JSON object for each action; each JSON object has two fields,
	 * 			ACTION_NAME_JSON_FLD and AVERAGE_TIME_JSON_FLD
	 */
	public String getStats ()
	{
		return this.getStatsAsJSONArray().toString();
	}
	
	/**
	 * Returns the average time for all actions stored with this object as a JSONArray object
	 * 
	 * @return a JSONArray containing a JSONObject for each action; each JSON object has two fields,
	 * 			ACTION_NAME_JSON_FLD and AVERAGE_TIME_JSON_FLD
	 */
	public JSONArray getStatsAsJSONArray ()
	{
		JSONArray actionsAverages = new JSONArray();
		
		Map<String, Integer> actionAveragesMap = this.getStatsAsMap();
		
		for ( String actionName : actionAveragesMap.keySet() )
		{
			JSONObject actionAvgJsonObj = new JSONObject();
			
			actionAvgJsonObj.put(AVERAGE_TIME_JSON_FLD, actionAveragesMap.get( actionName ) );
			actionAvgJsonObj.put( ACTION_NAME_JSON_FLD, actionName );
			
			actionsAverages.put( actionAvgJsonObj );
		}
		
		return actionsAverages;
	}
	
	/**
	 *  Returns the average time for all actions stored with this object as a Map
	 * 
	 * @return a Map containing the action name as keys and the average time as values
	 */
	public Map<String, Integer> getStatsAsMap ()
	{
		Map<String, Integer> averagesMap = new HashMap<String, Integer>();
		
		synchronized ( this.actionsAverageTimeData )     // object could also be cloned while under lock
		{												 // and then iterated over, which could be marginally more performant	
														 // albeit using more resources
			
			for ( String actionName : this.actionsAverageTimeData.keySet() )
			{
				AverageCalcData data = this.actionsAverageTimeData.get( actionName );
				
				averagesMap.put( actionName, data.getAverage() );  // AverageCalcData handles its own synchronization
				
			}
		}
		
		return averagesMap;
	}
	
	
	/**
	 * Main method for demo'ing/testing the above
	 * 
	 */
	public static void main ( String[] args ) throws Exception
	{
		System.out.println( "* * * * * * * * * * * * * * * * * * * * * * * * ");
		
		AddActionAssignment test = new AddActionAssignment();
		
		// 1. basic test
		test.addAction("{'action':'foo', 'time':10}");
		test.addAction("{'action':'foo', 'time':20}");
		
		System.out.println( "Test 1" );
		System.out.println( "expected output: [{'avg':15, 'action':'foo'}]");
		System.out.println( "actual output: " + test.getStats() );
		
		System.out.println();
		
		// 2. larger test with two actions
		test = new AddActionAssignment();
		BigInteger foototal = BigInteger.valueOf( 0 );
		BigInteger bartotal = BigInteger.valueOf( 0 );
		int N = 1000;
		for ( int i = 0; i < N; ++i )
		{
			int time = (int) (Math.random() * Integer.MAX_VALUE);
			
			String actionName = null;
			if ( i % 2 == 0 )
			{
				actionName = "foo";
				foototal = foototal.add( BigInteger.valueOf(time) );
			}
			else
			{
				actionName = "bar";
				bartotal = bartotal.add( BigInteger.valueOf(time) );
			}
			
			String json = "{'action':'" + actionName + "', 'time':" + time + "}";
			
			test.addAction( json );
			
		}
		
		int fooaverage = foototal.divide( BigInteger.valueOf( N / 2 ) ).intValue();
		int baraverage = bartotal.divide( BigInteger.valueOf( N / 2 ) ).intValue();
		
		System.out.println( "Test 2" );
		System.out.println( "expected output: [{'avg':" + baraverage + ", 'action':'bar'}, {'avg':" + fooaverage + ", 'action':'foo'}]");
		System.out.println( "actual output: " + test.getStats() );

		System.out.println();
		
		// 3. multithreaded test with two actions
		final AddActionAssignment mttest = new AddActionAssignment();
		N = 10000;
		foototal = BigInteger.valueOf( 0 );
		bartotal = BigInteger.valueOf( 0 );
		int numThreads = 100;
		java.util.concurrent.ScheduledThreadPoolExecutor executor = 
				new java.util.concurrent.ScheduledThreadPoolExecutor( numThreads ); 
		for ( int i = 0; i < N; ++i )
		{
			int time = (int) (Math.random() * Integer.MAX_VALUE);
			
			String action = null;
			if ( i % 2 == 0 )
			{
				action = "foo";
				foototal = foototal.add( BigInteger.valueOf(time) );
			}
			else
			{
				action = "bar";
				bartotal = bartotal.add( BigInteger.valueOf(time) );
			}
			
			String json = "{'action':'" + action + "', 'time':" + time + "}";
			executor.execute( 
								new Runnable () {
									public void run() { mttest.addAction( json ); }
								}
					 ); 
		}
		
		executor.shutdown();
		executor.awaitTermination( 10, java.util.concurrent.TimeUnit.SECONDS);
		
		fooaverage = foototal.divide( BigInteger.valueOf( N / 2 ) ).intValue();
		baraverage = bartotal.divide( BigInteger.valueOf( N / 2 ) ).intValue();
		
		System.out.println( "Test 3" );
		System.out.println( "expected output: [{'avg':" + baraverage + ", 'action':'bar'}, {'avg':" + fooaverage + ", 'action':'foo'}]");
		System.out.println( "actual output: " + mttest.getStats() );

		System.out.println();
	}
	
}

/**
 * Used internally by AddActionAssignment, containing the running total time and the current number of actions (the divisor) 
 * for calculating the average; instances are associated with a given action name in a Map.
 * 
 * Methods are synchronized (equivalent to synchronized(this)) to avoid incorrect value being returned from getAverage().
 * 
 * A weighted moving average could alternately be used (rather than maintaining the total), but the results will diverge.
 * 
 */
class AverageCalcData
{
	private BigInteger total = BigInteger.valueOf( 0 );
	private int count;
	
	/** 
	 * @param number : the amount to add to the total
	 */
	public synchronized void addToTotal ( int amount )
	{
		++this.count;
		this.total = this.total.add( BigInteger.valueOf( amount ) );  // operations on BigInteger do not modify the object itself
	}
	
	/**
	 * @return the current calculated average
	 */
	public synchronized int getAverage ()
	{
		return this.total.divide( BigInteger.valueOf( this.count ) ).intValue();
	}
}

