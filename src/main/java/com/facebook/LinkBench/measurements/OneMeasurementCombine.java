/**                                                                                                                                                                                
 * Copyright (c) 2010 Yahoo! Inc. All rights reserved.                                                                                                                             
 *                                                                                                                                                                                 
 * Licensed under the Apache License, Version 2.0 (the "License"); you                                                                                                             
 * may not use this file except in compliance with the License. You                                                                                                                
 * may obtain a copy of the License at                                                                                                                                             
 *                                                                                                                                                                                 
 * http://www.apache.org/licenses/LICENSE-2.0                                                                                                                                      
 *                                                                                                                                                                                 
 * Unless required by applicable law or agreed to in writing, software                                                                                                             
 * distributed under the License is distributed on an "AS IS" BASIS,                                                                                                               
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or                                                                                                                 
 * implied. See the License for the specific language governing                                                                                                                    
 * permissions and limitations under the License. See accompanying                                                                                                                 
 * LICENSE file.                                                                                                                                                                   
 */

package com.facebook.LinkBench.measurements;

import java.io.IOException;
import java.text.DecimalFormat;
import java.util.HashMap;
import java.util.Properties;
import java.util.Vector;

import com.facebook.LinkBench.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementCombine extends OneMeasurement
{
	class SeriesUnit
	{
		/**
		 * @param time
		 * @param average
		 */
		public SeriesUnit(long time, double average, int count) {
			this.time = time;
			this.average = average;
			this.count = count;
		}
		public long time;
		public double average; 
		public int count;
	}
	public static final String BUCKETS="combine.buckets";
	public static final String BUCKETS_DEFAULT="1000";
	public static final String GRANULARITY="combine.granularity";
	
	public static final String GRANULARITY_DEFAULT="1000";

	// histogram records
	int _buckets;
	int[] histogram;
	int histogramoverflow;

	// time series
	int _granularity;
	Vector<SeriesUnit> _measurements;
	
	long start=-1;
	long currentunit=-1;
	int count=0;
	long sum=0;

	// common one
	int operations;
	long totallatency;
	
	//keep a windowed version of these stats for printing status
	int windowoperations;
	long windowtotallatency;
	
	long min;
	long max;
	HashMap<Integer,int[]> returncodes;

	public OneMeasurementCombine(String name, Properties props)
	{
		super(name);
    // _granularity is milisec
		_granularity=Integer.parseInt(props.getProperty(GRANULARITY,GRANULARITY_DEFAULT));
		_measurements=new Vector<SeriesUnit>();

		_buckets=Integer.parseInt(props.getProperty(BUCKETS, BUCKETS_DEFAULT));
		histogram=new int[_buckets];
		histogramoverflow=0;

		operations=0;
		totallatency=0;

		windowoperations=0;
		windowtotallatency=0;
		min=-1;
		max=-1;
		returncodes=new HashMap<Integer,int[]>();
	}

	void checkEndOfUnit(boolean forceend)
	{
		long now=System.currentTimeMillis();
		
		if (start<0)
		{
			currentunit=0;
			start=now;
		}
		
		long unit=((now-start)/_granularity)*_granularity;
		
		if ( (unit>currentunit) || (forceend) )
		{
			double avg=((double)sum)/((double)count);
			_measurements.add(new SeriesUnit(currentunit,avg, count));
			
			currentunit=unit;
			
			count=0;
			sum=0;
		}
	}

	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code)
	{
		Integer Icode=code;
		if (!returncodes.containsKey(Icode))
		{
			int[] val=new int[1];
			val[0]=0;
			returncodes.put(Icode,val);
		}
		returncodes.get(Icode)[0]++;
	}

	@Override
	public synchronized void measure(long x, long y) {
		System.out.println("illegal use of combined measurement: no measure on coordinate");
	}


	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public synchronized void measure(long latency)
	{
		// the time passin is us
		// histogram
		if (latency/1000>=_buckets)
		{
			histogramoverflow++;
		}
		else
		{
			histogram[(int)(latency/1000)]++;
		}
		// time series
		checkEndOfUnit(false);
		
		count++;
		sum+=latency;

		// common ones
		operations++;
		totallatency+=latency;
		windowoperations++;
		windowtotallatency+=latency;

		if ( (min<0) || (latency<min) )
		{
			min=latency;
		}

		if ( (max<0) || (latency>max) )
		{
			max=latency;
		}
	}


  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    checkEndOfUnit(true);
    exporter.write(getName(), "Operations", operations);
    exporter.write(getName(), "AverageLatency(us)", (((double)totallatency)/((double)operations)));
    exporter.write(getName(), "MinLatency(us)", min);
    exporter.write(getName(), "MaxLatency(us)", max);
    
    int opcounter=0;
    boolean done95th=false;
    for (int i=0; i<_buckets; i++)
    {
      opcounter+=histogram[i];
      if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
      {
        exporter.write(getName(), "95thPercentileLatency(ms)", i);
        done95th=true;
      }
      if (((double)opcounter)/((double)operations)>=0.99)
      {
        exporter.write(getName(), "99thPercentileLatency(ms)", i);
        break;
      }
    }

    for (Integer I : returncodes.keySet())
    {
      int[] val=returncodes.get(I);
      exporter.write(getName(), "Return="+I, val[0]);
    }     
		// histogram
    for (int i=0; i<_buckets; i++)
    {
			if (histogram[i] == 0) continue;
      exporter.write(getName() + "CDF", Integer.toString(i), histogram[i]);
    }
		exporter.write(getName() + "CDF", ">"+_buckets, histogramoverflow);
		// time series
    for (SeriesUnit unit : _measurements)
    {
      exporter.write(getName() + "Seq", Long.toString(unit.time), unit.average);
    }
    for (SeriesUnit unit : _measurements)
    {
      exporter.write(getName() + "Count", Long.toString(unit.time), unit.count);
    }
  }

	@Override
	public String getSummary() {
		if (windowoperations==0)
		{
			return "";
		}
		DecimalFormat d = new DecimalFormat("#.##");
		double report=((double)windowtotallatency)/((double)windowoperations);
		windowtotallatency=0;
		windowoperations=0;
		return "["+getName()+" AverageLatency(us)="+d.format(report)+"]";
	}

}
