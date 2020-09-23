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

import com.facebook.LinkBench.measurements.exporter.MeasurementsExporter;


/**
 * Take measurements and maintain a histogram of a given metric, such as READ LATENCY.
 * 
 * @author cooperb
 *
 */
public class OneMeasurementPlot extends OneMeasurement
{
	HashMap<Long,OneMeasurementHistogram> histograms;

  static long max_x = 100;

	public OneMeasurementPlot(String name, Properties props) {
		super(name);
    histograms=new HashMap<Long,OneMeasurementHistogram>();
    for (long i = 0; i < max_x; i++) {
      histograms.put(i, new OneMeasurementHistogram(name + "x=" + String.valueOf(i), props));
    }
	}

	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#reportReturnCode(int)
	 */
	public synchronized void reportReturnCode(int code) {
		System.out.println("unimplemented");
	}

	@Override
	public synchronized void measure(long x, long y) {
    if (x > max_x) {
      System.err.println("x exceed limit: " + x);
    }
    histograms.get(x).measure(y);
	}

	/* (non-Javadoc)
	 * @see com.yahoo.ycsb.OneMeasurement#measure(int)
	 */
	public synchronized void measure(long latency) {
		System.out.println("illegal use of combined measurement: no measure on single latency");
	}


  @Override
  public void exportMeasurements(MeasurementsExporter exporter) throws IOException
  {
    for (Long k: histograms.keySet()) {
      OneMeasurementHistogram m = histograms.get(k);
      m.exportMeasurements(exporter);
    }
    // exporter.write(getName(), "Operations", operations);
    // exporter.write(getName(), "AverageLatency(us)", (((double)totallatency)/((double)operations)));
    // exporter.write(getName(), "MinLatency(us)", min);
    // exporter.write(getName(), "MaxLatency(us)", max);
    
    // int opcounter=0;
    // boolean done95th=false;
    // for (int i=0; i<_buckets; i++)
    // {
    //   opcounter+=histogram[i];
    //   if ( (!done95th) && (((double)opcounter)/((double)operations)>=0.95) )
    //   {
    //     exporter.write(getName(), "95thPercentileLatency(ms)", i);
    //     done95th=true;
    //   }
    //   if (((double)opcounter)/((double)operations)>=0.99)
    //   {
    //     exporter.write(getName(), "99thPercentileLatency(ms)", i);
    //     break;
    //   }
    // }

    // for (Integer I : returncodes.keySet())
    // {
    //   int[] val=returncodes.get(I);
    //   exporter.write(getName(), "Return="+I, val[0]);
    // }     

    // for (int i=0; i<_buckets; i++)
    // {
    //   exporter.write(getName(), Integer.toString(i), histogram[i]);
    // }
    // exporter.write(getName(), ">"+_buckets, histogramoverflow);
  }

	@Override
	public String getSummary() {
		// if (windowoperations==0)
		// {
		// 	return "";
		// }
		// DecimalFormat d = new DecimalFormat("#.##");
		// double report=((double)windowtotallatency)/((double)windowoperations);
		// windowtotallatency=0;
		// windowoperations=0;
    // return "["+getName()+" AverageLatency(us)="+d.format(report)+"]";
    return "";
	}

}
