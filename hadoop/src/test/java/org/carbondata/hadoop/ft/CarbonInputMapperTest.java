package org.carbondata.hadoop.ft;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.carbondata.core.carbon.AbsoluteTableIdentifier;
import org.carbondata.hadoop.CarbonInputFormat;
import org.carbondata.hadoop.test.util.StoreCreator;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

public class CarbonInputMapperTest  {

   public void setUp() throws Exception {
    StoreCreator.createCarbonStore();
  }

   public void testInputFormatMapReduce() throws Exception {

  }

  public static class Map extends Mapper<Void, Object[], Void, Text> {

    private BufferedWriter fileWriter;
    public void setup(Context context) throws IOException, InterruptedException {
      String outPath = context.getConfiguration().get("outpath");
      File outFile = new File(outPath);
      try {
        fileWriter = new BufferedWriter(new FileWriter(outFile));
      } catch (Exception e){
        throw new RuntimeException(e);
      }
    }
    public void map(Void key, Object[] value, Context context) throws IOException {
      StringBuilder builder = new StringBuilder();
      for (int i = 0; i < value.length; i++) {
        builder.append(value[i]).append(",");
      }
      fileWriter.write(builder.toString().substring(0, builder.toString().length()-1));
      fileWriter.newLine();
    }

    @Override public void cleanup(Context context) throws IOException, InterruptedException {
      super.cleanup(context);
      fileWriter.close();
    }
  }

  private void runJob(String outPath) throws Exception {

    Job job = Job.getInstance(new Configuration());
    job.setJarByClass(CarbonInputMapperTest.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);
    job.setMapperClass(Map.class);
//    job.setReducerClass(WordCountReducer.class);
    job.setInputFormatClass(CarbonInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);
    AbsoluteTableIdentifier abs = StoreCreator.getAbsoluteTableIdentifier();
    CarbonInputFormat.setTableToAccess(job.getConfiguration(), abs.getCarbonTableIdentifier());
    FileInputFormat.addInputPath(job, new Path(abs.getStorePath()));
    FileOutputFormat.setOutputPath(job, new Path(outPath));
    job.getConfiguration().set("outpath", outPath);
    boolean status = job.waitForCompletion(true);
    if (status) {
      System.exit(0);
    }
    else {
      System.exit(1);
    }



  }

  public static void main(String[] args) throws Exception {
    new CarbonInputMapperTest().runJob("target/output");
  }
}
