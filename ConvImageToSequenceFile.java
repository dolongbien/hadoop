import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.ByteWritable;
import org.apache.hadoop.io.BytesWritable;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
//import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.zookeeper.common.IOUtils;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;



public class ConvImageToSequenceFile {

 

    public static class ConvImageToSequenceFileMapper extends
    Mapper<Object, Text,Text,BytesWritable> {
        
        @Override
        public void map(Object key, Text value, Context context)
                throws IOException, InterruptedException {    
            String pathToRead = value.toString();
            
            Configuration conf=context.getConfiguration();
            Path path = new Path(pathToRead);    
            FileSystem fileToRead = FileSystem.get(URI.create(pathToRead), conf);
            
            DataInputStream dis = null;
            try{    
              
                
                dis = fileToRead.open(path);
                
               
                byte tempBuffer[]= new byte[1024 * 1024];
                
                ByteArrayOutputStream bout =new ByteArrayOutputStream();
                
               
                while(dis.read(tempBuffer, 0, tempBuffer.length)>= 0)
                {
                       
                    bout.write(tempBuffer);
                                    
                }
                
                context.write(value,new BytesWritable(bout.toByteArray()));
                
            
                }finally{
                    
                    dis.close();
                    IOUtils.closeStream(dis);                    
              
                }    
            }
        }
    
    public static void main(String[] args) throws Exception {
      
            Configuration conf = new Configuration();
            Job job = Job.getInstance(conf, "ConvImageToSequenceFile");
            job.setJarByClass(ConvImageToSequenceFile.class);
            
            job.setMapperClass(ConvImageToSequenceFileMapper.class);
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(BytesWritable.class);
            
            job.setInputFormatClass(TextInputFormat.class);
            job.setOutputFormatClass(SequenceFileOutputFormat.class);
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
           
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    System.exit(job.waitForCompletion(true)?0:1);        
       
        }
}

