import java.io.IOException;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;

import org.apache.hadoop.mapreduce.Job;

import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

public class ImageDuplicateFinder {


    public static class ImageDuplicateFinderMapper  extends
    Mapper<Text,BytesWritable,Text,Text> {
        
        @Override
        public void map(Text key,BytesWritable value, Context context)
                throws IOException, InterruptedException {
            
           
            byte tempImageData[] = value.getBytes();
            String md5Str = null;
            try {
             
                md5Str = convertToStr(tempImageData);
            } catch (NoSuchAlgorithmException e) {
                
                e.printStackTrace();
                context.setStatus("Internal Error Can't find the algorithm for the specified " +
                        "digest algorithm MD5 ");
                
                
                return ;
            }
           
            context.write(new Text(md5Str),key);
            
            
        }
        
        
        public String convertToStr(byte[] passImageData) throws NoSuchAlgorithmException{
            
            
            MessageDigest md = MessageDigest.getInstance("MD5");
            
                  
            md.update(passImageData);
          
            byte[] tempHash =md.digest();
            
            String hexString = new String();
           
                for(int i =0; i<tempHash.length; i++){
                    hexString += Integer.toString( (tempHash[i] & 0xff) + 0x100, 16 ).substring(1);
                }
            return hexString  ;
        }
    }
    
    public static class ImageDuplicateFinderReducer extends
    Reducer<Text,Text,Text,Text> {
    
        public void reduce(Text key, Iterable<Text> values,Context context)throws IOException, 
        InterruptedException{
            Text imagePath = null;
            for(Text tempPath : values)
            {
                imagePath = tempPath;
                return;
            }
            context.write(new Text(imagePath) , key);
        }   
    
    }
    
    public static void main(String[] args) throws Exception  {
        Configuration conf = new Configuration();
        
        String[] programArgs = new GenericOptionsParser(conf, args)
                .getRemainingArgs();
        if (programArgs.length != 2) {
               System.err.println("Usage: ImageDuplicateFinder <in>  <out>");
               System.exit(2);
            }
        
            Job job =Job.getInstance(conf,"ImageDuplicateFinder" );
            
            job.setJarByClass(ImageDuplicateFinder.class);
            job.setMapperClass(ImageDuplicateFinderMapper.class);
            job.setReducerClass(ImageDuplicateFinderReducer.class);
            
            
            job.setOutputKeyClass(Text.class);
            job.setOutputValueClass(Text.class);
            
            
   
            job.setInputFormatClass(SequenceFileInputFormat.class);
            job.setOutputFormatClass(TextOutputFormat.class);
            
            
            FileInputFormat.addInputPath(job, new Path(args[0]));
            FileOutputFormat.setOutputPath(job, new Path(args[1]));
	    
	    System.exit(job.waitForCompletion(true) ? 0 : 1);                    
 
        }

}
