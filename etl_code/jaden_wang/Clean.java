import java.io.IOException;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import com.opencsv.CSVReader;
import java.io.StringReader;


public class Clean {

    public static class CleanMapper extends Mapper<Object, Text, Text, Text>{
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
            StringReader stringReader = new StringReader(value.toString());
            CSVReader reader = new CSVReader(stringReader);
            String[] line = reader.readNext();
            String out = "";
            //drop unnneccesary columns, first column is ID
            for (int i=1; i<=line.length-1; i++){
                if( i != 8 && i!= 10) {
		    if (out == ""){
			out = line[i].trim();	
	            }
		    else {
                    	out = out + "," + line[i].trim();
		    }
                }
            }
            context.write(new Text(line[0].trim()), new Text(out));
            stringReader.close();
            reader.close();
        }
    }
             


    public static class CleanReducer extends Reducer<Text,Text,Text,Text> {
        public void reduce(Text key, Iterable<Text> values,Context context) throws IOException, InterruptedException {
                //checks for null
		for (Text value : values) {
			String[] cells = value.toString().split(",");
			boolean containsNull = false;
			for (String cell : cells) {
				if (cell.trim().isEmpty()){
					containsNull = true;
				}
			}
			if (!containsNull) {
				context.write(key, value);
			}
		}
        }
    }

    public static void main(String[] args) throws Exception {
        Configuration conf = new Configuration();
        conf.set("mapreduce.output.textoutputformat.separator", ",");
        Job job = Job.getInstance(conf, "clean data");
        job.setJarByClass(Clean.class);
        job.setMapperClass(CleanMapper.class);
        job.setReducerClass(CleanReducer.class);
        job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(Text.class);
        job.addFileToClassPath(new Path("opencsv.jar"));
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
