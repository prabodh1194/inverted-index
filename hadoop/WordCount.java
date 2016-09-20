import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.StringTokenizer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.HashSet;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class WordCount
{
    public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>
    {
        private IntWritable idi;
        private Text word = new Text();
        public void map(Object key, Text value, Context context) throws IOException, InterruptedException
        {
            FileInputStream fis = new FileInputStream(new File("stopwords.dat"));
            BufferedReader br = new BufferedReader(new InputStreamReader(fis));
            String token, line = null;
            ArrayList<String> sw = new ArrayList<String>();
            Stemmer st = new Stemmer();

            while((line = br.readLine()) != null)
                sw.add(line);

            br.close();

            int id = -1, flag = 0;
            Set<Integer> set = new HashSet<Integer>();
            Map<String, Set<Integer> > dict = new HashMap<String, Set<Integer> >();
            String a = value.toString();
            StringTokenizer strtok = new StringTokenizer(a," \n");
            Pattern reid = Pattern.compile("id=\"([0-9]+)\"");
            Pattern retitle = Pattern.compile("title=\"(.*)\" non");
            Matcher m;

            while(strtok.hasMoreTokens())
            {
                token = strtok.nextToken();
                if(token.equals("</doc>"))
                {
                    id = -1;
                    flag = 0;
                }

                if(id == -1)
                {
                    m = reid.matcher(token);
                    if(!m.find())                    
                        continue;
                    id = Integer.parseInt(m.group(1));
                    continue;
                }

                if(token.charAt(token.length()-1) == '>' && flag == 0)
                {
                    flag = 1;
                    continue;
                }

                if(flag == 0)
                    continue;

                token = token.replaceAll("[^a-zA-Z0-9 ]"," ");
                token = token.toLowerCase();

                StringTokenizer strtok1 = new StringTokenizer(token," ");
                StringBuilder sb = new StringBuilder();

                while(strtok1.hasMoreTokens())
                {
                    token = strtok1.nextToken();
                    token += " ";
                    if(Collections.binarySearch(sw, token) >= 0)
                        continue;
                    token = st.stem(token);
                    System.out.println(token);
                    idi = new IntWritable(id);
                    word.set(token);
                    context.write(word, idi);
                }
            }
        }
    }

    public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable>
    {
        private IntWritable result = new IntWritable();
        public void reduce(Text key, Iterable<IntWritable> values, Context context ) throws IOException, InterruptedException
        {
            for (IntWritable val : values)
            {
                System.out.println(key);
                System.out.println(val.get());
            }
        }
    }

    public static void main(String[] args) throws Exception
    {
        Configuration conf = new Configuration();
        Job job = Job.getInstance(conf, "word count");
        job.setJarByClass(WordCount.class);
        job.setMapperClass(TokenizerMapper.class);
        job.setCombinerClass(IntSumReducer.class);
        job.setReducerClass(IntSumReducer.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(IntWritable.class);
        FileInputFormat.addInputPath(job, new Path(args[0]));
        FileOutputFormat.setOutputPath(job, new Path(args[1]));
        System.exit(job.waitForCompletion(true) ? 0 : 1);
    }
}
