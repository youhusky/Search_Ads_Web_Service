import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.util.List;
import java.util.logging.FileHandler;
import java.util.logging.Logger;

public class Main {

    public static void main(String[] args) throws IOException {

        final int maxPage = 3;

        Logger logger = Logger.getLogger("Logger");
        FileHandler fileHandler = new FileHandler("output_page_3.log");
        logger.addHandler(fileHandler);
        ObjectMapper objectMapper = new ObjectMapper();
        BufferedReader bufferedReader = new BufferedReader(new FileReader("rawQuery3.txt"));
        FileWriter fileWriter = new FileWriter("output_page_3.json");
        BufferedWriter bufferedWriter = new BufferedWriter(fileWriter);
        try {

            String line;
            Crawler crawler = new Crawler();
            crawler.initProxyList("proxy.txt");
            crawler.initProxy();
            crawler.testProxy();

            bufferedWriter.write('[');

            int cnt = 0;
            while ((line = bufferedReader.readLine())!= null){
                if(line.isEmpty()){
                    continue;
                }
                System.out.println("query: " + line);
                String[] fields = line.split(",");
                // init
                String query = fields[0].trim();
                double bidPrice = Double.parseDouble(fields[1].trim());
                int campaignId = Integer.parseInt(fields[2].trim());
                int groupId = Integer.parseInt(fields[3].trim());


                List<Ad> ads = crawler.getAmazonProds(query, bidPrice, campaignId, groupId,maxPage,logger);
                for(Ad ad : ads){
                    String jsonFile = objectMapper.writeValueAsString(ad);
                    if (cnt == 0 && ad == ads.get(0)){
                        bufferedWriter.write(jsonFile);
                    }
                    else {
                        bufferedWriter.write(',');
                        bufferedWriter.write(jsonFile);
                        bufferedWriter.newLine();
                    }
                }
                cnt += 1;
                crawler.cleanup();
            }
            bufferedWriter.write(']');
            bufferedWriter.close();
        }
        catch (JsonGenerationException e){
            logger.warning("JsonGenerationException");
            e.printStackTrace();
        }
        catch (JsonMappingException e){
            logger.warning("JsonMappingException");
            e.printStackTrace();
        }
        catch (FileNotFoundException e) {
            logger.severe("FileNotFoundException");
            e.printStackTrace();
        }
        catch (IOException e) {
            logger.severe("IOException");
            e.printStackTrace();
        }

    }



}
