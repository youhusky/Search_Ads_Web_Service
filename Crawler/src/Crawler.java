import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileReader;
import java.io.IOException;
import java.net.Authenticator;
import java.net.PasswordAuthentication;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;


/**
 * Created by Joshua on 6/24/17.
 */
public class Crawler {
    private static final String USER_AGENT = "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_11_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/55.0.2883.95 Safari/537.36";
    private final String authUser = "bittiger";
    private final String authPassword = "cs504";
    private static final String AMAZON_QUERY_URL = "https://www.amazon.com/s/ref=nb_sb_noss?field-keywords=";
    BufferedWriter bufferedWriter;

    private int index = 0;
    private List<String> proxyList;
    void initProxyList(String proxy_file) {
        proxyList = new ArrayList<String>();
        try (BufferedReader br = new BufferedReader(new FileReader(proxy_file))) {
            String line;
            while ((line = br.readLine()) != null) {
                String[] fields = line.split(",");
                String ip = fields[0].trim();
                proxyList.add(ip);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        Authenticator.setDefault(
                new Authenticator() {
                    @Override
                    public PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                authUser, authPassword.toCharArray());
                    }
                }
        );

        System.setProperty("http.proxyUser", authUser);
        System.setProperty("http.proxyPassword", authPassword);
        System.setProperty("socksProxyPort", "61336"); // set proxy port
    }
    private void setProxy() {
        //rotate
        if (index == proxyList.size()) {
            index = 0;
        }
        String proxy = proxyList.get(index);
        System.setProperty("socksProxyHost", proxy); // set proxy server
        index++;
    }
    void initProxy() throws IOException {
        //System.setProperty("socksProxyHost", "199.101.97.161"); // set socks proxy server
        //System.setProperty("socksProxyPort", "61336"); // set socks proxy port


        System.setProperty("http.proxyHost", "199.101.97.169"); // set proxy server
        System.setProperty("http.proxyPort", "60099"); // set proxy port

        Authenticator.setDefault(
                new Authenticator() {
                    @Override
                    public PasswordAuthentication getPasswordAuthentication() {
                        return new PasswordAuthentication(
                                authUser, authPassword.toCharArray());
                    }
                }
        );
    }
    void testProxy() {


        String test_url = "http://www.toolsvoid.com/what-is-my-ip-address";
        try {
            Document doc = Jsoup.connect(test_url).userAgent(USER_AGENT).timeout(10000).get();
            String iP = doc.select("body > section.articles-section > div > div > div > div.col-md-8.display-flex > div > div.table-responsive > table > tbody > tr:nth-child(1) > td:nth-child(2) > strong").first().text(); //get used IP.
            System.out.println("IP-Address: " + iP);
        } catch (IOException e) {
            // TODO Auto-generated catch block
            e.printStackTrace();
        }
    }

    void cleanup(){
        if(bufferedWriter != null){
            try {
                bufferedWriter.close();
            }
            catch (IOException e){
                e.printStackTrace();
            }
        }
    }

    private String getDetailUrl(String asin){
        return "https://www.amazon.com/dp/" + asin;
    }

    private String getBrand(int lastIndex, int i, Element doc, Logger logger, String asin){
        String brand_path = "#result_" + Integer.toString(lastIndex+ i) + " > div > div > div > div.a-fixed-left-grid-col.a-col-right > div.a-row.a-spacing-small > div > span:nth-child(2)";
        Element brand = doc.select(brand_path).first();
        if (brand != null) {
            return brand.text();
        }
        else {
            logger.warning("not found brand "+ asin);

        }
        return "";
    }

    private Double getPrice(Element prodsById, Logger logger, String asin){
        String price = prodsById.getElementsByClass("a-color-base sx-zero-spacing").attr("aria-label");
        if (price != "") {
            price = price.replace("$", " ");
            price = price.replace(",", "");
            if (price.contains("-")) {
                int index = price.indexOf('-');
                price = price.substring(0, index);
            }
            try {
                double temp = Double.parseDouble(price);
                if (temp == 0.0){
                    logger.warning("this product does not have price " + asin);
                }
                return temp;

            } catch (NumberFormatException ne) {
                logger.warning("NumberFormatException");
                ne.printStackTrace();
            }

        }
        else {
            logger.warning("This item's price is not found " + asin);

        }
        return 0.0;
    }

    private String getThumbnail(int lastIndex, int i, Element doc, Logger logger, String asin){
        String thumbnail_path = "#result_" + Integer.toString(lastIndex + i) + " > div > div > div > div.a-fixed-left-grid-col.a-col-left > div > div > a > img";
        Element thumbnail = doc.select(thumbnail_path).first();
        if (thumbnail != null) {
            return thumbnail.attr("src");
        }
        else {
            logger.warning("not found thumbnail " + asin);
            //setBufferedWriter(bufferedWriter,"thumbnail",asin);
        }
        return "";
    }

    private Integer getGlobalPageSize(Elements size){
        if(!size.text().substring(0,5).contains("-")){
            return Integer.parseInt(size.text().substring(0,1));
        }
        else {
            String sizeStr = size.text().substring(size.text().indexOf("-")+1, size.text().indexOf("-")+3);
            return Integer.parseInt(sizeStr);
        }
    }

    List<Ad> getAmazonProds(String query, double bidPrice, int campaignId, int groupId, int maxPage, Logger logger) throws IOException {
        //ObjectMapper objectMapper = new ObjectMapper();
        List<Ad> ads = new ArrayList<>();
        if (false){
            testProxy();
            return ads;
        }
        setProxy();

        int globalPageSize = 0;
        for (int page = 1,lastIndex = 0; page <= maxPage; page++) {
            String originQuery = query;
            query = query.replace(" ", "+");
            String url = AMAZON_QUERY_URL + query + "&page=" + page;

            try {
                HashMap<String, String> headers = new HashMap<>();
                headers.put("Accept","application/xml,application/xhtml+xml,text/html;q=0.9, text/plain;q=0.8,image/png,*/*;q=0.5");
                headers.put("Accept-Encoding", "deflate, gzip;q=1.0, *;q=0.5");
                //headers.put("Accept-Language", "en-US,en;q=0.8");
                Document doc = Jsoup.connect(url).maxBodySize(0).headers(headers).userAgent(USER_AGENT).timeout(10000).get();

                Elements size = doc.getElementsByClass("a-size-base a-spacing-small a-spacing-top-small a-text-normal");

                // page
                if (globalPageSize == 0){
                    try {
                        globalPageSize = getGlobalPageSize(size);
                    }
                    catch (StringIndexOutOfBoundsException e){
                        continue;
                    }
                }

                Elements prods = doc.getElementsByClass("s-result-item celwidget ");
                if(prods.size() == 0){
                    logger.warning("not found this page " + originQuery +" "+ String.valueOf(page));
                    continue;
                }
                //DOM
                // category
                Element category = doc.select("#leftNavContainer > ul:nth-child(2) > div > li:nth-child(1) > span > a > h4").first();
                String categoryStr = category.text();
                if(categoryStr == ""){
                    logger.warning("not found category");
                    continue;
                }   for (Integer i = 0; i < prods.size(); i++) {
                    Ad ad = new Ad();
                    // init
                    Integer temp = lastIndex + i;
                    String id = "result_" + temp.toString();

                    try {
                        Element prodsById = doc.getElementById(id);
                        String asin = prodsById.attr("data-asin");

                        // init campaignId, bidPrice, adId
                        ad.campaignId = campaignId;
                        ad.bidPrice = bidPrice;
                        ad.adId = groupId;
                        ad.keyWords = ConvertWordstoAds.cleanAndTokenizeData(query);
                        ad.category = categoryStr;
                        ad.query = originQuery;
                        ad.title = prodsById.getElementsByAttribute("title").attr("title");
                        if(ad.title == ""){
                            continue;
                        }
                        ad.detail_url = getDetailUrl(asin);
                        ad.brand = getBrand(lastIndex, i, doc, logger, asin);
                        ad.thumbnail = getThumbnail(lastIndex, i, doc, logger, asin);
                        ad.price = getPrice(prodsById, logger, asin);
                        if(ad.price == 0.0){
                            continue;
                        }
                        ads.add(ad);
                    }
                    catch (NullPointerException n) {
                        n.printStackTrace();
                        logger.warning("not found it");
                    }
                }
                lastIndex += Math.min(prods.size(), globalPageSize);

            } catch (IOException e) {
                logger.warning("IOException");
                e.printStackTrace();
            } catch (NullPointerException e) {
                //setBufferedWriter(bufferedWriter,"not found in this","page");
                e.printStackTrace();
                logger.warning("not find this page");
            }
        }
        return ads;
    }
}
