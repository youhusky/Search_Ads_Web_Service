# AmazonWebCrawlerJoshua
![][1]  ![][2]  ![][3]  
## Task
- [x] Finished
  - [x] please use following url to query amazon, replace ABC with query
  - [x] extract price, product detail url, product image url, category from web page
  - [x] convert each product to Ads
  - [x] store Ads to file, each ads in JSON format.
  - [x] support paging
  - [x] log all exception

## File
### Origin
Just Run Main.java  

- src
	- Ad
	- ConvertWordsToAds 
	- Crawler
	- Main
- proxy.txt (Should be ignored proxy list)
- rawQuery3.txt (input file)  


### Finish the Task 5 and 6  
1.When Page is equals to one, crawler's result are: `output_page_1.json`, `output_page_1.log`  
2.When Page is equals to three, crawler's result are: 
`output_page_3.json`, `output_page_3.log`


## Reference
1. BitTiger-Master Progrma Backend Engineer -JiaYan
2. StackOverFlow and Google


  [1]: https://img.shields.io/badge/jackson-2.8.3-blue.svg
  [2]: https://img.shields.io/badge/jsoup-1.10.3-blue.svg
  [3]: https://img.shields.io/badge/lucene-4.0.0-purple.svg
  [4]: https://img.shields.io/badge/Finish-100-green.svg

 

## Implementation Steps

The first step is to write crawler(Java code) to crawl real product information from Amazon website, and then saved raw ads data on disk. Product information includes : raw query, ads title(defined as ads keywords in this project), ads category, ads price, ads thumbnails, ad id(generated continuously).

The second step is to read data information line by line, and generate inverted index with memcached. The purpose of inverted index is to find relevant time in shortest time. Key is cleaned query token(clean query with lucene library first), value is Ads list.

Then generate forward index to relate every ad with it's detailed information(title, url, price etc). At the same time, I save information on disk (MySQL database).

Query understanding : Select cleaned words from query and title, used sparkMLlib(word2vec algorithm) to train a classifier. Which helped find synonyms for each token word. Then save the synonyms list on disk. While input with online query, use N-gram algorithm to generate sub queries for this certain query, and find out all the relevant ads.

Select and generate 8 features from search log and use (Gradient Boosting Decision Tree & Logistic regression) to train pClick model offline. When online, extract feature from input query and use trained model to predict pClick rate.

After generating pClick rate, rank ads according to its relevant score and bid price.

Rank ads and display top K ads to users.