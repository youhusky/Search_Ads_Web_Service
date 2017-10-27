package io.ads;

import java.io.IOException;
import java.io.StringReader;
import java.net.InetSocketAddress;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.lucene.analysis.TokenStream;
import org.apache.lucene.analysis.Tokenizer;
import org.apache.lucene.analysis.core.StopFilter;
import org.apache.lucene.analysis.en.KStemFilter;
import org.apache.lucene.analysis.standard.StandardFilter;
import org.apache.lucene.analysis.standard.StandardTokenizer;
import org.apache.lucene.analysis.tokenattributes.CharTermAttribute;

import net.spy.memcached.MemcachedClient;

public class IndexBuilder {
	private int EXP = 72000; //0: never expire
	private String mMemcachedServer;
	private int mMemcachedPortal;
	private String mysql_host;
	private String mysql_db;
	private String mysql_user;
	private String mysql_pass;
	
	public IndexBuilder(String memcachedServer,int memcachedPortal,String mysqlHost,String mysqlDb,String user,String pass)
	{
		mMemcachedServer = memcachedServer;
		mMemcachedPortal = memcachedPortal;
		mysql_host = mysqlHost;
		mysql_db = mysqlDb;	
		mysql_user = user;
		mysql_pass = pass;	
	}
	public Boolean buildInvertIndex(Ad ad)
	{
		try 
		{
			String keyWords = Utility.strJoin(ad.keyWords, ",");
			MemcachedClient cache = new MemcachedClient(new InetSocketAddress(mMemcachedServer, mMemcachedPortal));	
			List<String> tokens = Utility.cleanedTokenize(keyWords);
			for(int i = 0; i < tokens.size();i++)
			{
				String key = tokens.get(i);
				if(cache.get(key) instanceof Set)
				{
					@SuppressWarnings("unchecked")
					Set<Long>  adIdList = (Set<Long>)cache.get(key);
					adIdList.add(ad.adId);
				    cache.set(key, EXP, adIdList);
				}
				else
				{
					Set<Long>  adIdList = new HashSet<Long>();
					adIdList.add(ad.adId);
					cache.set(key, EXP, adIdList);
				}
			}
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}
		return true;
	}
	public Boolean buildForwardIndex(Ad ad)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			mysql.addAdData(ad);
			
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
	public Boolean updateBudget(Campaign camp)
	{
		try 
		{
			MySQLAccess mysql = new MySQLAccess(mysql_host, mysql_db, mysql_user, mysql_pass);
			mysql.addCampaignData(camp);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			return false;
		}		
		return true;
	}
}
