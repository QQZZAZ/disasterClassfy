package com.sinosoft.utils;

import com.sinosoft.utils.EnumUtil;
import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.NameValuePair;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.HttpClient;
import org.apache.http.client.entity.UrlEncodedFormEntity;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.message.BasicNameValuePair;
import org.apache.http.util.EntityUtils;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by guo on 2017/7/16.
 */
public class GetPlaceWeb {
    public static final String ADD_URL = "http://10.10.20.1:7171/schedule/search";
    private static final HttpClient client = HttpClientBuilder.create().build();
    public static List<String> queryWebAll(String source) {
        List<String> resList = new ArrayList<String>();
        HttpPost post = new HttpPost(ADD_URL);
        List<NameValuePair> list = new ArrayList<NameValuePair>();
        String sourceData = "{\"serviceProviders\": \""+source+"\"}";
        list.add(new BasicNameValuePair("data",sourceData));
        list.add(new BasicNameValuePair("code","1004"));
            list.add(new BasicNameValuePair("data_type","shudi,inner,outer"));//shudi,inner,outer
        list.add(new BasicNameValuePair("pageIndex","1"));
        list.add(new BasicNameValuePair("pageSize","1000"));
        try {
            post.setEntity(new UrlEncodedFormEntity(list,"UTF-8"));
            HttpResponse response = client.execute(post);
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity,"UTF-8");
            JSONObject jo = new JSONObject(result);
            JSONObject json = jo.isNull("subData") ? new JSONObject() : jo.getJSONObject("subData");
            String totlePage = json.isNull("totlePage") ? "0" : json.getString("totlePage");
            int num = Integer.parseInt(totlePage);
            for(int i = 1; i <= num; i++){
                post = new HttpPost(ADD_URL);
                list = new ArrayList<NameValuePair>();
                list.add(new BasicNameValuePair("data",sourceData));
                list.add(new BasicNameValuePair("code","1004"));
                list.add(new BasicNameValuePair("data_type","shudi,inner,outer"));
                list.add(new BasicNameValuePair("pageIndex",i+""));
                list.add(new BasicNameValuePair("pageSize","1000"));
                post.setEntity(new UrlEncodedFormEntity(list,"UTF-8"));
                response = client.execute(post);
                entity = response.getEntity();
                result = EntityUtils.toString(entity,"UTF-8");
                jo = new JSONObject(result);
                String status = jo.isNull("status") ? "" : jo.getString("status");
                if(status.equals("0")){
                    JSONArray data = jo.getJSONArray("data");
                    for (int j = 0; j < data.length(); j++){
                        JSONObject jsonObject = data.getJSONObject(j);
                        String website_url = jsonObject.isNull("urls") ? "" : jsonObject.getString("urls").replace("[","").replace("]","").replace("\'","");
                        resList.add(website_url);
                    }
                }else{
                    System.out.println("接口响应失败： "+jo.getString("error"));
                }
            }
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return resList;
    }
    public static List<String> queryWebHistoryUrl(String source) {
        List<String> resList = new ArrayList<String>();
        HttpPost post = new HttpPost(ADD_URL);
        List<NameValuePair> list = new ArrayList<NameValuePair>();
        String sourceData = "{\"info_status\":\"4\",\"serviceProviders\": \""+source+"\"}";
        list.add(new BasicNameValuePair("data",sourceData));
        list.add(new BasicNameValuePair("code","1004"));
        list.add(new BasicNameValuePair("data_type","shudi"));//shudi,inner,outer
        list.add(new BasicNameValuePair("pageIndex","1"));
        list.add(new BasicNameValuePair("pageSize","1000"));
        try {
            post.setEntity(new UrlEncodedFormEntity(list,"UTF-8"));
            HttpResponse response = client.execute(post);
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity,"UTF-8");
            JSONObject jo = new JSONObject(result);
            JSONObject json = jo.isNull("subData") ? new JSONObject() : jo.getJSONObject("subData");
            String totlePage = json.isNull("totlePage") ? "0" : json.getString("totlePage");
            int num = Integer.parseInt(totlePage);
            for(int i = 1; i <= num; i++){
                post = new HttpPost(ADD_URL);
                list = new ArrayList<NameValuePair>();
                list.add(new BasicNameValuePair("data",sourceData));
                list.add(new BasicNameValuePair("code","1004"));
                list.add(new BasicNameValuePair("data_type","shudi,inner,outer"));
                list.add(new BasicNameValuePair("pageIndex",i+""));
                list.add(new BasicNameValuePair("pageSize","1000"));
                post.setEntity(new UrlEncodedFormEntity(list,"UTF-8"));
                response = client.execute(post);
                entity = response.getEntity();
                result = EntityUtils.toString(entity,"UTF-8");
                jo = new JSONObject(result);
                String status = jo.isNull("status") ? "" : jo.getString("status");
                if(status.equals("0")){
                    JSONArray data = jo.getJSONArray("data");
                    for (int j = 0; j < data.length(); j++){
                        JSONObject jsonObject = data.getJSONObject(j);
                        String website_url = jsonObject.isNull("urls") ? "" : jsonObject.getString("urls").replace("[","").replace("]","").replace("\'","");
                        String website_name = jsonObject.isNull("name") ? "" : jsonObject.getString("name");
                        String web_type = jsonObject.isNull("LeicaXing") ? "" : jsonObject.getString("LeicaXing");
                        String web_is_newswebsite = jsonObject.isNull("is_newswebsite") ? "" : jsonObject.getString("is_newswebsite");
                        String web_country = jsonObject.isNull("state") ? "" : jsonObject.getString("state");
                        String web_province = jsonObject.isNull("province") ? "" : jsonObject.getString("province");
                        String web_city = jsonObject.isNull("city") ? "" : jsonObject.getString("city");
                        String web_county = jsonObject.isNull("county") ? "" : jsonObject.getString("county");
                        String historicalurl = jsonObject.isNull("historicalurl") ? "" : jsonObject.getString("historicalurl");
                        String info_status = jsonObject.isNull("info_status") ? "" : jsonObject.getString("info_status");
                        String domain = jsonObject.isNull("url") ? "" : jsonObject.getString("url");
                        String data_type = jsonObject.isNull("data_type") ? "" : jsonObject.getString("data_type");
                        String tmp = website_url+"|||||"+website_name+"|||||"+web_type+"|||||"+web_is_newswebsite+"|||||"+web_country+"|||||"+web_province+"|||||"+web_city+"|||||"+web_county+"|||||"+historicalurl+"|||||"+info_status+"|||||"+domain+"|||||"+data_type;
                        resList.add(tmp);
                    }
                }else{
                    System.out.println("接口响应失败： "+jo.getString("error"));
                }
            }


        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return resList;
    }
    public static void delete() {
        HttpPost post = new HttpPost(ADD_URL);
        List<NameValuePair> list = new ArrayList<NameValuePair>();
        list.add(new BasicNameValuePair("data","{\"info_status\":\"4\"}"));
        list.add(new BasicNameValuePair("code","1006"));
        list.add(new BasicNameValuePair("data_type","shudi,inner,outer"));
        try {
            post.setEntity(new UrlEncodedFormEntity(list,"UTF-8"));
            HttpResponse response = client.execute(post);
            HttpEntity entity = response.getEntity();
            String result = EntityUtils.toString(entity,"UTF-8");
            System.out.println(result);
        } catch (UnsupportedEncodingException e) {
            e.printStackTrace();
        } catch (ClientProtocolException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args)throws Exception {
        List<String> list = queryWebAll("web");
        System.out.println("新闻网站全部信息： "+list.size());
        List<String> listHistory = queryWebHistoryUrl("web");
        System.out.println("新闻网站url修改过的信息： "+listHistory.size());
//        Thread.sleep(10000);
//        System.out.println("------------------------------------------------------");
//        List<String> listwechat = queryWebAll("WeChat");
//        System.out.println("微信公众号全部信息： "+listwechat.size());
//        List<String> listWechatHistory = queryWebHistoryUrl("WeChat");
//        System.out.println("微信公众号url修改过的信息： "+listWechatHistory.size());
//        delete();
    }
}
