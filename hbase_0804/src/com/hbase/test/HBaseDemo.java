package com.hbase.test;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class HBaseDemo {
	Configuration conf;
	
	HBaseAdmin hBaseAdmin;
	
	static final String TN = "PHONE";
	
	String table_name = "t_person";
	
	HTable hTable;

	@Before
	public void begin() throws Exception{
//		System.setProperty("HADOOP_USER_NAME", "root")
//		System.setProperty("hadoop.home.dir", "/Users/zhaopan5250/Downloads/jar-catalog/hadoop-2.5.1");
//		
//		conf = new Configuration();
//		conf.set("hbase.zookeeper.quorum", "bbz.com,node1,node2");
//		hBaseAdmin = new HBaseAdmin(conf);
//		hTable = new HTable(conf, table_name);
	}
	
	@After
	public void end(){
		if (hBaseAdmin != null){
			
			try {
				hBaseAdmin.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
	
	@Test
	public void createTbl() throws Exception{
		if (hBaseAdmin.tableExists(TN)) {
			hBaseAdmin.disableTable(TN);
			hBaseAdmin.deleteTable(TN);
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(TN));
		HColumnDescriptor family = new HColumnDescriptor("tf1");
		family.setMaxVersions(1);
		family.setInMemory(true);
		family.setBlockCacheEnabled(true);
		desc.addFamily(family);
		hBaseAdmin.createTable(desc);
	}
	
	@Test
	public void createPersonTbl() throws Exception{
		if (hBaseAdmin.tableExists(table_name)) {
			hBaseAdmin.disableTable(table_name);
			hBaseAdmin.deleteTable(table_name);
		}
		HTableDescriptor desc = new HTableDescriptor(TableName.valueOf(table_name));
		HColumnDescriptor family = new HColumnDescriptor("cf1");
		family.setMaxVersions(1);
		family.setInMemory(true);
		family.setBlockCacheEnabled(true);
		desc.addFamily(family);
		HColumnDescriptor family2 = new HColumnDescriptor("cf2");
		family2.setMaxVersions(1);
		family2.setInMemory(true);
		family2.setBlockCacheEnabled(true);
		desc.addFamily(family2);
		hBaseAdmin.createTable(desc);
	}
	
	@Test
	public void insertPerson() throws Exception{
		String rowkey = "0001";
		Put put = new Put(Bytes.toBytes(rowkey));
		//put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("xiaoming"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("20"));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("1001"), Bytes.toBytes("1"));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("1002"), Bytes.toBytes("2"));
		hTable.put(put);
		
		rowkey = "0002";
		put = new Put(Bytes.toBytes(rowkey));
		//put.add(Bytes.toBytes("cf1"), Bytes.toBytes("name"), Bytes.toBytes("lilei"));
		put.add(Bytes.toBytes("cf1"), Bytes.toBytes("age"), Bytes.toBytes("30"));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("1001"), Bytes.toBytes("0"));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("1002"), Bytes.toBytes("1"));
		hTable.put(put);
	}
	
	@Test
	public void insertRole() throws Exception{
		String rowkey = "1001";
		Put put = new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("0002"), Bytes.toBytes("lilei"));
		hTable.put(put);
		
		rowkey = "1002";
		put = new Put(Bytes.toBytes(rowkey));
		put.add(Bytes.toBytes("cf2"), Bytes.toBytes("0001"), Bytes.toBytes("xiaoming"));
		hTable.put(put);
	}
	@Test
	public void getRoleOfPerson() throws IOException{
		String rowkey = "0002";//即为id
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addFamily(Bytes.toBytes("cf2"));
		Result rs = hTable.get(get);
		for (Cell cell : rs.listCells()) {
			String columName = new String(CellUtil.cloneQualifier(cell));
			System.out.println(columName + "-"+new String(CellUtil.cloneValue(cell)));
			Get getNew = new Get(Bytes.toBytes(columName));
			 //getNew.addFamily(Bytes.toBytes("cf1"));
			 getNew.addColumn(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			 hTable = new HTable(conf, "t_role");
			 Result rsRole = hTable.get(getNew);
			 Cell roleCell = rsRole.getColumnLatestCell(Bytes.toBytes("cf1"), Bytes.toBytes("name"));
			 System.out.println(columName +"-" + new String(CellUtil.cloneValue(roleCell)));
			 
		}
		
		
	}
	@Test
	public void test() throws Exception{
		byte [] buf = Bytes.toBytes(1);
		System.out.println(buf);
		
		System.out.println(getPhoneNum("186"));
		System.out.println(getDate("2018"));
		System.out.println(getDate2("20180806"));
		System.out.println(getDateMonth("201808"));
	}
	
	static Random r = new Random();
	
	/**
	 * 随机生成手机号码
	 * @param prefix：手机号码前缀eq:186,177
	 * @return
	 */
	private static String getPhoneNum(String prefix){
		return prefix + String.format("%08d", r.nextInt(99999999));
	}
	
	/**
	 * 获取某个月的随机某天
	 * @param month
	 * @return
	 */
	private static int getDayOfMonth(int month,String year){
		if(month == 4 || month == 6 || month == 9 || month ==11 ){
			return r.nextInt(30)+1;
		}else if(month == 2){
			int y = Integer.valueOf(year);
			if(y % 4 == 0 && y % 100 != 0 || y % 400 == 0 ){//瑞年
				return r.nextInt(29)+1;
			}else{
				return r.nextInt(28)+1;
			}
		}else{
			return r.nextInt(31)+1;
		}
	}
	
	/**
	 * 随机生成时间
	 * @param year 年
	 * @return
	 */
	private static String getDate(String year){
		int month = r.nextInt(12)+1;
		return year + String.format("%02d%02d%02d%02d%02d", 
				new Object[]{month,getDayOfMonth(month, year),
				r.nextInt(24),r.nextInt(60),r.nextInt(60)});
	}
	/**
	 * 随机生成时间
	 * @param prefix 年月201808
	 * @return
	 */
	private static String getDateMonth(String prefix) throws Exception{
		if(prefix.length() != 6){
			throw new Exception("日期格式不正确...");
		}
		String year = prefix.substring(0, 4);
		int month = Integer.parseInt(prefix.replaceAll(year, ""));
		return prefix + String.format("%02d%02d%02d%02d", 
				new Object[]{getDayOfMonth(month, year),
				r.nextInt(24),r.nextInt(60),r.nextInt(60)});
	}
	
	/**
	 * 随机生成时间
	 * @param prefix 年月日
	 * @return
	 */
	private static String getDate2(String prefix){
		return prefix + String.format("%02d%02d%02d", 
				new Object[]{r.nextInt(24),r.nextInt(60),r.nextInt(60)});
	}
	
	/**
	 * 插入10个手机号 100条通话记录
	 * 满足查询 时间降序排列
	 * @throws Exception 
	 */
	@Test
	public void insertDB() throws Exception{
		hTable = new HTable(conf, TN);
		List<Put> puts = new ArrayList<Put>();
		for (int i = 0; i < 10; i++) {
			String phoneNum = getPhoneNum("186");
			for (int j = 0; j < 100; j++) {
				String phoneData = getDate("2017");
				SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMddHHmmss");
				try {
					long dataTime = sdf.parse(phoneData).getTime();
					String rowkey = phoneNum + (Long.MAX_VALUE - dataTime);
					System.out.println(rowkey);
					
					Put put = new Put(Bytes.toBytes(rowkey));
					put.add(Bytes.toBytes("tf1"), Bytes.toBytes("type"), Bytes.toBytes(r.nextInt(2)+""));
					put.add(Bytes.toBytes("tf1"), Bytes.toBytes("time"), Bytes.toBytes(phoneData));
					put.add(Bytes.toBytes("tf1"), Bytes.toBytes("pnum"), Bytes.toBytes(getPhoneNum("177")));
					
					puts.add(put);
				} catch (ParseException e) {
					e.printStackTrace();
				}
			}
		}
		hTable.put(puts);
	}
	
	/**
	 * 插入10个手机号 100条通话记录
	 * 满足查询 时间降序排列
	 * @throws Exception 
	 */
	@Test
	public void insertDB2() throws Exception{
		hTable = new HTable(conf, TN);
		for (int i = 0; i < 10; i++) {
			String phoneNum = getPhoneNum("186");
			String rowkey = phoneNum + "_" + (Long.MAX_VALUE - Long.parseLong("210170808"));
			System.out.println(rowkey);
			//一天的通话记录
			Phone.pday.Builder pday = Phone.pday.newBuilder();
			for (int j = 0; j < 100; j++) {
				String phoneDate = getDate2("20170808");
				//一条通话记录
				Phone.pdetail.Builder detail = Phone.pdetail.newBuilder();
				detail.setType(r.nextInt(2)+"");
				detail.setTime(phoneDate);
				detail.setPnum(getPhoneNum("177"));
				pday.addPlist(detail);
			}
			Put put = new Put(Bytes.toBytes(rowkey));
			put.add(Bytes.toBytes("tf1"), Bytes.toBytes("pday"), pday.build().toByteArray());
			hTable.put(put);
		}
	}
	/**
	 * 18685743777一天的通话记录
	 * rowkey:18685743777_9223372036644604999
	 * @throws IOException 
	 */
	@Test
	public void getPhoneDetail() throws IOException{
		hTable = new HTable(conf, TN);
		String rowkey = "18685743777_9223372036644604999";
		Get get = new Get(Bytes.toBytes(rowkey));
		get.addColumn(Bytes.toBytes("tf1"), Bytes.toBytes("pday"));	
		Result rs = hTable.get(get);
		Cell cell = rs.getColumnLatestCell(Bytes.toBytes("tf1"), Bytes.toBytes("pday"));
		Phone.pday pday = Phone.pday.parseFrom(CellUtil.cloneValue(cell));
		//以防排序出现异常，需要重新new个list装起来
		List<Phone.pdetail> list = new ArrayList<>(pday.getPlistList());
		Collections.sort(list);
		for (Phone.pdetail detail : list) {
			System.out.println(detail.getPnum() + "-" + detail.getTime() + "-" + detail.getType());
		}
	}
	
}
