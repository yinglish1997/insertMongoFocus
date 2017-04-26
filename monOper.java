

import java.io.File;
import java.io.IOException;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.List;

import EmotionAnalysis.myAnalysis.FouceAnalysis;

import com.hankcs.hanlp.corpus.document.Document;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.Mongo;
import com.mongodb.MongoException;

public class monOper {

	/**
	 * 把一部电影的关注点存入mongodb
	 * 需要调用FindFocus类，构造该类时放入电影文档地址
	 * @param args
	 */
	String[] fouces;//设置关注点序列，注意，一定要保证与FouceAnalysis类一样的顺序
	 //要先把原文件.txt通过FouceAnalysis的分析
	FindFouce resultList;
	String name;
	
	public monOper(String path) throws IOException{
		this.name = getName(path);
		//this.name = "XingQiuDaZhan";
		System.out.println("name : " + name);
		this.fouces = new String[]{"制片", "投资出品", "导演", "编剧", "市场商业", "制作","预告", "主题", "思想" ,
				"故事题材", "电影类型", "文化元素", "情节内容", "开头", "结局", "发展", "台词", "剧情","视听效果", 
				"画面", "音乐", "动作", "特效", "造型设计", "演员角色", "正派", "男主", "女主", "反派", "配角", "配音"};
		//this.fouces = new String[]{ "制作", "出品公司", "选景", "导演","编剧","主题", "风格", "题材内容" ,
				//"剧情", "开头", "发展", "结局", "笑点", "泪点", "视听", "动作", "画面","镜头", "音乐", "角色", "男主角", "女主角", "反派", "配角"};
		FindFouce entry = new FindFouce(path);
		this.resultList = new FindFouce(path);
		//this.numList = new FouceAnalysis(path);
		double[] numList = this.resultList.result;

		FullMongo(this.name, numList);
		
	} 

	private String getName(String path) {
		//通过截取路径得到电影名字
		String[] array = path.split("/");	
		return array[array.length - 1].replace(".txt", "");
	}

	public  void FullMongo(String name, double[] numArray) throws UnknownHostException, MongoException{
		//输入高中低三条关注点列表，插入数据库
		System.out.println("FullMongo");
//		for(int i = 0; i < this.fouces.length; i ++)
//			System.out.println(this.fouces[i] + numArray[i]);
		/*
		 * 创建数据库的时候先为数据库设置了相应的用户名和密码
		 * db.addUser('root', 'iiip');
		 * db.auth('root', 'iiip')
		 * */
			Mongo connection = new Mongo("192.168.235.20", 27017);
			DB db = connection.getDB("Focus");
			boolean ok = db.authenticate("root", "iiip".toCharArray());
			if(ok){
				System.out.println("db connection success ");
				DBCollection collection = db.getCollection("totalFocus");
				System.out.println(name  + "   collection has been created");
				//如果numArray的长度为三，说明是有高中低三类分析数据。否则长度为一，是总体的分析。

					BasicDBObject docum = new BasicDBObject();
					docum.put("_id", name);
					for(int i = 0; i < numArray.length; i ++){
						docum.append(fouces[i], numArray[i]);
					}
					collection.insert(docum);
					System.out.println(name + "   doucment insert success");
				}	else{
					System.out.println("false");
				}

	}

	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		monOper test = new monOper("/home/yingying/sqlIdFile/chineseName/星球大战外传：侠盗一号.txt");

	}

}
