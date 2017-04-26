import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import EmotionAnalysis.myAnalysis.FouceAnalysis;

import scala.Tuple2;


public class FindFoucs {

	/**
	 * @param args
	 */
	
	JavaSparkContext sc;
	JavaRDD<String> inputRDD;
	double[] result ;
	public FindFoucs(String path) throws IOException{
		SparkConf conf = new SparkConf().setMaster("local").setAppName("FindFouce");
		this.sc = new JavaSparkContext(conf);
		this.inputRDD = sc.textFile(path);
		this.result = totalComAnalysis();
//		HashMap<Integer, String> fouceIndexMap = setHashMap(false);
//		for(int i = 0; i < this.result.length; i ++)
//			System.out.println(fouceIndexMap.get(i) + " : " + result[i]);
		this.sc.close();
	}
	
	public static HashMap setHashMap(Boolean wordIndex){
		//设置哈希树，是关注点与下标的对应			 
		//若wordIndex  = true，创建【关注点：下标】映射
		String[] fouces = new String[]{ "制片", "投资出品", "导演", "编剧", "市场商业", "制作","预告", "主题", "思想" ,
				"故事题材", "电影类型", "文化元素", "情节内容", "开头", "结局", "发展", "台词", "剧情","视听效果", 
				"画面", "音乐", "动作", "特效", "造型设计", "演员角色", "正派", "男主", "女主", "反派", "配角", "配音"};
		if(wordIndex){
			HashMap<String, Integer> WordIndex = new HashMap<String, Integer>();
		   for(int i = 0; i < fouces.length; i ++){
			   WordIndex.put(fouces[i], i); 
		   }
		   return WordIndex;			
		   //否则，创建【下标：关注点】映射
		}else{
			HashMap<Integer, String> IndexWord = new HashMap<Integer, String>();
			   for(int i = 0; i < fouces.length; i ++){
				   IndexWord.put(i, fouces[i]); 
			   }
			   return IndexWord;					
		}
	}
	
	public double[] totalComAnalysis() throws IOException{
		JavaRDD<String[]> splitRDD = this.inputRDD.map(new splitRecord()).filter(new filterEmpty());
		JavaPairRDD<String, Integer[]> totalComScoreList = splitRDD.mapToPair(new createPair());
		JavaRDD<Tuple2<String, Integer[]>> tResult = totalComScoreList.map(new findFouces());
		double[] tFinalCalculate = finalCalculate(tResult);
		return tFinalCalculate;
	}
	  static double[] finalCalculate(JavaRDD<Tuple2<String, Integer[]>> hResult){	 
		  /*
		   *  通过aggregate() 的RDD操作把所有的　Tuple2<String, Integer[]>记录进行数组对应下标累加计算，得到一类电影的24个关注点总得分
		   * 每组数据都计算百分数，四舍五入
		   * */
		  int[] initial = new int[]{0,0,0,0,0,0,0,0,0,0,
				  0,0,0,0,0,0,0,0,0,0,
				  0,0,0,0,0,0,0,0,0,0,
				  0};
		  Function2<int[], Tuple2<String, Integer[]>, int[]> seqOp = 
				  new Function2<int[], Tuple2<String, Integer[]>, int[]>(){
			  			public int[] call(int[] array, Tuple2<String, Integer[]> tup){
			  				for(int i = 0; i < array.length; i ++){
			  					array[i] += tup._2[i];
			  				}
			  				return array;
			  			}
		  		} ;
		Function2<int[], int[], int[]> combOp =
				new Function2<int[], int[], int[]>(){
					public int[] call(int[] one, int[] two){
						for(int i = 0; i < one.length; i ++){
							one[i] += two[i];
						}
						return one;
					}
				};
		int[] hfinalRDD = hResult.aggregate(initial, seqOp, combOp);
		double[] pres =new double[hfinalRDD.length];
		double total = 0.00;
		for(int i: hfinalRDD)
			total += i;
		DecimalFormat   df   =new   java.text.DecimalFormat("#.0000");  
		for(int k= 0; k < hfinalRDD.length; k ++){
				pres[k] = Double.valueOf(df.format(hfinalRDD[k]/ total));
		}
		return pres;
	  }
	static class findFouces implements Function<Tuple2<String, Integer[]>, Tuple2<String, Integer[]>>{
		String fouceTxt  ;
		HashMap<String, Integer> fouceIndexMap = setHashMap(true);
		ArrayList<String> filmmaking  ;
		ArrayList<String> invest  ;
		ArrayList<String> director  ;
		ArrayList<String> scriptWriter  ;
		ArrayList<String> market  ;
		ArrayList<String> manufacture  ;
		ArrayList<String> forenotice  ;
		
		ArrayList<String> theme  ;
		ArrayList<String> ideal  ;
		ArrayList<String> story  ;
		ArrayList<String> type  ;
		ArrayList<String> culture  ;
		
		ArrayList<String> content  ;
		ArrayList<String> start  ;
		ArrayList<String> end  ;
		ArrayList<String> develop  ;
		ArrayList<String> line  ;
		ArrayList<String> plot  ;
		
		ArrayList<String> video  ;
		ArrayList<String> picture  ;
		ArrayList<String> music  ;
		ArrayList<String> action  ;
		ArrayList<String> specialEffect  ;
		ArrayList<String> model  ;
		
		ArrayList<String> role  ;
		ArrayList<String> decent  ;
		ArrayList<String> hero  ;
		ArrayList<String> heroine  ;
		ArrayList<String> villain  ;
		ArrayList<String> costar  ;
		ArrayList<String> voice;
		
		public findFouces() throws IOException{
			this.fouceTxt  = CatchFile("/home/yingying/下载/关注点目录.txt", "utf-8");
			this.filmmaking = MatchAndCreate("制片:(.*?)投资出品:", fouceTxt);
			this.invest = MatchAndCreate("投资出品:(.*?)导演:", fouceTxt);
			this.director = MatchAndCreate("导演:(.*?)编剧:", fouceTxt);
			this.scriptWriter = MatchAndCreate("编剧:(.*?)市场商业:", fouceTxt);
			this.market = MatchAndCreate("市场商业:(.*?)制作:", fouceTxt);
			this.manufacture = MatchAndCreate("制作:(.*?)预告:", fouceTxt);
			this.forenotice = MatchAndCreate("预告:(.*?)主题:", fouceTxt);
			
			this.theme = MatchAndCreate("主题:(.*?)思想:", fouceTxt);
			this.ideal = MatchAndCreate("思想:(.*?)故事题材:", fouceTxt);
			this.story = MatchAndCreate("故事题材:(.*?)电影类型:", fouceTxt);
			this.type = MatchAndCreate("电影类型:(.*?)文化元素:", fouceTxt);
			this.culture = MatchAndCreate("文化元素:(.*?)情节内容:", fouceTxt);
			
			this.content = MatchAndCreate("情节内容:(.*?)开头:", fouceTxt);
			this.start = MatchAndCreate("开头:(.*?)结局:", fouceTxt);
			this.end = MatchAndCreate("结局:(.*?)发展:", fouceTxt);
			this.develop= MatchAndCreate("发展:(.*?)台词:", fouceTxt);
			this.line = MatchAndCreate("台词:(.*?)剧情:", fouceTxt);
			this.plot = MatchAndCreate("剧情:(.*?)演员角色:", fouceTxt);
			
			this.video = MatchAndCreate("视听效果:(.*?)画面:", fouceTxt);
			this.picture= MatchAndCreate("画面:(.*?)音乐:", fouceTxt);
			this.music = MatchAndCreate("音乐:(.*?)动作:", fouceTxt);
			this.action = MatchAndCreate("动作:(.*?)特效:", fouceTxt);
			this.specialEffect = MatchAndCreate("特效:(.*?)造型设计:", fouceTxt);
			this.model = MatchAndCreate("造型设计:(.*?)正派:", fouceTxt);
			
			this.role = MatchAndCreate("演员角色:(.*?)正派:", fouceTxt);
			this.decent = MatchAndCreate("正派:(.*?)男主:", fouceTxt);
			this.hero = MatchAndCreate("男主:(.*?)女主:", fouceTxt);
			this.heroine = MatchAndCreate("女主:(.*?)反派:", fouceTxt);
			this.villain = MatchAndCreate("反派:(.*?)配角:", fouceTxt);
			this.costar = MatchAndCreate("配角:(.*?)配音:", fouceTxt);
			this.voice = MatchAndCreate("配音:(.*?)END", fouceTxt);
		}
		public String CatchFile(String path, String decode) throws IOException{
			//读取文件：把文件转换为一个字符串返回
			String s = "";
			FileInputStream in = new FileInputStream(path);
			InputStreamReader inReader = new InputStreamReader(in, decode);
			BufferedReader bufReader = new BufferedReader(inReader);
			System.out.println("FindFouce: success to read the txt file");
			String line = "";
			while((line = bufReader.readLine()) != null){
				if(!line.equals("--------------------------------------------------------------------"))
					s += line;
			}
			s+= "END";//（注：在文件最后添加“角色名”是为了正则容易匹配）
			return s;
		}
	    
		private ArrayList MatchAndCreate(String string, String fouceTxt2) {
			ArrayList<String> resultList = new ArrayList<String>();
			String s = "";
			Pattern p = Pattern.compile(string);
			Matcher m = p.matcher(fouceTxt2);
			if(m.find()){
				s = m.group(1);
			}
			if(s.length() == 0){
				resultList.add(string.substring(0, string.indexOf(":")));
			}else{
				String[] sArray = s.split(" ");
				for(String word: sArray)
					resultList.add(word);
			}
			return resultList;
		}
		public Tuple2<String, Integer[]> call(Tuple2<String, Integer[]> tup){
			//theme
			for(String word: this.theme){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("主题")] += 1;
				}
			}
			for(String word: this.ideal){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("思想")] += 1;
				}
			}
			for(String word: this.story){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("故事题材")] += 1;
				}
			}
			for(String word: this.type){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("电影类型")] += 1;
				}
			}
			for(String word: this.culture){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("文化元素")] += 1;
				}
			}
			tup._2[fouceIndexMap.get("主题")] += tup._2[ fouceIndexMap.get("思想")] + tup._2[ fouceIndexMap.get("电影类型")] 
					+ tup._2[ fouceIndexMap.get("文化元素")]	+ tup._2[ fouceIndexMap.get("故事题材")];
			
			//shiting 
			for(String word: this.action){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("动作")] += 1;
				}
			}
			for(String word: this.video){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("视听效果")] += 1;
				}
			}
			for(String word: this.picture){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("画面")] += 1;
				}
			}
			for(String word: this.music){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("音乐")] += 1;
				}
			}
			for(String word: this.specialEffect){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("特效")] += 1;
				}
			}
			for(String word: this.model){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("造型设计")] += 1;
				}
			}
			tup._2[fouceIndexMap.get("视听效果")] += tup._2[ fouceIndexMap.get("动作")] + tup._2[ fouceIndexMap.get("造型设计")] + 
					 tup._2[ fouceIndexMap.get("画面")] + tup._2[ fouceIndexMap.get("音乐")]  + tup._2[ fouceIndexMap.get("特效")];
			
			//content
			for(String word: this.start){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("开头")] += 1;
				}
			}
			for(String word: this.develop){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("发展")] += 1;
				}
			}
			for(String word: this.end){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("结局")] += 1;
				}
			}
			for(String word: this.content){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("情节内容")] += 1;
				}
			}
			for(String word: this.line){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("台词")] += 1;
				}
			}
			for(String word: this.plot){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("剧情")] += 1;
				}
			}
			tup._2[fouceIndexMap.get("情节内容")] += tup._2[ fouceIndexMap.get("开头")] + tup._2[ fouceIndexMap.get("发展")] + 
					 tup._2[ fouceIndexMap.get("结局")] + tup._2[ fouceIndexMap.get("台词")]  + tup._2[ fouceIndexMap.get("剧情")] ;
			
			//manufacture
			for(String word: this.scriptWriter){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("编剧")] += 1;
				}
			}
			for(String word: this.director){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("导演")] += 1;
				}
			}
			for(String word: this.filmmaking){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("制片")] += 1;
				}
			}
			for(String word: this.market){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("市场商业")] += 1;
				}
			}
			for(String word: this.manufacture){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("制作")] += 1;
				}
			}
			for(String word: this.invest){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("投资出品")] += 1;
				}
			}
			for(String word: this.forenotice){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("预告")] += 1;
				}
			}
			tup._2[ fouceIndexMap.get("制片")] += tup._2[ fouceIndexMap.get("导演")] + tup._2[ fouceIndexMap.get("投资出品")] + 
					 tup._2[fouceIndexMap.get("编剧")] +  tup._2[ fouceIndexMap.get("市场商业")]+ 
					 tup._2[fouceIndexMap.get("投资出品")] +  tup._2[ fouceIndexMap.get("预告")] ;
			
			//role
			for(String word: this.role){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("演员角色")] += 1;
				}
			}
			for(String word: this.decent){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("正派")] += 1;
				}
			}
			for(String word: this.voice){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("配音")] += 1;
				}
			}
			for(String word: this.hero){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("男主")] += 1;
				}
			}
			for(String word: this.heroine){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("女主")] += 1;
				}
			}
			for(String word: this.villain){				
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("反派")] += 1;
				}
			}
			for(String word: this.costar){
				if(tup._1.contains(word)){
					tup._2[ fouceIndexMap.get("配角")] += 1;
				}
			}
			tup._2[fouceIndexMap.get("演员角色")] =  tup._2[ fouceIndexMap.get("男主")] +tup._2[ fouceIndexMap.get("女主")] 
					+ tup._2[ fouceIndexMap.get("反派")] +  tup._2[ fouceIndexMap.get("配角")]+tup._2[ fouceIndexMap.get("正派")] 
							+ tup._2[ fouceIndexMap.get("反派")] +  tup._2[ fouceIndexMap.get("配音")] ;
			return tup;
		}
		}

	
	
	static class createPair implements PairFunction<String[], String, Integer[]>{
		public Tuple2<String, Integer[] > call(String[] str){
			return new Tuple2<String, Integer[]>(str[str.length - 1], new Integer[]{0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0});
		}
	}
	static class splitRecord implements Function<String, String[]>{
		public String[] call(String s){
			return s.split(",");
		}
	}
	static class filterEmpty implements Function<String[], Boolean>{
		//数据清洗：确保　评分和评论　的正确性
		public Boolean call(String[] array){
			if(array.length == 3){
				Pattern pattern = Pattern.compile("^[0-9]+(.[0-9]+)?$"); 
			   Matcher isNum = pattern.matcher(array[0]);
			   if( (!array[0].isEmpty()) && (isNum.matches()) && (! array[2].isEmpty()) ){
				   return true;
			}else 
				return false; 	
		}else
			return false;
	}
	}
	
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		//FindFouce fa = new FindFouce("/home/yingying/sqlIdFile/ChangChen.txt");
	}
}

