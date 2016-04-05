/**
 * class: MapReduceMeta
 * vasion:v1.0
 * time: 9-20
 * author:jessicajin
 */
package com.tencent.isd.lhotse.runner.util;

import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MapReduceMeta {

	/**
	 * @param args
	 */
	String day="%TM_DAY";
	String yesterday="%TM_YESTERDAY";
	String month="%TM_MONTH";
	String lastMonth="%TM_LASTMONTH";
	String yesMonth="%TM_YESMONTH";
	String hour="%TM_HOUR";
	String func="$SUBSTR";
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		MapReduceMeta ob=new MapReduceMeta();
		System.out.println(ob.parasPath(" hadoop fs -D fs.default.name=sn-tcss.tencent-distribute.com:54310 -D hadoop.job.ugi=boss,boss -ls /user/boss/pgv/%TM_YESTERDAY"));

	}
	/**
	 * parasPath
	 *     the script contain a lot of self-define command,we need to standard
	 * @param script
	 * @return
	 */
	 public String parasPath(String script){
		   String value="";
		   SimpleDateFormat sdf;
		   //parent
		   Pattern dayp=Pattern.compile("%TM_DAY(\\-[0-9]+)?");
		   Pattern yesterdayp=Pattern.compile("%TM_YESTERDAY");
		   
		   Pattern monthp=Pattern.compile("%TM_MONTH(\\-[0-9]+)?");
		   Pattern lastmonthp=Pattern.compile("%TM_LASTMONTH");
		   Pattern yesterday_monthp=Pattern.compile("%TM_YESMONTH");
		   
		   Pattern hourp=Pattern.compile("%TM_HOUR(\\-[0-9]+)?");
		   Pattern funcp=Pattern.compile("(\\$substr)|(\\$SUBSTR)\\(.*\\)");
		   //---time---
		   Matcher daym=dayp.matcher(script);
		   while (daym.find()) {
			   if(daym.group().length()>day.length()){
				   int offset =-Integer.parseInt(daym.group().substring(day.length()+1,daym.group().length()));
					Calendar c = Calendar.getInstance();
					c.setTime(new Date());
					c.add(Calendar.DAY_OF_MONTH, offset);
					sdf = new SimpleDateFormat("yyyyMMdd");
					value=sdf.format(c.getTime());
					script=script.replace(daym.group(),value);
			   }else{
			      sdf = new SimpleDateFormat("yyyyMMdd");
			      value=sdf.format(new Date());
			      script=script.replace(daym.group(),value);
			   }
		   }
		   Matcher yesterdaym=yesterdayp.matcher(script);
		   if (yesterdaym.find()) {
			    int offset =-1;
				Calendar c = Calendar.getInstance();
				c.setTime(new Date());
				c.add(Calendar.DAY_OF_MONTH, offset);
				sdf = new SimpleDateFormat("yyyyMMdd");
				value=sdf.format(c.getTime());		
			   script=yesterdaym.replaceAll(value);
		   }
		   
		   Matcher monthm=monthp.matcher(script);
		   while (monthm.find()) {
			   if(monthm.group().length()>month.length()){
				    int offset =-Integer.parseInt(monthm.group().substring(month.length()+1,monthm.group().length()));
					Calendar c = Calendar.getInstance();
					c.setTime(new Date());
					c.add(Calendar.MONTH, offset);
					sdf = new SimpleDateFormat("yyyyMM");
					value=sdf.format(c.getTime());
					script=script.replace(monthm.group(),value);
			   }else{
				   sdf = new SimpleDateFormat("yyyyMM");
			       value=sdf.format(new Date());
			       script=script.replace(monthm.group(),value);
			   }
		   }
		   Matcher lastmonthm=lastmonthp.matcher(script);
		   if (lastmonthm.find()) {
			   int offset =-1;
				Calendar c = Calendar.getInstance();
				c.setTime(new Date());
				c.add(Calendar.MONTH, offset);
				sdf = new SimpleDateFormat("yyyyMM");
				value=sdf.format(c.getTime());
			   script=lastmonthm.replaceAll(value);
		   }
		   
		   Matcher yesterday_monthm=yesterday_monthp.matcher(script);
		   if (yesterday_monthm.find()) {
			   int offset =-1;
				Calendar c = Calendar.getInstance();
				c.setTime(new Date());
				c.add(Calendar.DAY_OF_MONTH, offset);
				sdf = new SimpleDateFormat("yyyyMM");
				value=sdf.format(c.getTime());
			   script=yesterday_monthm.replaceAll(value);
		   }
		   
		   Matcher hourm=hourp.matcher(script);
		   while(hourm.find()) {
			   if(hourm.group().length()>hour.length()){
				    int offset =-Integer.parseInt(hourm.group().substring(hour.length()+1,hourm.group().length()));
					Calendar c = Calendar.getInstance();
					c.setTime(new Date());
					c.add(Calendar.HOUR, offset);
					sdf = new SimpleDateFormat("yyyyMMddHH");
					value=sdf.format(c.getTime());
					script=script.replace(hourm.group(),value);
			   }else{
			      sdf = new SimpleDateFormat("yyyyMMddHH");
			      value=sdf.format(new Date());
			      script=script.replace(hourm.group(),value);
			   }
		   }
		   //----------funcation----------------------
		   Matcher funcm=funcp.matcher(script);
		   while(funcm.find()){
			   System.out.println(funcm.group());
			   String params[]=funcm.group().substring(func.length()+1,funcm.group().length()-1).split(",");
				int fromindex=Integer.parseInt(params[0].trim());
				int toindex=Integer.parseInt(params[1].trim());
				value=params[2].trim().substring(fromindex,toindex);
				script=script.replace(funcm.group(),value);
				 System.out.println(script);
		   }
		   return script;
		   
	   }

}
