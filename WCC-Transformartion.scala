// Databricks notebook source
import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.functions._
import sqlContext.implicits._
import com.microsoft.azure.sqldb.spark.query._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

/* Selects active month id from WCC_PARAM*/
var flag=1
val outputFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

/********************************************************************************************/
/* Current Execution Month/Peiod is captured and Stored in the variable select_month_query*/
/********************************************************************************************/

val select_month_query="""
                      |select PARAM_VAL from dbo.WCC_PARAM where PARAM_NM='MON_ID' and ACTV_FLG='Y'
                      """
/********************************************************************************************/
/* Secret/ Credntials are retrieved using pre build Scala functions and Config file for Azure SQL DB is created*/
/********************************************************************************************/

val DBQueryConfig = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "db_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
  "queryCustom"       -> "",
  "bulkCopyTableLock" -> "true"
)

val SelectMonth = DBQueryConfig + ("queryCustom" -> select_month_query)
val SelectMonthConfig =Config(SelectMonth)

var df_month = spark.read.sqlDB(SelectMonthConfig)

/********************************************************************************************/
/* month_id_list array type variable will be used in code whose 0th array contains the current execution month */
/********************************************************************************************/

val month_id_list = df_month.select("PARAM_VAL").collect().map(_(0)).toList   

/********************************************************************************************/
/* Secret/ Credntials are retrieved using pre build Scala functions and Config file for Azure SQL DW is created*/
/********************************************************************************************/

val DWQueryConfig = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "dw_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    //"dbTable"           -> "abcd",
    "queryCustom"       -> "",
    "bulkCopyTableLock" -> "true"
  )

var now = Calendar.getInstance().getTime()
var StartDate = outputFormat.format(now)

/********************************************************************************************/
/* Truncates WCC_FILE_RUN and WCC_MOD_RUN tables in DW and inserts a start entry in MOD_RUN in DW indicating Data Bricks code is started */
/********************************************************************************************/

val delete_insert_log ="""
                    |TRUNCATE TABLE dbo.WCC_FILE_RUN 
                    |TRUNCATE TABLE dbo.WCC_MOD_RUN
                    |INSERT INTO dbo.WCC_MOD_RUN (MOD_ID,MOD_STRT_DT,MON_ID,STATUS)
                    |VALUES (2, '%s' , '%s', 'Started')
                   """.stripMargin.format(StartDate,month_id_list(0))

val DeleteLoadEntryToLog = DWQueryConfig + ("queryCustom" -> delete_insert_log)
val DeleteLoadEntryToLogConfig =Config(DeleteLoadEntryToLog)
sqlContext.sqlDBQuery(DeleteLoadEntryToLogConfig)

/********************************************************************************************/
/* ADLS Folder structure in configured in metadata based on subject area and retrieved to loop through ADLS to fetch the list
of transformed files for downstream processing*/
/********************************************************************************************/

val mapg_query_adls = """
SELECT '/mnt/'+FileName AS FolderPath,FileName AS FolderName,row_number() OVER(PARTITION BY NULL ORDER BY FileName) AS RN FROM DBO.FileIdentifier
WHERE 1=1
--AND FileName='Development_CashFlow'
AND FileName NOT IN ('Development_StatusReport_Construction_Development','MajorWorks_KPI')
--AND FileName ='Development_StatusReport'
            """

val Copy_adls = DBQueryConfig + ("queryCustom" -> mapg_query_adls)
val CopyConfig_adls =Config(Copy_adls)

var df_mapg_adls = spark.read.sqlDB(CopyConfig_adls)
df_mapg_adls.show()

/********************************************************************************************/
/*Intermdidate Variables are created to store the Folder Path and Folder Name*/
/********************************************************************************************/

var a_len :Int =0
val fn_len =df_mapg_adls.select("RN").collect().map(_(0)).toList
val fn_path=df_mapg_adls.select("FolderPath").collect().map(_(0)).toList
val fn_name=df_mapg_adls.select("FolderName").collect().map(_(0)).toList

var df=Seq.empty[(String, String, String)].toDF("FolderName", "FileName","GeneratedTime")
//var df2=Seq.empty[(String, String)].toDF("FolderName", "FileName")
var df2 = Seq(
  ("", "", "")
).toDF("FolderName", "FileName","GeneratedTime")

/********************************************************************************************/
/* Looped through ADLS folders based on metadata to store subject area wise transformed files and captured in a scala dataframe named DF */
/********************************************************************************************/

for ( a <- 0 to fn_len.length-1 )
{
  
  val folder_name=fn_name(a)
  val temp=fn_path(a)
  val path="dbfs:"+fn_path(a)+"/"
  /*println("\n")
  println("*****************************************************")
  println("*****************************************************")
  println(path)
  println("*****************************************************")
  println("*****************************************************")
  println("\n")*/
  val filelist=dbutils.fs.ls(path)
  //println(filelist)
  
    var b_len :Int =0
    for (b_len <- 0 to filelist.length -1 )
    {
        val file_dtls=filelist(b_len)
        //println(file_dtls)
          
        var file_nm : String= file_dtls.toString
        //println(file_nm)
      
        var adls_end_pos = file_nm.indexOf(",",1)
        var adls_file = file_nm.substring(9,adls_end_pos)
        //println(adls_file)
      
        var x=adls_file.indexOf("/",10)
        var y=adls_file.indexOf(".csv",10)
      
        var adls_file_only=adls_file.substring(x+1,y+4)
       // println(adls_file_only)
        //println(folder_name)
        
        var df3 = df2.withColumn("FolderName", lit(folder_name)).withColumn("FileName", lit(adls_file_only)).withColumn("GeneratedTime", lit(StartDate))
        //df3.show()
        df=df.union(df3)   
            
    }    
}

df.show()

val WriteToDBConfig = Config(Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "db_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    "dbTable"           -> "dbo.ADLS_files",
    "bulkCopyTableLock" -> "true"
  ))

/********************************************************************************************/
/*Scala data frame df is loaded in the temporary table ADLS_files in Azure SQL*/
/********************************************************************************************/

df.write.mode(SaveMode.Overwrite).sqlDB(WriteToDBConfig)

/********************************************************************************************/
/*Subject area wise, folder wise, table name, column name, column ordinal positions are stored in fileidenfier, sheettracker, columnheaders table in Azure
SQL DB. Azure function logs the parsed file details into ADLS in FileAuditLog,ProcessedFileAuditLog tables. Both metadata tables and log tables combined
together to get the file wise, sheet wise, process file wise source column mapping vs target landing table mapping

The output of this quey is a 1-> n relation wrt sheet wise parsed file vs landing table name based on no of files uploaded in sharepoint
Entire data data set is captured in variable mapg_query
*/
/********************************************************************************************/

val mapg_query = """
|select C.* FROM
|(
|	select TOP 100000 ROW_NUMBER() OVER (ORDER BY A.SheetID ASC) as RowNumber,
|	B.ProcessedFileName as [Sheet_Name]
|	,A.*,
|	LTRIM(RTRIM(SUBSTRING(B.ProcessedFileContainerName,26,LEN(B.ProcessedFileContainerName)))) as [ADLS_Folder] from
|	(
|		select FI.FileID, 
|		ST.SheetID, 
|		ST.LandingTableName as [TBL_NM],
|		string_agg('['+CH.HeaderName+']',',') AS [Source_Column_names],
|		string_agg('['+CH.HeaderName+']'+' varchar(1000)',',') as CreateTableStmt,
|		string_agg('['+CH.LandingColumnName+']',',') AS [Landing_Column_names],
|		ST.InclHeaders as InclHeaders
|		from 
|		dbo.FileIdentifier FI
|		INNER JOIN dbo.SheetTracker ST on ST.FileID=FI.FileID
|		INNER JOIN (select top 100000 SheetID,HeaderName,LandingColumnName from dbo.ColumnHeaders where isActive=1 ORDER BY sheetid,Sequence asc) CH  ON ST.SheetID=CH.SheetID
|		where ISNULL(ST.LND_FLAG,'N')='Y'
|		GROUP BY
|		ST.SheetID, 
|		FI.FileID , ST.LandingTableName, ST.InclHeaders
|	)A
|	INNER JOIN 
|	(
|		select I.* from 
|		(
|			select ROW_NUMBER() OVER (PARTITION BY PFA.ProcessedFileName ORDER BY PFA.ProcessEndDate desc) as RowNumber, 
|			PFA.FileSheetID,
|			PFA.BlobFileDetailsTransactionID,FA.TransactionID,PFA.ProcessedFileName,PFA.ProcessedFileContainerName 
|			from dbo.FileAuditLog FA
|			inner join dbo.ProcessedFileAuditLog PFA 
|			on PFA.BlobFileDetailsTransactionID=FA.TransactionID
|			where FA.MonthID=%s
|		)I where I.RowNumber=1
|			--and PFA.AdlsTransferStatus='END'
|	)B 
|	ON A.SheetID=B.FileSheetID ORDER BY B.FileSheetID
|)C
|INNER JOIN dbo.adls_files AF ON AF.FileName=C.Sheet_Name
            """.stripMargin.format(month_id_list(0))
//println(mapg_query)
val Copy = DBQueryConfig + ("queryCustom" -> mapg_query)
val CopyConfig =Config(Copy)

var df_mapg = spark.read.sqlDB(CopyConfig)
df_mapg.show()
df_mapg.createOrReplaceTempView("MapgTable")

/********************************************************************************************/
/* Previously created variable mapg_query holds information about unique set of landing tables will be loaded in this iteration of databricks
Thus distinct list of table names are captured in the variable dataframe df_distinct_lnd_table
List of captured landing tables to be deleted for current execution month before processing same month*/
/********************************************************************************************/

val df_distinct_lnd_table = spark.sql("SELECT distinct TBL_NM FROM MapgTable")
df_distinct_lnd_table.show()
val distinct_tbl_nm = df_distinct_lnd_table.select("TBL_NM").collect().map(_(0)).toList

//val sheet_id_list = df_mapg.select("SheetID").collect().toList

/********************************************************************************************/
/* Variable Sheet_list stores the list of unique transformed file name to iterate in loop to load in Landing layer table */
/********************************************************************************************/

val sheet_list = df_mapg.select("Sheet_Name").collect().map(_(0)).toList

var a :Int = 0

    val BulkCopytoTempConfig = Config(Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "dw_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    //"dbTable"           -> "abcd",
    "dbTable"       -> "dbo.create_for_test"
  ))
    val BulkCopytoDBConfig = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "db_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    //"dbTable"           -> "abcd",
    "dbTable"       -> ""
  )

var CreateTable = List[Map[String,String]]()
var CreateTableConfig = List[Config]()

//var DeleteFromTable = List[Map[String,String]]()
//var DeleteFromTableConfig = List[Config]()

var UpdateFileRunStatus = List[Map[String,String]]()
var UpdateFileRunStatusConfig = List[Config]()

var DWInsert = List[Map[String,String]]()
var DWInsertConfig = List[Config]()

var InsertIntoFileRunTable = List[Map[String,String]]()
var InsertIntoFileRunTableConfig = List[Config]()

/********************************************************************************************/
/* dataframe df_distinct_lnd_table executed in loop to delete the pre existing data from landing tables for current execution month*/
/********************************************************************************************/

for (a <- 0 to distinct_tbl_nm.length-1)
{
  val delete_query=
    """
    |DELETE FROM %s WHERE MON_ID='%s'
    """.stripMargin.format("dbo."+distinct_tbl_nm(a),month_id_list(0))
  
    println(delete_query)

    var DeleteFromTable = DWQueryConfig + ("queryCustom" -> delete_query)
    var DeleteFromTableConfig =Config(DeleteFromTable)
  
    sqlContext.sqlDBQuery(DeleteFromTableConfig)
}

var landing_table="abcd"

/********************************************************************************************/
/* Previously created variable mapg_query holds information about list of files to be processed in respective landing layer tables
Loop will be running for list of files and loaded in landing layer tables defined as per metadata table*/
/********************************************************************************************/

for (a <- 0 to sheet_list.length-1)
{
    //println(row_number(a))
    /*var sheet_name_str= "select Sheet_Name from MapgTable where RowNumber=%d".format(a+1)
    println(sheet_list(a))
    println(sheet_name_str)
    var sheet_name :String= spark.sql(sheet_name_str).first().toString
    println(sheet_name)
    var sheet_name1=sheet_name.replace('[',' ')*/
    println(sheet_list(a))
    
    /********************************************************************************************/
    /* sheet_id variable stores the sheetid of the transformed file indicating the transformed file belongs to which file type and sheet type*/
    /********************************************************************************************/
    var sheet_id_str = "select cast(SheetID as VARCHAR(10)) from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var sheet_id : String= spark.sql(sheet_id_str).first.getString(0)
    println(sheet_id)
  
    /********************************************************************************************/
    /* lnd_tbl variable stores the landing table name of the transformed file */
    /********************************************************************************************/
    var lnd_tbl_str = "select TBL_NM from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var lnd_tbl : String= spark.sql(lnd_tbl_str).first.getString(0)
    println(lnd_tbl)
  
    /********************************************************************************************/
    /* adls_folder variable stores the folder name of the transformed file to dynamically navigate to the respective folder*/
    /********************************************************************************************/
    var adls_folder_str = "select adls_folder from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var adls_folder : String= spark.sql(adls_folder_str).first.getString(0)
    println(adls_folder)
  
    /********************************************************************************************/
    /* src_clmn variable stores the column sequence of the transformed file to dynamically generate INSERT INTO statement*/
    /********************************************************************************************/
  
    var src_clmn_str = "select Source_Column_names from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var src_clmn : String= spark.sql(src_clmn_str).first.getString(0)
    println(src_clmn)
  
    /********************************************************************************************/
    /* create_tbl_clmn variable stores the column sequence of the temporary table to dynamically generate INSERT INTO statement*/
    /********************************************************************************************/
  
    var create_tbl_str = "select CreateTableStmt from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var create_tbl_clmn : String= spark.sql(create_tbl_str).first.getString(0)
    //println(create_tbl_clmn)
    
    /********************************************************************************************/
    /* lnd_clmn variable stores the column sequence of the Laniding table to dynamically generate INSERT INTO statement*/
    /********************************************************************************************/
  
    var lnd_clmn_str: String = "select Landing_Column_names from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var lnd_clmn : String= spark.sql(lnd_clmn_str).first.getString(0)
    //println(lnd_clmn)
    
    /********************************************************************************************/
    /* InclHeaders variable stores the Flag to indicate whether header record to be captured or not*/
    /********************************************************************************************/
    
    var InclHeaders_str: String = "select InclHeaders from MapgTable where Sheet_Name='%s'".format(sheet_list(a))                             
    var InclHeaders : String= spark.sql(InclHeaders_str).first.getString(0)
    println(InclHeaders)

    var month_id=month_id_list(0)
   // df_new.createOrReplaceTempView("Data")
    //var month_id=dbutils.widgets.get("X")
  
    /********************************************************************************************/
    /* Temporary table create_for_test is created with looped file column sequence*/
    /********************************************************************************************/
  
    val sql_query_1="""
                    |IF OBJECT_ID('dbo.create_for_test', 'U') IS NOT NULL
                    |DROP table dbo.create_for_test
                    |CREATE table dbo.create_for_test(%s,month_id varchar(255),LOAD_DATE varchar(255))
                    """.stripMargin.format(create_tbl_clmn)
  
    /********************************************************************************************/
    /* File Name vs Landing table entry added in WCC_FILE_RUN in DW layer to indicate initiation*/
    /********************************************************************************************/
  
    var now2 = Calendar.getInstance().getTime()
    var StartDate2 = outputFormat.format(now2)
     val insert_into_file_run_start="""
               |INSERT INTO dbo.WCC_FILE_RUN (SHEET_ID,MOD_ID,FILE_NM,LND_TBL_NM,FILE_STRT_DT,MON_ID,STATUS)
               |VALUES (%d, 2,  '%s', '%s', '%s',  %s, 'Started')
              """.stripMargin.format(sheet_id.toInt,sheet_list(a),lnd_tbl,StartDate2, month_id)
    //println(insert_into_file_run_start)
  
  
    val sql_query_2="""
                |BEGIN TRY
                |  INSERT INTO %s(%s,MON_ID,LOAD_DATE)
                |  select %s,month_id,LOAD_DATE from dbo.create_for_test
                |
                |  UPDATE dbo.WCC_FILE_RUN 
                |  SET [STATUS]='Success',FILE_END_DT=GETDATE()
                |  WHERE SHEET_ID=%d AND FILE_NM= '%s'
                |END TRY
                |BEGIN CATCH
                |  UPDATE dbo.WCC_FILE_RUN 
                |  SET [STATUS]='Failure',FILE_END_DT=GETDATE(),ERR_DTLS= ERROR_MESSAGE()
                |  WHERE SHEET_ID=%d AND FILE_NM= '%s'
                |END CATCH
          """.stripMargin.format("dbo."+lnd_tbl,lnd_clmn,src_clmn,sheet_id.toInt,sheet_list(a),sheet_id.toInt,sheet_list(a))
  
    now = Calendar.getInstance().getTime()
    var EndDate = outputFormat.format(now)
    
    CreateTable = CreateTable :+ DWQueryConfig + ("queryCustom" -> sql_query_1)
    CreateTableConfig =CreateTableConfig  :+ Config(CreateTable(a))
    
    
    DWInsert = DWInsert :+ DWQueryConfig + ("queryCustom" -> sql_query_2)
    DWInsertConfig = DWInsertConfig :+ Config(DWInsert(a))
    
    InsertIntoFileRunTable = InsertIntoFileRunTable :+ DWQueryConfig + ("queryCustom" -> insert_into_file_run_start)
    InsertIntoFileRunTableConfig =InsertIntoFileRunTableConfig  :+ Config(InsertIntoFileRunTable(a))
    
    //UpdateFileRunStatus = UpdateFileRunStatus :+ DBQueryConfig + ("queryCustom" -> update_file_run_success)
    //UpdateFileRunStatusConfig =UpdateFileRunStatusConfig  :+ Config(UpdateFileRunStatus(a))
    
    println("Logging into File Run !")
    sqlContext.sqlDBQuery(InsertIntoFileRunTableConfig(a))
    
    /********************************************************************************************/
    /* path variable stores the absolute location of ADLS for parsing
       InclHeaders variable to decide whether header record is required or not
    */
    /********************************************************************************************/
  
    var path="/mnt/"+adls_folder+"/"+sheet_list(a)
    println(path)
    
    val df1 =
    if (InclHeaders=="Y")
    {
      spark.read.option("escape", "\"").csv(path)
    }
    else
    {
      spark.read.option("header", "true").option("escape", "\"").csv(path)
    }
    //df1.show()
    //var month_id=dbutils.widgets.get("X").toInt
    
    /********************************************************************************************/
    /* month_id variable and LOAD_DATE to capture auditing details in landing table*/
    /********************************************************************************************/
  
    val df_new = df1.withColumn("month_id",lit(month_id))
                   .withColumn("LOAD_DATE",lit(StartDate))
    
    /*if (lnd_tbl!=landing_table)
    {
      sqlContext.sqlDBQuery(DeleteFromTableConfig(a))
    }
    
    landing_table=lnd_tbl*/
  
    /********************************************************************************************/
    /* Temporary tables data is loaded into final landing table and WCC_FILE_RUN is updated to capture the status*/
    /********************************************************************************************/
  
    try{
        println("Creating Table")
        sqlContext.sqlDBQuery(CreateTableConfig(a))
        println("Table created")
        df_new.bulkCopyToSqlDB(BulkCopytoTempConfig)
        println("Data inserted to temp")
        /*var UpdateFileRunToSuccess = DBQueryConfig + ("queryCustom" -> update_file_run_success)
        var UpdateFileRunToSuccessConfig =Config(UpdateFileRunToSuccess)
        sqlContext.sqlDBQuery(UpdateFileRunStatusConfig(a))*/
        sqlContext.sqlDBQuery(DWInsertConfig(a))
        println("DATA LOADING COMPLETE !")
        println("Updating File run to success !\n\n")
    
     }
  catch{
        case t: Throwable => println(sheet_list(a),"Throwing Exception : ",t)
       //("CAUGHT AN EXCEPTION IN !",sheet_list(a))
          var flag=0
         val update_file_run_failure="""
               |UPDATE dbo.WCC_FILE_RUN 
               |SET [STATUS]='Failure',FILE_END_DT=GETDATE(),ERR_DTLS= '%s'
               |WHERE SHEET_ID=%d AND FILE_NM= '%s'
              """.stripMargin.format(t.toString().slice(0,2000),sheet_id.toInt,sheet_list(a))
        println(update_file_run_failure)
        var UpdateFileRunToFailure = DWQueryConfig + ("queryCustom" -> update_file_run_failure)
        var UpdateFileRunToFailureConfig = Config(UpdateFileRunToFailure)

        sqlContext.sqlDBQuery(UpdateFileRunToFailureConfig)
        }
 
}
    val update_mod_run_success="""
               |UPDATE dbo.WCC_MOD_RUN 
               |SET [STATUS]='Success',MOD_END_DT=GETDATE()
               |WHERE MOD_ID=2
              """.stripMargin

    val update_mod_run_failure ="""
               |UPDATE dbo.WCC_MOD_RUN 
               |SET [STATUS]='Failed',MOD_END_DT=GETDATE(),ERR_DTLS='Refer to WCC_File_Run'
               |WHERE MOD_ID=2
              """.stripMargin

    var UpdateModRunToSuccess = DWQueryConfig + ("queryCustom" -> update_mod_run_success)
    var UpdateModRunToFailure = DWQueryConfig + ("queryCustom" -> update_mod_run_failure)
   
    /********************************************************************************************/
    /* WCC_MOD_RUN status column is used to determine the overall failure and trap error messages*/
    /********************************************************************************************/

   var UpdateModRunConfig :com.microsoft.azure.sqldb.spark.config.Config= null
    
    if (flag==1)
    {
      UpdateModRunConfig=Config(UpdateModRunToSuccess)
      sqlContext.sqlDBQuery(UpdateModRunConfig)
    }
    else
    {
      UpdateModRunConfig=Config(UpdateModRunToFailure)
      sqlContext.sqlDBQuery(UpdateModRunConfig)
      throw new Exception("Failing the code")
    }

val wcc_file_run_query="select * from dbo.WCC_FILE_RUN"
val CopyWccFileRun = DWQueryConfig + ("queryCustom" -> wcc_file_run_query)
val CopyWccFileRunConfig =Config(CopyWccFileRun)

var df_wccfilerun = spark.read.sqlDB(CopyWccFileRunConfig)
df_wccfilerun.show()

val db_wccfilerun_del="DELETE from dbo.WCC_FILE_RUN where MON_ID='%s'".format(month_id_list(0))
println(db_wccfilerun_del)

    /********************************************************************************************/
    /* Entire WCC_FILE_RUN is transferred from DW to DB*/
    /********************************************************************************************/

sqlContext.sqlDBQuery(Config(DBQueryConfig + ("queryCustom" -> db_wccfilerun_del)))
df_wccfilerun.bulkCopyToSqlDB(Config(BulkCopytoDBConfig + ("dbTable" -> "dbo.WCC_FILE_RUN")))

// COMMAND ----------

import com.microsoft.azure.sqldb.spark.config.Config
import com.microsoft.azure.sqldb.spark.connect._
import org.apache.spark.sql.functions._
import sqlContext.implicits._
import com.microsoft.azure.sqldb.spark.query._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.SaveMode
import java.text.SimpleDateFormat
import java.util.{Calendar, Date}

val ReadTablefromDBMap = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "db_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    "dbTable"         -> "",
  //"queryCustom"       -> select_month_query,
  "bulkCopyTableLock" -> "true"
)

var a :Int = 0

  val DwQueryConfig = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "dw_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    //"dbTable"           -> "",
    "queryCustom"       -> "",
    //"bulkCopyBatchSize" -> "2500",
    "bulkCopyTableLock" -> "true"
  )

    val WriteToDWConfig = Map(
    "url"               -> dbutils.secrets.get(scope = "wcc",key = "url"),
    "databaseName"      -> dbutils.secrets.get(scope = "wcc",key = "dw_name"),
    "user"              -> dbutils.secrets.get(scope = "wcc",key = "username"),
    "password"          -> dbutils.secrets.get(scope = "wcc",key = "password"),
    "dbTable"           -> "",
    //"queryCustom"       -> "",
    //"bulkCopyBatchSize" -> "2500",
    "bulkCopyTableLock" -> "true"
  )

var ReadTablefromDB = List[Map[String,String]]()
var ReadTablefromDBConfig = List[Config]()

var DeleteFromTable = List[Map[String,String]]()
var DeleteFromTableConfig = List[Config]()

var DWWrite = List[Map[String,String]]()
var DWWriteConfig = List[Config]()
/*
var SPCall = [Map[String,String]]
var SPCallConfig = [Config]*/

/********************************************************************************************/
/* Metadata tables are synced between DB and DW to always have the latest instance of metadata in both environments*/
/********************************************************************************************/

val table_list=List[String]("dbo.FileIdentifier","dbo.SheetTracker","dbo.ColumnHeaders","dbo.WCC_PARAM")

for (a <- 0 to table_list.length-1)
{
  //try{
    //var month_id=month_id_list(0)
   // df_new.createOrReplaceTempView("Data")
    //var month_id=dbutils.widgets.get("X")
    val delete_query="""
                    |IF OBJECT_ID('%s','U') IS NOT NULL
                    |TRUNCATE TABLE %s
                   """.stripMargin.format(table_list(a),table_list(a))
    println(table_list(a))
    ReadTablefromDB = ReadTablefromDB :+ ReadTablefromDBMap + ("dbTable" -> table_list(a))
    ReadTablefromDBConfig =ReadTablefromDBConfig  :+ Config(ReadTablefromDB(a))
    
    DeleteFromTable = DeleteFromTable :+ DwQueryConfig + ("queryCustom" -> delete_query)
    DeleteFromTableConfig =DeleteFromTableConfig  :+ Config(DeleteFromTable(a))
    
    DWWrite = DWWrite :+ WriteToDWConfig + ("dbTable" -> table_list(a))
    DWWriteConfig = DWWriteConfig :+ Config(DWWrite(a))
  
  //df_new.write.mode(SaveMode.Append).sqlDB(createTableConfig)
    println("Reading Table from DB")
    val df_new=spark.read.sqlDB(ReadTablefromDBConfig(a))
    df_new.show()
    println("Deleting entries from DW table")
    sqlContext.sqlDBQuery(DeleteFromTableConfig(a))
    println("Writing table to DW")
    df_new.write.mode(SaveMode.Append).sqlDB(DWWriteConfig(a))
    println("TABLE WRITTEN TO DW !")
    
  /*}
  catch{
        case _: Throwable => println("CAUGHT AN EXCEPTION IN !",sheet_list(a))
        }*/
  
}
    val select_month_query="""
                      |select PARAM_VAL from WCC_PARAM where PARAM_NM='MON_ID' and ACTV_FLG='Y'
                      """.stripMargin

    var SelectMonthID = DwQueryConfig + ("queryCustom" -> select_month_query)
    var SelectMonthIDConfig =Config(SelectMonthID)

    var df_month = spark.read.sqlDB(SelectMonthIDConfig)

/********************************************************************************************/
/* This SP SP_STG_COMP_FLD_TRNS in DW layer is responsible for creating normalised version for FY for capex tables for power BI usage*/
/********************************************************************************************/

   val month_id_list = df_month.select("PARAM_VAL").collect().map(_(0)).toList 
   println(month_id_list(0))
   val call_sp_query="""
                    |exec SP_STG_COMP_FLD_TRNS %s
                   """.stripMargin.format(month_id_list(0))
    println(call_sp_query)

    var SPCall = DwQueryConfig + ("queryCustom" -> call_sp_query)
    var SPCallConfig =Config(SPCall)

  println("Calling Stored Procedure !")
  sqlContext.sqlDBQuery(SPCallConfig)
  println("Sp executed")
