import time
ini_time = time.time()
from loguru import logger
logger.add(f"logs/ETL_{time.ctime().replace(' ','_').replace(':','_')}.log")
from transformers import AutoTokenizer
from optimum.pipelines import pipeline as optpipeline

onnx_model_path = 'model/model.onnx'
tokenizer = AutoTokenizer.from_pretrained(onnx_model_path)
onnx_pipeline = optpipeline('text-classification',
                            model=onnx_model_path,
                            truncation=True,
                            max_length=256,
                            accelerator='ort')
logger.info(f'onnx model was loaded in {int(time.time()-ini_time)} seconds.')                          

import os, sys
#import numpy as np
import pandas as pd
import re,math

from pyspark.sql.window import Window
from sparknlp.base import DocumentAssembler
from sparknlp.annotator import  SentenceDetector,RegexMatcher
from pyspark.ml import Pipeline as sparkPipeline
import pyspark.sql.functions as F
from spark_start import spark_start

spark =  spark_start("NHPI_ETL")                         

url = "jdbc:sqlserver://"
logger.info(f'spark session started in {int(time.time()-ini_time)} seconds.')


###### STEP 1: Full text search ######

step_1_time = time.time()
with open('resources/NHPI_keywords.txt','r') as f:
    lines = [','.join(x.replace('\n','').split()) for x in f.readlines()]

q = []
for k in lines:
    if k.find(',') > 0:
        keyword = 'near('+ k +')'
    else:
        keyword = k
    q.append( f'''
            Select   SID 
            from XXX.XXX.FullTextSearch('{keyword}') 
        ''')

q_full_text_search = ''' select distinct SID from ( ''' + ''' union\n '''.join(q) + ''' ) as a'''

step_1_table = "[Step_1_KeyTerm_FullTextSearch] "
spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option('query', q_full_text_search) \
        .option("schemaCheckEnabled", 'false') \
        .load()\
    .write\
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", step_1_table) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

step_1_df_count = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option('dbtable', step_1_table) \
        .option("schemaCheckEnabled", 'false') \
        .load().count()
logger.info(f'Full text search took {int(time.time()-step_1_time)} seconds, and returned {step_1_df_count} distinct documents.')

###### STEP 2: Get new documents that were not processed yet. ######

q_step2 = '''
    Select xxx,xxx,xxx
	from 
		xxxx
'''

step_2_table = "[Step_2_KeyTerm_FullTextSearch_docs_to_process] "
spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
        .option("url", url) \
        .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
        .option('query', q_step2) \
        .option("schemaCheckEnabled", 'false') \
        .load()\
    .write\
    .format("com.microsoft.sqlserver.jdbc.spark") \
    .mode("overwrite") \
    .option("url", url) \
    .option("dbtable", step_2_table) \
    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
    .save()

max_rowid = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
                .option("url", url) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .option('dbtable', step_2_table) \
                .option("schemaCheckEnabled", 'false') \
                .load().count()	

logger.info(f'New documents generation took {int(time.time()-step_1_time)} seconds, and returned {max_rowid} distinct documents.')


###### STEP 3: Batch processing. ######

#### Get dim table of matched terms to categories.  ####
dim = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
            .option("url", url) \
            .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
            .option('dbtable', "[term_category_mappings]") \
            .option("schemaCheckEnabled", 'false') \
            .load().toPandas()
dim['normlized_term'] = dim['norm_term']
dim = dim[['normlized_term', 'Category1', 'Category2', 'Partial']]

id2label = {0:'Other',
1: 'Patient',
2: 'Direct ancestor',
3: 'Patient native speaker',
4: 'Patient lived or lives',
5: 'Patient_negated',
6: 'Patient was born/from',
7: 'Patient speaks'}    


#### Regex to match terms sparknlp pipeline    
documentAssembler = DocumentAssembler() \
    .setInputCol('Text') \
    .setOutputCol("document")

sentencer = SentenceDetector()\
    .setInputCols(["document"]) \
    .setOutputCol("sentence")\
    .setCustomBounds(['\n\n\n','\n\n','\r\n\r\n\r\n','\r\n\r\n','\r\n \r\n'])\
    .setSplitLength(200)
            
regex_matcher_doc = RegexMatcher() \
    .setInputCols('sentence') \
    .setOutputCol('reg_matches') \
    .setExternalRules(path='resources/nhpi.txt', delimiter='@') \
    .setStrategy("MATCH_ALL") \
    .setLazyAnnotator(False)

  
doc_pipeline = sparkPipeline(stages=[
    documentAssembler,sentencer,regex_matcher_doc
])

empty_df = spark.createDataFrame([['']]).toDF("Text")
doc_Model = doc_pipeline.fit(empty_df)    

#### Exclusion sparknlp pipeline
documentAssembler2 = DocumentAssembler() \
    .setInputCol('context') \
    .setOutputCol("document")
            
regex_matcher_2 = RegexMatcher() \
    .setInputCols('document') \
    .setOutputCol('exclusion_matches') \
    .setExternalRules(path='resources/nhpi_exclusions.txt', delimiter='@') \
    .setStrategy("MATCH_FIRST") \
    .setLazyAnnotator(False)  
    
doc_pipeline2 = sparkPipeline(stages=[
    documentAssembler2,regex_matcher_2
])

empty_df_2 = spark.createDataFrame([['']]).toDF("context")
doc_Model_2 = doc_pipeline2.fit(empty_df_2) 

logger.info('sparknlp pipelines created.')


to_process_table= '[Step_2_KeyTerm_FullTextSearch_docs_to_process]'	
min_rowid = 1  

logger.info(f'Notes {min_rowid} - {max_rowid} to be processed.')
output_table = "[batch_output]"

batch_size = 10000
max_processed_rowid = min_rowid
last_failed = []

while max_processed_rowid - 1 < max_rowid:
    #### If a batch failed twice, skip and go to the next batch.
    if len(last_failed) > 2 and max_processed_rowid == last_failed[-2]:
        max_processed_rowid = max_processed_rowid + batch_size
        continue
        
    try:
        batch_start_time = time.time()
        batch_start_did = max_processed_rowid 
        batch_end_did = batch_start_did + batch_size
        
        query_template = '''SELECT A.*, B.Text 
        FROM {} A with(nolock) 
        left join xxx B with(nolock) 
        on A.SID=B.SID
        WHERE rn>={} AND rn<{} '''

        batch_query=query_template.format(to_process_table, batch_start_did, batch_end_did)

        logger.info(f'Read data: {batch_start_did} -- {batch_end_did - 1} ')

        batch_sdf = spark.read.format("com.microsoft.sqlserver.jdbc.spark") \
                    .option("url", url) \
                    .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                    .option('dbtable', f'({batch_query}) as query_table') \
                    .option("schemaCheckEnabled", 'false') \
                    .option('partitionColumn', 'rn') \
                    .option('numPartitions', 32) \
                    .option('lowerBound', batch_start_did) \
                    .option('upperBound', batch_end_did) \
                    .option('tableLock','false')\
                    .load().cache()
        doc_df = doc_Model.transform(batch_sdf).cache()

        df_matches = doc_df.select( 'SID', 'Text','sentence', F.explode('reg_matches').alias('match'))\
            .select('SID', 'sentence','Text',
                    F.col('match')['result'].alias('matched_term'),
                    F.col('match')['metadata']['sentence'].cast('int').alias('matched_sent'),
                    F.col('match')['begin'].cast('int').alias('span_start'),
                    F.col('match')['end'].cast('int').alias('span_end')
                    )\
            .dropDuplicates([ 'SID','span_start','span_end'])\
            .cache()
        matched_doc_n =  df_matches.select('SID').distinct().count()   
        logger.info(f"There are {matched_doc_n} unique documetns that have matched terms.")
        
        if matched_doc_n==0:
            max_processed_rowid = batch_end_did 
            continue

        df_context = df_matches.select(
                     'SID',
                     'matched_sent',
                     'span_start',
                     'span_end',
                     F.trim(F.regexp_replace('matched_term', r'\s+', ' ')).alias('matched_term'),
                     F.explode('sentence').alias('sent')) \
            .select(
                    'SID', 
                    'matched_sent',
                     'span_start',
                     'span_end',
                     'matched_term',
                     F.regexp_replace( F.col('sent')['result'],r'\s+',' ').alias('original_sentence'),
                     F.col('sent')['metadata']['sentence'].cast('int').alias('sent_num'),
                    ) \
            .filter("matched_sent-sent_num<=2 AND matched_sent-sent_num>=-2") 

        df_context2 = df_context\
            .withColumn('InstanceID',
                F.concat(F.col('SID'), F.lit('_'),F.col('span_start'), F.lit('_'),F.col('span_end')) )\
            .withColumn("marker_sent", F.when(F.col('sent_num')==F.col('matched_sent'),
                        F.expr("regexp_replace(original_sentence,matched_term, concat(' [TERM] ', matched_term,' [/TERM] '))"))\
                        .otherwise(F.col('original_sentence')))\
            .orderBy('InstanceID','matched_sent','sent_num')\
            .groupBy('InstanceID','matched_sent')\
            .agg(F.first('SID').alias('SID'),
                 F.first('matched_term').alias('matched_term'),
                 F.first('span_start').alias('span_start'),
                 F.first('span_end').alias('span_end'),
                 F.concat_ws("\n", F.collect_list('marker_sent')).alias('marker_context'),
                 F.concat_ws(' ', F.collect_list('original_sentence')).alias('context')
                 )\
            .orderBy('InstanceID','matched_sent')\
            .withColumn('normlized_term',
                        F.trim(F.regexp_replace(F.regexp_replace(F.lower(F.col('matched_term')),',',''),                        r'/|-',' ')
                            )
                        )\
            .dropDuplicates(['InstanceID'])\
            .cache()
       
        
        doc_df_2 = doc_Model_2.transform(df_context2).cache()
        
     
        to_predict = doc_df_2.withColumn("excluded_term",F.when(F.size(F.col('exclusion_matches'))==0,'None')\
                                     .otherwise(F.element_at(F.col('exclusion_matches.result'),1)))\
                         .withColumn("excluded_category",F.when(F.size(F.col('exclusion_matches'))==0,'None')\
                                     .otherwise(F.element_at(F.col('exclusion_matches.metadata'),1)['identifier']))\
                        .select('InstanceID', 
                                'SID', 
                                'matched_term',
                                'normlized_term',
                                'span_start',
                                'span_end',
                                'excluded_term',
                                'excluded_category',
                                'marker_context').toPandas().dropna()         
        
        to_predict = to_predict.merge(dim,how='left',on='normlized_term')
      
        pred = onnx_pipeline(to_predict.marker_context.to_list())
        df_pred = pd.DataFrame(pred)
        
        to_predict['prediction'] = df_pred['label'].apply(lambda x: id2label[int(x.replace('LABEL_',''))])
        to_predict['probability'] = df_pred['score']
        to_predict['Category2'] = to_predict['Category2'].fillna('None')
        
        logger.info(f'{to_predict.shape[0]} instances were predicted.')
        
        spark.createDataFrame(to_predict).write\
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("append") \
                .option("url", url2) \
                .option("dbtable", output_table) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .save()
                
        processed_batch = "[processed_batch]"
        batch_sdf.select('patienticn',
                          'SID',
                          'patientsid',
                          'sta3n',
                          'ETLDate')\
            .write\
                .format("com.microsoft.sqlserver.jdbc.spark") \
                .mode("append") \
                .option("url", url) \
                .option("dbtable", processed_batch) \
                .option("driver", "com.microsoft.sqlserver.jdbc.SQLServerDriver") \
                .save()
         
        spark.catalog.clearCache()
        logger.info(f'Processed {batch_start_did} to {batch_end_did-1} and saved to database in {int(time.time() - batch_start_time)} seconds.')
        max_processed_rowid = batch_end_did 
        
    except Exception as e:
        logger.error(f'{str(e)}')
        max_processed_rowid = batch_start_did
        last_failed.append(max_processed_rowid)

## Save processed batch into all_tiu_fulltext


logger.info(f'Processed {max_rowid} notes and saved to database in {int((time.time() - ini_time)/3600)} hours.')    	