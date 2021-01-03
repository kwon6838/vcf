# import pysam
# from pysam import VariantFile
# from pysam import VariantHeader
# from pysam import VariantHeaderRecord
# from pysam import VariantRecord
from cyvcf2 import VCF, Writer
# from cyvcf2.cyvcf2 import HREC
import time
import os
import datetime
import gzip
import shutil
# from vcfmanagement.hbase_connection_phoenix import Hbase_phoenix_connection
from vcfmanagement.hbase_connection_startbase import HbaseConnection
import json

class VcfSamplemanagement:
    from_directory = ""
    target_directory = ""
 

    def __init__(self, from_directory, target_directory):
        self.from_directory = from_directory
        self.target_directory = target_directory
        self.connection = HbaseConnection()

    
    def search_vcf_file(self, directory):
        search_file_list = []
        for(path, dir, files) in os.walk(directory):
            for filename in files:
                if filename.endswith(".vcf") or filename.endswith(".vcf.gz"):
                    # print(path, " | ", filename)
                    search_file_list.append(path+"/"+filename)
        return search_file_list
    

    def seperate_vcffile(self):
        # start = time.time()
        file_list = self.search_vcf_file(self.from_directory)
        for file in file_list:    
            vcf_read = VCF(file)
            samples = vcf_read.samples
            chromosome_num = ""
            for variant in vcf_read:
                chromosome_num = variant.CHROM
                break

            for sample in samples:
                start = time.time()
                # print(sample, "file write start...  ", start)
                try:
                    if not(os.path.isdir(self.target_directory)):
                        os.makedirs(os.path.join(self.target_directory))
                    if not(os.path.isdir(self.target_directory+"/"+sample)):
                        os.makedirs(os.path.join(self.target_directory+"/"+sample))
                except OSError as e:
                    print("Failed to create directory!!!!!", e)
                    raise
                        
                filepath = os.path.join(self.target_directory+"/"+sample, chromosome_num+"-"+sample+".vcf")
                index = 0
                while os.path.exists(filepath):
                    index = index+1
                    filepath = os.path.join(self.target_directory+"/"+sample, chromosome_num+"-"+sample+str(index)+".vcf")

                out_read_vcf = VCF(file, samples=[sample])
                write_file = Writer(filepath, out_read_vcf)
                
                for variant in out_read_vcf:
                    if chromosome_num == "Y":
                        if not variant.genotypes[0][0] == 0:
                            write_file.write_record(variant)
                                 
                    elif not (variant.genotypes[0][0] == 0 and variant.genotypes[0][1] == 0):
                        write_file.write_record(variant)
                    
                write_file.close()
                out_read_vcf.close()

                with open(filepath, "rb") as f_in:
                    with gzip.open(filepath+".gz", "wb") as f_out:
                        shutil.copyfileobj(f_in, f_out)

                os.remove(filepath)
                sec = time.time()- start
                print(sample+" write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))
                break
            vcf_read.close()

    def set_vcf_columns(self, table_name):
        self.connection.drop_talbe(table_name)
        column_list = ['SAMPLE']
        # column_list = ['CHORM', 'POS', 'ID', 'REF', 'ALT', 'QUAL', 'FILTER', 'INFO', 'FORMAT', 'SAMPLE']
        self.connection.create_table(table_name, *column_list)

    def drop_table(self, table_name):
        self.connection.drop_talbe(table_name)
 
    # def create_table(self, table_name):
    #     self.connection.create_table(table_name)

    def upload_batch_to_hbase(self, target_directory, table_name):
        file_list = self.search_vcf_file(target_directory)
        all_start = time.time()
        
        for file in file_list:
            start = time.time()
            vcf_read = VCF(file)
            # for header in vcf_read.header_iter():
            #     if header.info().get("HeaderType") == "INFO":
            #         info_list.append(header.info().get("ID"))
            variant_index = 0
            insert_dic = {}
            sample_dic = {}
            row_key_list = []
            bulk_upload_list = []
            for variant in vcf_read:
                row_key = variant.CHROM+"-"+str(variant.start)+"-"+str(variant.REF)
                for alt in variant.ALT:
                    row_key = row_key+"-"+alt
                
                index =0
                for sample_id in vcf_read.samples:
                    for variant_format in variant.FORMAT:
                        # sample_dic[sample_id] 
                        tmp_data = {}
                        if variant_format == "GT" :
                            if variant.CHROM == "Y":
                                tmp_data[variant_format] = str(variant.genotypes[index][0])
                            else:
                                tmp_data[variant_format] = str(variant.genotypes[index][0])+ '|' + str(variant.genotypes[index][1])
                            sample_dic[sample_id] = json.dumps(tmp_data)
                    index = index+1

                insert_dic["SAMPLE"] = sample_dic
                row_key_list.append(row_key)
                bulk_upload_list.append(insert_dic)
                # print("current len ", len(row_key_list))
                if variant_index % 5000 == 0 and not variant_index == 0 and len(bulk_upload_list) > 0:
                    self.connection.insert_batch(table_name=table_name, key_list=row_key_list.copy(), data_list=bulk_upload_list.copy())
                    print(file, "  :: bulk uploading... ", variant_index)
                    insert_dic = {}
                    sample_dic = {}
                    row_key_list = []
                    bulk_upload_list = []
                variant_index = variant_index + 1
            
            self.connection.insert_batch(table_name=table_name, key_list=row_key_list, data_list=bulk_upload_list)
            print(file, "file last bulk uploading... ", variant_index)
            insert_dic = {}
            sample_dic = {}
            row_key_list = []
            bulk_upload_list = []

            sec = time.time()- start
            print(file, " write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))
            # break
        sec = time.time() - all_start
        print(" all file write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))

    def bulk_download_from_hbase_rowkey(self, table_name, rowkey):
        result = self.connection.search_data_rowkey(table_name, rowkey)
        # print(json.dumps(result))

        try:
            if not(os.path.isdir(os.getcwd()+"/result")):
                os.makedirs(os.path.join(os.getcwd()+"/result"))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to create directory!!!!!")
                raise
        index =0
        filename = os.getcwd()+"/result/"+rowkey+".json"
        while os.path.exists(filename):
            index = index+1
            filename = os.path.join(os.getcwd()+"/result", rowkey+str(index)+".json")
            # filepath = os.path.join(self.target_directory+"/"+sample, sample+index+".vcf")
        with open(filename, 'w') as out_file:
            json.dump(result, out_file)

    def bulk_download_from_hbase_rowkey_filter(self, table_name, s_rowkey, e_rowkey):
        start = time.time()
        result = self.connection.search_data_rowkey_with_filter(table_name, s_rowkey, e_rowkey)
        try:
            if not(os.path.isdir(os.getcwd()+"/result")):
                os.makedirs(os.path.join(os.getcwd()+"/result"))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to create directory!!!!!")
                raise
        index =0
        filename = os.getcwd()+"/result/"+s_rowkey+":"+e_rowkey+".json"
        while os.path.exists(filename):
            index = index+1
            filename = os.path.join(os.getcwd()+"/result", s_rowkey+":"+e_rowkey+str(index)+".json")
            # filepath = os.path.join(self.target_directory+"/"+sample, sample+index+".vcf")
        result_list = []
        for re in result:
            result_list.append(re)
            print(re)
        with open(filename, 'w') as out_file:
            json.dump(result_list, out_file)
        
        sec = time.time()- start
        print(filename +" write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))

    def bulk_download_from_hbase_sample(self, table_name, sample_id):
        self.connection.search_data_columnkey(table_name, sample_id)

    # def bulk_download_from_hbase_gt(self, table_name, gt_value):
    #     self.connection.get_data()

    def bulk_download_from_hbase(self, table_name):
        start = time.time()
        result = self.connection.alldata(table_name)
        print(type(result))
        print(result.__sizeof__)
        print(result)
        sec = time.time()- start
        print(" fetch all row...", time.strftime("%H:%M:%S", time.gmtime(sec)))
        try:
            if not(os.path.isdir(os.getcwd()+"/result")):
                os.makedirs(os.path.join(os.getcwd()+"/result"))
        except OSError as e:
            if e.errno != errno.EEXIST:
                print("Failed to create directory!!!!!")
                raise
        file_index = 0
        result_index = 0
        # filename = os.getcwd()+"/result/"+table_name+".json"
        result_list = []
        for row in result:
            result_list.append(row)
            # print("current row: ", row, "index : ", result_index)
            result_index = result_index + 1
            # print("index : ", result_index, result_index%1000==0)
            if result_index>0 and result_index%10000==0:
                start = time.time()
                print("current row: ", row, "index : ", result_index)
                filename = os.path.join(os.getcwd()+"/result", table_name+"_"+str(file_index)+".json")
                while os.path.exists(filename):
                    file_index = file_index+1
                    filename = os.path.join(os.getcwd()+"/result", table_name+"_"+str(file_index)+".json")
                print(filename, "result is writing.", result_index)
                with open(filename, 'w') as out_file:
                    json.dump(result_list, out_file)
                sec = time.time()- start
                print("write file fetch all row...", file_index, time.strftime("%H:%M:%S", time.gmtime(sec)))

                result_list = []
                break

"""
    def bulk_upload_to_hbase(self, target_directory, table_name):
        file_list = self.search_vcf_file(target_directory)
        hbase_connection = Hbase_phoenix_connection()
        hbase_connection.drop_table(table_name)
        sql = self.create_sql(target_directory, table_name)
        # print(sql)
        hbase_connection.create_table(sql)
        bulk_upload_list = []
        # sql = "UPSERT INTO "+ table_name
        for file in file_list:
            insert_dic = {}
            vcf_read = VCF(file)
            info_list=[]
            for header in vcf_read.header_iter():
                if header.info().get("HeaderType") == "INFO":
                    info_list.append(header.info().get("ID"))

            for i, variant in enumerate(vcf_read):
                insert_dic["CHROM"] = variant.CHROM
                insert_dic["POS"] = variant.start
                insert_dic["ID"] = variant.ID
                insert_dic["REF"] = variant.REF
                insert_dic["ALT"] = variant.ALT[0]
                insert_dic["QUAL"] = variant.QUAL
                insert_dic["FILTER"] = variant.FILTER
                                
                
                for info in info_list:
                    try:
                        insert_dic["INFO_"+info] = variant.INFO[info]
                    except KeyError:
                        pass
                        # print("no info data : ", info)
                for sample_id in vcf_read.samples:
                    insert_dic["\"SAMPLE\".\"key\""] = sample_id
                    insert_dic["\"SAMPLE\".\"value\""] = str(variant.genotypes)
                
                if insert_dic.get("INFO_MULTI_ALLELIC"):
                    insert_dic["INFO_"+info] = "TRUE"
                else: 
                    insert_dic["INFO_"+info] = "FALSE"
                
                sql_change_list = []
                if insert_dic["INFO_MULTI_ALLELIC"] == "TRUE":
                    len_sql = 0
                    for key, value in insert_dic.items():
                        if type(value) is tuple:
                            if len(value) > len_sql :
                                len_sql = len(value)
                    print(len_sql)
                
                    for index in range(0, len_sql-1):
                        insert_dic_copy = insert_dic.copy()
                        for key, value in insert_dic_copy.items():
                            if type(value) is tuple:
                                try:
                                    insert_dic_copy[key] = value[index]
                                    print("\n\n\ntest index : ", key, value, index, value[index], insert_dic_copy[key])
                                except IndexError:
                                    pass
                        sql_change_list.append(insert_dic_copy)
                else:
                    sql_change_list.append(insert_dic)
                
                for list_item in sql_change_list:
                    bulk_upload_list.append(self.upsert_sql(list_item, table_name))

                # sql = self.upsert_sql(insert_dic, table_name)
                # print(sql)
                # bulk_upload_list.append(sql)
                if len(bulk_upload_list) > 10000 or vcf_read.__sizeof__()-1 == i:
                    start = time.time()
                    hbase_connection.bulk_upsert(bulk_upload_list)
                    bulk_upload_list[:]
                    sec = time.time() - start
                    print("bulk upload 10000 write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))
                

    def upsert_sql(self, insert_dic, table_name):
        sql = "UPSERT INTO "+ "\""+table_name.upper()+"\"" + " ("
        key_list = ""
        value_list = ""
        
        for key, value in insert_dic.items():
            # key_list = key_list + "\""+key +"\""+  ", "
            key_list = key_list + key +  ", "
            if type(value) == str or type(value) == bool:
                value_list = value_list + "\'"+str(value)+"\'" + ", "
            elif value == None:
                value_list = value_list + "\'None\'" + ", "
            else:
                value_list = value_list + str(value) + ", "
        if key_list.endswith(", "):
            key_list = key_list[:-2]
        if value_list.endswith(", "):
            value_list = value_list[:-2]
        sql = sql + key_list + ") VALUES( " + value_list + " )"
        return sql
                
    

        

    def create_sql(self, target_directory, table_name):
        # cyvcf2.cyvcf2.HREC
        file_list = self.search_vcf_file(target_directory)
        sql = "CREATE TABLE IF NOT EXISTS " + table_name
        vcf_info = {}
        vcf_format = {}

        for file in file_list:
            vcf_read = VCF(file)
            # print(vcf_read.raw_header)
            for header in vcf_read.header_iter():
                # print(type(header))
                # print(header.info())
                if header.info().get("HeaderType") == "INFO":
                    info_id = header.info().get("ID")
                    info_type = header.info().get("Type")
                    if info_type == "Float":
                        info_type = "FLOAT"
                    elif info_type == "String":
                        info_type = "VARCHAR(50)"
                    elif info_type == "Integer":
                        info_type = "INTEGER"
                    elif info_id == "INFO_MULTI_ALLELIC":
                        info_type == "BOOLEAN"
                    else:
                        info_type = "VARCHAR(50)"
                    
                    vcf_info["INFO_"+info_id] = info_type
                elif header.info().get("HeaderType") == "FORMAT":
                    vcf_format[header.info().get("ID")] = "VARCHAR(100)"
        vcf_read.close()
        sql = sql + "( CHROM VARCHAR(10) NOT NULL, POS UNSIGNED_LONG NOT NULL, ID VARCHAR(500), REF VARCHAR(500) NOT NULL, ALT VARCHAR(500) NOT NULL, QUAL FLOAT, FILTER VARCHAR(100), "
        for key, value in vcf_info.items():
            sql = sql + key +" "+ value + ", "
        for key, value in vcf_format.items():
            sql = sql + key +" "+ value + ", "

        sql = sql + "\"SAMPLE\".\"key\" VARCHAR(100), \"SAMPLE\".\"value\" VARCHAR(500), CONSTRAINT KDNA_PK PRIMARY KEY(CHROM, POS, REF, ALT))"
        print(sql)
        return sql

            

    # def hbase_write(self, tableName, rowName, rowContentsInDict):
    #     fileList = self.search_vcf_file(target_directory)
    #     hbase_connection = Hbase_phoenix_connection()










###########################################################################
# pysam vcf file management
    def create_sample_files(self):
        vcf_in = vcf_in = VariantFile(self.input_filepath, 'r')
        vcf_out_name = []
        vcf_out_header= []
        index = 0
        for sample in vcf_in.header.samples:
            vcf_out_name.append(sample + ".vcf")
            header = VariantHeader()
            for record in vcf_in.header.records:
                header.add_record(record)
                # print(type(record))
            header.add_sample(sample)
            vcf_out_header.append(header)
            try:
                if not(os.path.isdir(self.directory)):
                    os.makedirs(os.path.join(self.directory))
                if not(os.path.isdir(self.directory+"/"+sample)):
                    os.makedirs(os.path.join(self.directory+"/"+sample))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    print("Failed to create directory!!!!!")
                    raise
            index = index + 1

    def search_vcf_sample_info(self):
        vcf_in = vcf_in = VariantFile(self.input_filepath, 'r')
        start = time.time()
        for variant in vcf_in.fetch("HG00096"):
            print(variant)
        print(" file search... : "+ time.time()-start)

    def append_vcf_sample_info(self):
        vcf_in = vcf_in = VariantFile(self.input_filepath, 'r')
        start = time.time()
        for variant in vcf_in.fetch():
            for sample in variant.samples:
                sample_id = sample
                print(sample_id)
                for sample_type in variant.samples[sample_id]:
                    filepath = os.path.join(self.directory+"/"+sample, sample_id+".vcf")
                    write_file = VariantFile(filepath, 'wa')
                    record = write_file.write_file.new_record(contig=variant.contig, start=variant.stop, stop=variant.pos, alleles=variant.alleles, filter=variant.filter)
                    if not sample_type == 'GT':
                        print("sample type : ", sample_type)
                        record.sample[sample_id][sample_type] = variant[sample_id][sample_type]
                    if sample_type == 'GT':
                        sample_gt = variant.samples[sample_id]['GT']
                        if not(sample_gt[0]==0 and sample_gt[1]==0):
                            record.samples[sample_id]['GT'] = sample_gt
                            for info in variant.info.items():
                                record.info.__setitem__(info[0], info[1])
                            record.id = variant.id
                            record.qual = variant.qual
                            write_file.write(record)
                        else:
                            break
            print(sample_id, " file write... : "+ time.time()-start)
                
    def search_vcf_sample(self, sample_id):
        vcf_in = VariantFile(self.input_filepath, 'r')
        samples = vcf_in.header.samples
        vcf_in.close()
        print(type(samples))
        samples.remove(sample_id)
        vcf_search_in = VariantFile(self.input_filepath, 'r', drop_samples=samples)
        for variant in vcf_search_in:
            print(variant)

    def seperate_vcffile_by_sample(self):
        vcf_in = vcf_in = VariantFile(self.input_filepath, 'r')
        vcf_out_name = []
        vcf_out_header= []
        index = 0
        start = time.time()
        for sample in vcf_in.header.samples:
            vcf_out_name.append(sample + ".vcf")
            header = VariantHeader()
            for record in vcf_in.header.records:
                header.add_record(record)
                # print(type(record))
            header.add_sample(sample)
            vcf_out_header.append(header)
            try:
                if not(os.path.isdir(self.directory)):
                    os.makedirs(os.path.join(self.directory))
                if not(os.path.isdir(self.directory+"/"+sample)):
                    os.makedirs(os.path.join(self.directory+"/"+sample))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    print("Failed to create directory!!!!!")
                    raise
            filepath = os.path.join(self.directory+"/"+sample, vcf_out_name[index])
            write_file = VariantFile(filepath, 'w', header=header)
            # write_record = []
            for variant in vcf_in.fetch():
                record = write_file.new_record(contig=variant.contig, start=variant.stop, stop=variant.pos, alleles=variant.alleles, filter=variant.filter)
                sample_id = sample
                for sample_type in variant.samples[sample_id]:
                    if not sample_type == 'GT':
                        print("sample type : ", sample_type)
                        record.sample[sample_id][sample_type] = variant[sample_id][sample_type]
                    elif sample_type == 'GT':
                        sample_gt = variant.samples[sample_id]['GT']
                        if not(sample_gt[0]==0 and sample_gt[1]==0):
                            record.samples[sample_id]['GT'] = sample_gt
                            for info in variant.info.items():
                                record.info.__setitem__(info[0], info[1])
                            record.id = variant.id
                            record.qual = variant.qual
                            write_file.write(record)
                            # write_record.append(record)
                        else:
                            break
                print(sample_id, " file write... : "+ time.time()-start)
                write_file.close()
                index = index+1
        vcf_in.close()
    
    def seperate_vcffile_by_sample_memory(self):
        vcf_in = vcf_in = VariantFile(self.input_filepath, 'r')
        vcf_out_header = {}
        vcf_out_record = {}
        start = time.time()        
        
        #set heder data by each sample data
        for sample in vcf_in.header.samples:
            header = VariantHeader()
            for record in vcf_in.header.records:
                header.add_record(record)
            header.add_sample(sample)
            vcf_out_header[sample] = header
            vcf_out_record[sample] = []
            try:
                if not(os.path.isdir(self.directory)):
                    os.makedirs(os.path.join(self.directory))
                if not(os.path.isdir(self.directory+"/"+sample)):
                    os.makedirs(os.path.join(self.directory+"/"+sample))
            except OSError as e:
                if e.errno != errno.EEXIST:
                    print("Failed to create directory!!!!!")
                    raise
    


        #record data setting by each sample data
        for variant in vcf_in.fetch():
            for sample in variant.samples:
                sample_id = sample
                filepath = os.path.join(self.directory+"/"+sample_id+"/"+sample_id+".vcf")
                header = vcf_out_header[sample_id]
                write_file = VariantFile(filepath, 'w', header=header)
                record = write_file.new_record(contig=variant.contig, start=variant.stop, stop=variant.pos, alleles=variant.alleles, filter=variant.filter)
                for sample_type in variant.samples[sample_id]:
                    if not sample_type == 'GT':
                        print("sample type : ", sample_type)
                        record.sample[sample_id][sample_type] = variant[sample_id][sample_type]
                    elif sample_type == 'GT':
                        sample_gt = variant.samples[sample_id]['GT']
                        if not(sample_gt[0]==0 and sample_gt[1]==0):
                            record.samples[sample_id]['GT'] = sample_gt
                            for info in variant.info.items():
                                record.info.__setitem__(info[0], info[1])
                            record.id = variant.id
                            record.qual = variant.qual
                            vcf_out_record[sample_id].append(record)
                            print(record)
                write_file.close()
        
        #set file data and write vcf files
        for sample in vcf_in.header.samples:
            print("Starting ")
            filepath = os.path.join(self.directory+"/"+sample_id+"/"+sample_id+".vcf")
            header = vcf_out_header[sample_id]
            write_file = VariantFile(filepath, 'w', header=header)
            for record in vcf_out_record[sample]:
                write_file.write(record)
            write_file.close()
            print(sample_id, " file write... : "+ time.time()-start)
        vcf_in.close()
"""