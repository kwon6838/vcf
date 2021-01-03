from vcfmanagement.samplemanagment import VcfSamplemanagement
import os
from cyvcf2 import VCF, Writer
import sys
from starbase import Connection
import json


def main(target_dir):
    target_directory = target_dir
    fileList = search_vcf_file(target_directory)
    hconnection = HbaseConnection()
    hconnection.drop_talbe("kdna_variant")


def search_vcf_file(self, directory):
    search_file_list = []
    for(path, dir, files) in os.walk(directory):
        for filename in files:
            if filename.endswith(".vcf") or filename.endswith(".vcf.gz"):
                # print(path, " | ", filename)
                # print(dir)
                # print(os.path.join(path,filename))
                search_file_list.append(os.path.join(path,filename))
    return search_file_list



class HbaseConnection:

    def __init__(self):
        # self.connection = Connection(host="kdna.edison.re.kr", port="9090", user="kdna", password="kdna2020!!", secure=True, verify_ssl=False, retries=3, retry_delay=10)
        self.connection = Connection(host="kdna.edison.re.kr", port="9090", user="tuser", password="tuser", secure=True, verify_ssl=False, retries=3, retry_delay=10)

    def get_talbeList(self):
        return self.connection.tables()

    def create_table(self, table_name, *column_list):
        table = self.connection.table(table_name)
        if not table.exists():
            table.create(*column_list)
            # for item in table.columns():
            #     print(item)
            return True
        else:
            return False

    def add_columns(self, table_name, column_list):
        table = self.connection.table(table_name)
        if not table.exists():
            return False
        else:
            table.add_columns(column_list)
            # print(table.columns())
            return True
    # def batch_upload(self):

    def drop_talbe(self, table_name):
        table = self.connection.table(table_name)
        if table.exists():
            table.drop()
    
    def insert_batch(self, table_name, key_list, data_list):
        table = self.connection.table(table_name)
        batch = table.batch()
        if batch:
            for i in range(len(data_list)):
                # print(i, "    :     ", key_list[i], data_list[i] )
                batch.update(key_list[i], data_list[i])
            response_return = batch.commit(finalize=True)
            print(response_return)

    def search_data_rowkey(self, table_name, rowkey):
        table = self.connection.table(table_name)
        result = table.fetch(rowkey)
        # print(type(result))
        # print(len(result))
        # print(result)
        return result
    
    def search_data_rowkey_with_filter(self, table_name, start_rowkey, end_rowkey):
        table = self.connection.table(table_name)
        filter_configuration = {}
        filter_configuration["type"] = "FilterList"
        filter_configuration["op"] = "MUST_PASS_ALL"


        hbase_filter1 = {}
        hbase_filter1["type"] = "RowFilter"
        hbase_filter1["op"] = "GREATER_OR_EQUAL"
        comparator1 = {}
        comparator1["type"] = "BinaryComparator"
        # comparator["value"] = base64.b64encode(start_rowkey.encode("UTF_8"))
        comparator1["value"] = start_rowkey
        hbase_filter1["comparator"] = comparator1
        
        hbase_filter2 = {}
        hbase_filter2["type"] = "RowFilter"
        hbase_filter2["op"] = "LESS_OR_EQUAL"
        comparator2 = {}
        comparator2["type"] = "BinaryComparator"
        comparator2["value"] = end_rowkey
        hbase_filter2["comparator"] = comparator2

        filter_configuration["filters"] = []
        filter_configuration["filters"].append(hbase_filter1)
        # filter_configuration["filters"].append(hbase_filter2)

        print(json.dumps(filter_configuration))
        # f_string = '{"type": "RowFilter", "op": "GREATER_OR_EQUAL", "comparator": { "type": "BinaryComparator", "value": "MTNfMTAwMTE3MjM4X1RfRw=="} }'
        # f_string = '{"type": "FamilyFilter", "op": "EQUAL", "comparator": {"type": "ColumnPrefixFilter", "value": "SAMPLE:HG00566" } }'
        # result = table.fetch_all_rows(with_row_id=True, filter_string=f_string)
        result = table.fetch_all_rows(with_row_id=True, filter_string=json.dumps(filter_configuration))
        print("scan filter end...")
        return result

    def search_data_columnkey(self, table_name, columnkey):
        table = self.connection.table(table_name)
        hbase_filter = {}
        hbase_filter["type"] = "FamilyFilter"
        hbase_filter["value"] = "SAMPLE:"+columnkey

        f_string = '{"type": "FamilyFilter", "op": "EQUAL",  "comparator": { "type": "BinaryComparator", "value": "HG00566"} }'
        # f_string = '{"type": "FamilyFilter", "op": "EQUAL", "comparator": {"type": "ColumnPrefixFilter", "value": "SAMPLE:HG00566" } }'
        result = table.fetch_all_rows(with_row_id=True, filter_string=f_string)
        print(type(result))
        print(next(result))
        

    def alldata(self, table_name):
        table = self.connection.table(table_name)
        return table.fetch_all_rows(with_row_id=True, perfect_dict=True)



if __name__ == "__main__":
    main(sys.argv[1])