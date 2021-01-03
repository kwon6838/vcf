from vcfmanagement.samplemanagment import VcfSamplemanagement
import os
from cyvcf2 import VCF, Writer
import sys
from starbase import Connection
import json
import base64
import time
import datetime

class HbaseIndex:
    default_host = "kdna.edison.re.kr"
    default_port = "9090"
    default_userId = "tuser"
    default_password = "tuser"
    default_secure = True
    default_verify_ssl = False
    default_retries = 3
    default_retry_deply = 10


    def __init__(self, host, port, user, password):
        if host == None:
            self.host = self.default_host
        if port == None:
            self.port = self.default_port
        if user == None:
            self.user = self.default_userId
        if password == None:
            self.password = self.default_password
        
        self.connection = Connection(self.host, self.port, self.user, self.password, secure=self.default_secure, 
                                        verify_ssl=self.default_verify_ssl, retries=self.default_retries, retry_delay=self.default_retry_deply)

    def __del__(self):
        print('finalizing')
        self.flush()


    def index(self, table_name, key_list, data_list):
        table = self.connection.table(table_name)
        self.batch = table.batch()
        if self.batch and len(key_list)>0 and len(data_list)>0:
            for i in range(len(data_list)):
                self.batch.update(key_list[i], data_list[i])
    
    def flush(self):
        response_return = self.batch.commit(finalize=True)
        print(response_return)
    
    def tables(self):
        return self.connection.tables()

    def close(self):
        print('OK')
        self.flush()

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


    def drop(self, table_name):
        table = self.connection.table(table_name)
        if table.exists():
            table.drop()



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
