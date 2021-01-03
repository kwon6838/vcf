# main test python file
from vcfmanagement.samplemanagment import VcfSamplemanagement
import os
from cyvcf2 import VCF, Writer
# import happybase
# from starbase import Connection
import sys



def main(from_dir, target_dir):
    # current_directory = os.getcwd()
    # from_directory = current_directory+"/vcfmanagement/1000genome"
    # target_directory = current_directory+"/test1000g"
    from_directory = from_dir
    target_directory = target_dir
    
    # print("=====================\ntest")
    # vcf_read = VCF(current_directory+"/vcfmanagement/1000genome/"+"ALL.chr1.phase3_shapeit2_mvncall_integrated_v5a.20130502.genotypes.vcf.gz")
    # for variant in vcf_read:
    #     print(type(vcf_read.samples))
    #     print("genotype :", type(variant.genotypes))
    #     break
    # print("=====================\n")
    
    vcf = VcfSamplemanagement(from_directory, target_directory)
    # vcf.seperate_vcffile()
    # 타겟 디렉토리에 존재하는 모든 vcf 파일 업로딩
    # 타겟 디렉토리, 테이블명
    # vcf.drop_table("kdna_variant")
    # vcf.drop_table("KDNA_VARIANT")
    # vcf.set_vcf_columns("kdna_variant")
    # vcf.upload_batch_to_hbase(target_directory, "kdna_variant")

    # connection = happybase.Connection("150.183.247.84")
    # connection.open()
    # table = connection.table("kdna_variant")
    # print("connection successs")
    # for key, data in table.scan():
    #     print(key, data)

    # vcf = VcfSamplemanagement(from_directory, target_directory)
    # vcf.set_vcf_columns("KDNA_VARIANT")
    # vcf.upload_batch_to_hbase(target_directory, "KDNA_VARIANT")
    # vcf.bulk_download_from_hbase_rowkey_filter("kdna_variant", "1-900000000-A-A", "1-900000000-A-A")
    vcf.bulk_download_from_hbase_rowkey_filter("kdna_variant", "1-*", "1-")
    # vcf.bulk_download_from_hbase_sample("KDNA_VARIANT", "HG")
    # vcf.bulk_download_from_hbase("kdna_variant")

if __name__ == "__main__":
    main(sys.argv[1], sys.argv[2])
