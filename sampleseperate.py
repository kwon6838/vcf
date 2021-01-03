import os
from cyvcf2 import VCF, Writer
import sys
import time
import os
import datetime
import gzip
import shutil



def main(sysarg):
    sample_seperate = samplemagement(sysarg[1], sysarg[2])
    if os.path.isfile(sysarg[1]):
        sample_seperate.file_seperate(sysarg[1], sysarg[2], sysarg[3])
        print("test job submit ended...")
    elif os.path.isdir(sysarg[1]):
        print("submitted job create...")
        job_list = sample_seperate.job_script_create(sample_seperate.search_vcf_file(sysarg[1]))
        for job in job_list:
            # print('sbatch '+job)
            os.system('sbatch '+job)
        print("job submitted.. ", len(job_list) )
    else:
        sys.stderr.write("argument is no corrects...\n")
        return



class samplemagement:
    from_directory = ""
    target_directory = ""
    from_directory_name = ""
    target_directory_name = ""
    
    def __init__(self, from_dir, target_dir):
        if from_dir.endswith('/'):
            self.from_directory = from_dir[:-1]
            self.from_directory_name = from_dir[:-1].split('/')[-1]
        else:
            self.from_directory = from_dir
            self.from_directory_name = from_dir.split('/')[-1]
        
        if target_dir.endswith('/'):
            self.target_directory = target_dir[:-1]
            self.target_directory_name = target_dir[:-1].split('/')[-1]
        else:
            self.target_directory = target_dir
            self.target_directory_name = self.target_directory.split('/')[-1]

        # print("init [1] : ", self.from_directory)
        # print("init [2] : ", self.from_directory_name)
        # print("init [3] : ", self.target_directory)
        # print("init [4] : ", self.target_directory_name)

    def search_vcf_filename(self, directory):
        search_file_list = []
        for(path, dir, files) in os.walk(directory):
            for filename in files:
                if filename.endswith(".vcf") or filename.endswith(".vcf.gz"):
                    # print(path, " | ", filename)
                    # from_directory_name/middlepath/filename
                    full_path = path+filename
                    # print("test full path : ", full_path)
                    # print(self.from_dircetory_name)
                    # print(full_path.split(self.from_dircetory_name)[-1])
                    search_file_list.append(full_path.split(self.from_dircetory_name)[-1])
        return search_file_list

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

    def job_script_create(self, file_list):
        #job script create
        # targetdir / filename / job.sh
        # dir_list = []
        
        # try:
        #     if not(os.path.isdir(self.target_directory)):
        #         os.makedirs(os.path.join(self.target_directory))
            
        #     for file in file_list:
        #         if not(os.path.isdir(self.target_directory+"/jobs/"+file.split("/")[-1])):
        #             os.makedirs(os.path.join(self.target_directory+"/jobs/"+file.split("/")[-1]))
        # except OSError as e:
        #     print("Failed to create directory!!!!!", e)
        #     pass
        
        job_list = []
        

        for filepath in file_list:
            file = filepath.split("/")[-1]
            
            # print("data resource file path : ", filepath)
            sys.stdout.write("data resource file path : "+ filepath+"\n")

            ###sample index management section
            vcf_read = VCF(filepath)
            samples = vcf_read.samples
            sample_size = len(samples)
            # print(sample_size)
            chromosome_num = ""
            for variant in vcf_read:
                chromosome_num = variant.CHROM
                break
            vcf_read.close()

            ###job script create section
            current_index = 0
            # print(current_index, sample_size)
            while(current_index < sample_size):
                filename = file.split("/")[-1]

                jobpath = os.path.join(self.target_directory+"/jobs/"+filename+"/chr"+chromosome_num+"_"+str(current_index))
                ab_jobpath = os.path.join(self.target_directory_name+"/jobs/"+filename, "chr"+chromosome_num+"_"+str(current_index) )
                # print("job script path : ", jobpath)
                # print("job script absolute path : ", ab_jobpath)
                try: 
                    if not os.path.isdir(jobpath):
                        os.makedirs(jobpath)
                except OSError as e:
                    print("Failed to create directory!!!!!", e)
                    pass
                
                # print("sh file path :  ", jobpath+"/job_"+str(current_index)+".sh")
                job_file = open(os.path.join(jobpath, "job_"+str(current_index)+".sh"), 'w')
                job_file.write("#!/bin/bash\n")
                job_file.write("#SBATCH --job-name=chr"+chromosome_num+"_"+str(current_index)+"_seperate\n")
                job_file.write("#SBATCH --output=/EDISON/SCIDATA/sdr/tmp/vcf/vcfcode/"+ab_jobpath+"/std_"+str(current_index)+".out\n")
                job_file.write("#SBATCH --error=/EDISON/SCIDATA/sdr/tmp/vcf/vcfcode/"+ab_jobpath+"/std_"+str(current_index)+".err\n")
                job_file.write("#SBATCH --nodes=1\n")
                job_file.write("#SBATCH --ntasks=1\n")
                job_file.write("#SBATCH --ntasks-per-node=1\n")
                job_file.write("#SBATCH --chdir=/EDISON/SCIDATA/sdr/tmp/vcf\n")
                job_file.write("JOBDIR=/EDISON/SCIDATA/sdr/tmp/vcf/vcfcode/"+ab_jobpath+"\n")
                job_file.write("USER=kwon\n")
                job_file.write("SINDIR=/EDISON/SCIDATA/sdr/singularity-images\n")
                job_file.write("source /usr/lib64/anaconda3/etc/profile.d/conda.sh\n")
                cmd = "/usr/bin/singularity exec -H ${JOBDIR}:/home/kwon -B /EDISON/SCIDATA/sdr/tmp/vcf:/tmp/vcf --pwd ${JOBDIR} "
                cmd = cmd + "/EDISON/SCIDATA/singularity-images/userenv3 "
                cmd = cmd + "python /tmp/vcf/vcfcode/sampleseperate.py /EDISON/SCIDATA/sdr/tmp/vcf/"
                from_abspath = os.path.abspath(filepath).split("/tmp/vcf/")[-1]
                # print("resource file directory : ", from_abspath)
                cmd = cmd+from_abspath+" "
                cmd = cmd+"/EDISON/SCIDATA/sdr/tmp/vcf/vcfcode/"+self.target_directory_name
                cmd = cmd+" "+str(current_index)
                # cmd = cmd+" "+str(currentIndex)
                job_file.write("\n")
                job_file.write(cmd)
                job_file.write("\n")
                job_file.flush()
                job_file.close()
                job_list.append(os.path.join(jobpath, "job_"+str(current_index)+".sh"))
                current_index = current_index+100
        
        return job_list
    
    def file_seperate(self, file, target_dir, targetindex):
        vcf_read = VCF(file)
        samples = vcf_read.samples
        # print(type(samples))
        # print(samples.__sizeof__())
        chromosome_num = ""
        for variant in vcf_read:
            chromosome_num = variant.CHROM
            break

        count = 0
        for index in range(int(targetindex), int(targetindex)+100):
            sample = samples[index]
            start = time.time()
            # print(sample, "file write start...  ", start)
            sys.stdout.write(sample+"file write start...  "+ str(start))
            # print(os.path.isdir(self.target_directory), os.path.isdir(self.target_directory+"/"+sample))
            try:
                if not(os.path.isdir(self.target_directory)):
                    os.makedirs(os.path.join(self.target_directory))
            except OSError as e:
                print("Failed to create directory!!!!!", e)
                pass

            try:
                if not(os.path.isdir(self.target_directory+"/samples/"+sample)):
                    os.makedirs(os.path.join(self.target_directory+"/samples/"+sample))
            except OSError as e:
                print("Failed to create directory!!!!!", e)
                pass
                        
            filepath = os.path.join(self.target_directory+"/samples/"+sample, chromosome_num+"-"+sample+".vcf")
            fileindex = 0
            while os.path.exists(filepath):
                fileindex = fileindex+1
                filepath = os.path.join(self.target_directory+"/samples/"+sample, chromosome_num+"-"+sample+"_"+str(fileindex)+".vcf")

            out_read_vcf = VCF(file, samples=[sample])
            sys.stdout.write("write file path : "+ filepath+ "\n")
            # print(filepath)
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
            if os.path.exists(filepath):
                os.remove(filepath)
            
            sec = time.time()- start
            print(sample+" write end...", time.strftime("%H:%M:%S", time.gmtime(sec)))
            sys.stdout.write(sample+" write end... " + time.strftime("%H:%M:%S", time.gmtime(sec)) + "\n")
            count = count+1
            if count == 100 or index == len(samples)-1:
                break
        vcf_read.close()

if __name__ == "__main__":
    main(sys.argv)