import os
import sys



def main(sysarg):
    search_list = []
    search_list = search_err_file(sysarg[1])
    for errfile in search_list:
        read_file = open(errfile, 'r')
        while True:
            line = read_file.readline()
            if not line: break
            if not "The index file is older than the data file:" in line:
                print(errfile)
                print(line)
    print("\n\n\n\n")
    print("count : ", len(search_list))


def search_err_file(directory):
    search_file_list = []
    for(path, dir, files) in os.walk(directory):
        for filename in files:
            if filename.endswith(".err") and os.path.getsize(os.path.join(path,filename)) > 0 :
                search_file_list.append(os.path.join(path,filename))
    return search_file_list

if __name__ == "__main__":
    main(sys.argv)