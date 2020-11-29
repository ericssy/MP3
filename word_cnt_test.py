import sys
import os 
# file_name = sys.argv[1]
# f  = open(file_name, 'r')
# lines = f.readlines()


files = os.listdir("test/")
dict_ = {}

for file in files:
    f  = open("test/" + file, 'r')
    lines = f.readlines()
    for line in lines:
        line = line.split()
        for word in line:
            if (word.isalpha()):
                if (word in dict_):
                    dict_[word] += 1
                else:
                    dict_[word] = 1

for key in dict_:
    print(key, dict_[key])
                
