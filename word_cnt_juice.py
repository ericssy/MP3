import sys


file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()


for line in lines:
    line = line.strip()
    l = eval(line)
    key = l[0]
    value = l[1]
    value_list = [int(i) for i in value] 

    list_sum = sum(value_list)
    print(key, list_sum)