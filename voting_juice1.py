import sys
from ast import literal_eval

file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()


for line in lines:
    line = line.strip()
    l = eval(line)
    key = l[0]
    value = l[1]

    value_list = [int(i) for i in value]
    cnt_0 = 0
    cnt_1 = 0
    for num in value_list:
        if num == 0:
            cnt_0 += 1
        if num == 1:
            cnt_1 += 1


    name0 = key[0]
    name1 = key[1]

    if cnt_1 > cnt_0:
        print(name0, name1)
    else:
        print(name1, name0)
