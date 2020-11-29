import sys
from ast import literal_eval

file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()

dict_ = {}

for line in lines:
    line = line.strip()
    lst = eval(line)[1]
    for each in lst:
        each.strip("\"")
        each = eval(each)
        if each[0] not in dict_:
            dict_[each[0]] = 1
        else:
            dict_[each[0]] += 1

winner = []
max_count = -1
flag = False
for key in dict_:
    max_count = max(max_count, dict_[key])
    if dict_[key] == 2:
        print(key, "Condorcet winner!")
        flag = True
        break

if not flag:
    for key in dict_:
        if dict_[key] == max_count:
            winner.append(key)
    print(str(winner), "No Condorcet winner, Highest Condorcet counts")
