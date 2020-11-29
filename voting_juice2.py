import sys
from ast import literal_eval

file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()

'''
Reduce2 (key=1, value = array of (A,B) pairs) // only 1 reduce task
    create int Carray[m] of candidates, init to 0
    for each (A,B) pair in value
    Carray[A]++
    if some element Carray[A]==m-1
    Emit (key=A, value=”Condorcet winner!”)
    else
    Find max count in Carray
    Find set S of all candidates whose Carray counts == max
    Emit (key=set S, value=”No Condorcet winner, Highest Condorcet counts”)
    // Note: also ok if your solution emits only candidate(s) highest condorcet counts,
    as this would include a Condorcet winner

(1, [(A,B), (B,C), (A,C)])
'''
dict_ = {}

for line in lines:
    line = line.strip()
    list = eval(line)[1]
    for each in list:
        if each[0] not in dict_:
            dict_[each[0]] = 1
        else:
            dict_[each[0]] += 1

winner = []
max_count = -1
for key in dict_:
    max_count = max(max_count, dict_[key])
    if dict_[key] == 2:
        print(key, "Condorcet winner!")
        return

for key in dict_:
    if dict_[key] == max_count:
        winner.append(key)
print(winner, "No Condorcet winner, Highest Condorcet counts")
