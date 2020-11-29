# import sys
# import re

# for line in sys.stdin:
#     for word in line[:-1].split():
#         print(word, 1)

import sys
file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()

for line in lines:
    line = line.split()
    for word in line:
        if (word.isalpha()):
            print(word, 1)
