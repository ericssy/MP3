# import sys
# file_name = sys.argv[1]
# f  = open(file_name, 'r')
# lines = f.readlines()

# for line in lines:
#     line = line.split()
#     for word in line:
#         if (word.isalpha()):
#             print(word, 1)

import sys
file_name = sys.argv[1]
f  = open(file_name, 'r')
lines = f.readlines()

for line in lines:
    # (A,B)\n
    l = eval(line.strip())
    print(1, l)
