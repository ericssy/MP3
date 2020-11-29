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
    names = line.split(",")
    names = [name.strip() for name in names]
    for i in range(0, 2):
        for j in range(i + 1, 3):
            #print(names[i] < names[j], names[i], names[j])
            if (names[i] < names[j]):
                print(( (names[i], names[j]), 1))
            else:
                print(( (names[j], names[i]), 0))

