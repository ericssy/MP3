from itertools import permutations 
import random
lst = ["A", "B", "C"]
result = []

result = list(permutations(lst))
print(result)

num_files = 10
data = ""
for i in range(num_files):
    for j in range(100):
        vote = random.choice(result)
        data += ", ".join(vote) + "\n"
    f = open("test/voting_file_" + str(i + 1) + ".txt", "w")
    f.write(data)
    f.close()





