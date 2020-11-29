# import threading
# import time

# #scpFileTransfer(toHostName, fromFile, toFile)

# def print_func(toHostName, fromFile, toFile):
#     time.sleep(2)
#     print(toHostName, fromFile, toFile)

# threads = []
# for i in range(4):
#     t = threading.Thread(target=print_func, args=("hostname", "fromFile", "toFile", ))
#     threads.append(t)
 
# for t in threads:
#     t.start()

# for t in threads:
#     t.join()

# t = threading.Thread(target=scpFileTransfer, args=(toHostName, fromFile, toFile, ))
#                             threads.append(t)
#                             t.start()