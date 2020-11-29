

def scpFileTransfer(toHostname, fromFile, toFile):
    from paramiko import SSHClient
    import paramiko
    from scp import SCPClient

    ssh = SSHClient()
    ssh.load_system_host_keys()
    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    print("SCP starts ", toHostname, " " , fromFile, " ", toFile)
    # ssh.connect(hostname='fa20-cs425-g21-01.cs.illinois.edu',
    #             username='jiaxusu2',
    #             password='Lysws1994')

    ssh.connect(hostname=toHostname,
                username='jiaxusu2',
                password='Lysws1994')

    print("ssh done")
    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())

    scp.put(fromFile, toFile)
    # scp.get('file_path_on_remote_machine', 'file_path_on_local_machine')

    scp.close()


# if __name__ == '__main__':
#     scpFileTransfer('172.22.94.68','test1.txt','/home/jiaxusu2/'  )