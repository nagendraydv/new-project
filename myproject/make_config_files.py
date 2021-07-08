import string, random
from codecs import encode, decode

def write_encrypted_file(path='.',filename='mysql.config'):
  with open(path + filename,'w') as f:
    for i in range(6):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[0]))
    for i in range(4):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[1]))
    for i in range(1):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,105)))))
    f.write('%r,\n' %(en[2]))
    for i in range(5):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,95)))))
    f.write('%r,\n' %(en[3]))
    for i in range(9):
        f.write('%r,\n'%(''.join(random.choice(string.ascii_letters + string.digits + r'/\n\t=@#<>-;!') for _ in range(random.randint(20,125)))))

# Config file for recoengine

path = "."
h,u,p,d = ["localhost", "root", "root", "mint_loan_admin"]
en = [encode(h.encode(), 'base-64'), encode(u.encode(),'uu'), encode(encode(p.encode(),'uu'), 'base-64'), encode(encode(d.encode(), 'base64'), 'uu')]
write_encrypted_file(path=path,filename="mysql.config")
