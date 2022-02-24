from tkinter.messagebox import NO
from lstore.db import Database
from lstore.query import Query

db = Database()
table = db.create_table('table', 5, 0)

query = Query(table)

ins = False
for i in range(1, 1100):
    query.insert(i,i + 1,i + 6,i * 9, i % 5)
    rec = query.select(i, 0,[1,1,1,1,1])[0]
    if not rec.columns[0] == i:
        print("Failed Insert")
        ins = True
        break
    
    if not rec.columns[1] == i + 1:
        print("Failed Insert")
        ins = True
        break

    if not rec.columns[2] == i + 6:
        print("Failed Insert")
        ins = True
        break

    if not rec.columns[3] == i * 9:
        print("Failed Insert")
        ins = True
        break

    if not rec.columns[4] == i % 5:
        print("Failed Insert")
        ins = True
        break

if not ins:
    print('All Insertions succeeded')

print("Testing sum Answer should be : 65, Answer is ", query.sum(1,10,1))

dele = False
up = False 
for i in range(1, 1100):
    query.update(i, None, None, None, None, 5)
    rec = query.select(i, 0,[1,1,1,1,1])[0]
    if not rec.columns[0] == i:
        print("Failed Update")
        up = True
        break
    
    if not rec.columns[1] == i + 1:
        print("Failed Update")
        up = True
        break

    if not rec.columns[2] == i + 6:
        print("Failed Update")
        up = True
        break

    if not rec.columns[3] == i * 9:
        print("Failed Update")
        up = True
        break

    if not rec.columns[4] == 5:
        print("Failed Update")
        up = True
        break

if not up:
    print('all updates worked')

for j in range(1, 1100):
    query.delete(j)
    fail = query.select(j, 0,[1,1,1,1,1])
    if fail:
        dele = True
        print('Failed Delete')

if not dele:
    print("All Deletions worked")
    
