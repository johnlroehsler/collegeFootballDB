import MySQLdb
db = MySQLdb.connect(
    host="localhost",
    user="root",
    passwd="roehsler12!",
    db="myResellDB"
)
cursor = db.cursor()
cursor.execute("")
results = cursor.fetchall()
for row in results:
    print(row)