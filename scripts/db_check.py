import sqlite3, json

db='data/polymirror.db'
conn=sqlite3.connect(db)
cur=conn.cursor()
cur.execute('SELECT * FROM followed_wallets')
cols=[d[0] for d in cur.description]
wallets=[dict(zip(cols,row)) for row in cur.fetchall()]
cur.execute('SELECT starting_balance,cash FROM account WHERE id=1')
acc_row=cur.fetchone()
account={'starting_balance':acc_row[0],'cash':acc_row[1]} if acc_row else {}
cur.execute("SELECT COUNT(*) FROM sim_positions WHERE status='open'")
open_pos=cur.fetchone()[0]
cur.execute('SELECT COUNT(*) FROM sim_trades')
trades=cur.fetchone()[0]
print(json.dumps({'wallets':wallets,'account':account,'open_positions':open_pos,'sim_trades':trades}, indent=2))
conn.close()
