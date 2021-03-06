import psycopg2
import getpass
import os
import shutil
import random
from hashlib import sha256
from tabulate import tabulate





# TODO refactor this into a connection class
class CovidDBConnection:
    pass

def connect_covid():

    # name of the database connecting to
    DBNAME = "jzhan18_covid"
    USER = input("Enter Username: ")
    HOST = "ada.hpc.stlawu.edu"
    PASSWORD = getpass.getpass("Enter Password: ")
    # get password out of user table using username
    
    
    # Security
    # On every line of code, "what can go wrong?"
    # Real software should never ever crash
    # always fail gracefully

    # PASSWORD = getpass.getpass("Please enter password:")

    #try:
    #    pgpass_file = open(PASSWORD_FILE)
    #except OSError:
    #    print("Cannot open credential file")
    #    exit(1)


    # Watchout, password visible in debugger. This means password is visible in
    # Main memory. We can assume that our main memory is protected
    #passwd = pgpass_file.readline()
    passwd = PASSWORD
    if len(passwd) == 0:
        print("Cannot open credential file")
        exit(1)

    try:
        conn = psycopg2.connect(
            dbname = DBNAME,
            user = USER,
            host = HOST,
            password = passwd
        )
    except psycopg2.Error as e:
        print(e)
        exit(1)

    return conn


def login(conn):
    cur = conn.cursor()
    # take in username and password
    cmd = "select salt, password_hashed from users where username = %s;"
    USER = input("Enter Username: ")
    PASSWORD = getpass.getpass("Enter Password: ")
    try:
        cur.execute(cmd, (USER,))
        salt, password_hashed = cur.fetchone()
    except psycopg2.Error as e:
        print("User not found")
        login(conn)

        
    # hash the password from the user and see if it matches the one we have on record

    PASSWORD = sha256(bytes(PASSWORD + salt, 'utf-8'))
    byte_pass = PASSWORD.digest()
    print(str(byte_pass), password_hashed)
    if str(byte_pass) != password_hashed:
        print("Password incorrect")
        login(conn)
    else:
        print_menu()
        
def print_menu():
    #FIXME
    menu = [["1)", "Update DB"], 
            ["2)", "Lots of Rows"], 
            ["3)", "The fraction of cases that are deaths in the past year"],
            ["4)", 
"""
Asian countries where the ratio of female smokers to male smokers is less than 
the average ratio of female smokers to male smokers in European countries in 2020 
(exclude all countries whose extreme poverty rate is below the Asian average)
""".replace("\n", "")],
            ["5)", "Query 3 (Other team)"],
            ["6)", "Query 4 (Other team)"],
            ["Q)", "Quit/Log Off"]
           ]
    print(tabulate(menu))

def update_db(conn, updated_file):

    cur = conn.cursor()
    sql_file = open(updated_file) #open the update.sql file 
    conn.commit()
    sql_file.read() #read the SQL to execute 
    #cur.execute(open('create_covid.sql', 'r').read())
    cur.execute("select max(date) from covid where covid.location = 'Zimbabwe';")
    for row in cur:
        print(row)

#finish these functions
def lots_of_rows(conn):
    cur = conn.cursor()
    cur.execute("select date from covid where covid.location = 'Zimbabwe';")
    
    # max rows
    # (c, r)
    size = shutil.get_terminal_size()[1]
    scroll(cur, size, cur.rowcount)

def scroll(cur, size, count):
    for i in range(size):
        if i >= count:
            break
        else:
            print(next(cur)[0])
            if i == size - 1:
                print("{} rows remaining".format(count - i))
                if count > size:
                    keypress = input("Press enter to continue")
                break

    count = count - size
    if count > 0 and len(keypress) == 0:
        scroll(cur, size, count)
            
def ex_team_query1(conn):
    cur = conn.cursor()
    cmd = " select sum(new_deaths_smoothed) / sum(new_cases_smoothed) as fraction from covid where date like '%2020%';"
    
    cur.execute(cmd,)
    value = cur.fetchone()[0] 
    
    output({"Fraction":[value]})
    
def ex_team_query2():
    cur = conn.cursor()
    cmd = """with asian(female_smokers, male_smokers, location) as 
(select
	female_smokers, male_smokers, location
from
	covid
where
	continent = 'Asia' and
	date like '%2020%' and -- we also only want 2020 data
	extreme_poverty < (select -- note that we exclude all asian countries where the extreme_poverty 
				avg(extreme_poverty) -- is lower than the average
			from
				covid
			where
				continent = 'Asia')),
-- same thing with Europe but we only need the average
european(average_smokers) as 
(select 
	avg(female_smokers / male_smokers)
from
	covid
where
	continent = 'Europe' and
	date like '%2020%' and
	extreme_poverty < (select
				avg(extreme_poverty)
			from
				covid
			where
				continent = 'Asia'))

select
	distinct asian.location
from
	asian,european
where -- And here we have the comparison between the averages
	(asian.female_smokers / asian.male_smokers) < european.average_smokers;"""
    
    cur.execute(cmd,)
    value = cur.fetchall()
    table = list()
    for v in value:
        table.append(v[0])
    
    output({"Countries":table})

def ex_other_query1():
    print("5")

def ex_other_query2():
    print("6")

def gracefully_exit():
    print("exit")
    exit()
        
        #conn.close()
        #open home page to restart -> this means running the show_table function
        # this function will restart the app 
        
# values: a dictionary where keys are headers and values are lists of values
def output(values):
    print(tabulate(values, headers="keys", tablefmt="simple"))

    
# M a i n    P r o g r a m
if __name__ == "__main__":
    conn = connect_covid()

    # Assumpption: We have a valid connection
    # Watchout, cannot assume connection stays valid, could lose connection
    # Keep checking the closed attribute on conn

    while True:
        # login(conn)
        print_menu()
        option = str(input("Enter Menu option:"))
        
        if option == '1':
            update_db(conn, "update_covid.sql")


        elif option == '2':
            lots_of_rows(conn)

        elif option == '3':
            ex_team_query1(conn)

        elif option == '4':
            ex_team_query2()
        
        elif option == '5':
            ex_other_query1
        
        elif option == '6':
            ex_other_query1
        
        elif option in ['q', 'Q']:
            done = True
            gracefully_exit()

        #except psycopg2.Error as e:
        #   print(e)
        #    exit(1)


        

"""

What options should we provide the user?
1) Put yourself in the shoes of the customer
2) analyze the domain (parts)
3) interview potential users and customers

"""
