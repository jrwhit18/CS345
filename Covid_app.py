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

    # try:
    #    pgpass_file = open(PASSWORD_FILE)
    # except OSError:
    #    print("Cannot open credential file")
    #    exit(1)

    # Watchout, password visible in debugger. This means password is visible in
    # Main memory. We can assume that our main memory is protected
    # passwd = pgpass_file.readline()
    passwd = PASSWORD
    if len(passwd) == 0:
        print("Cannot open credential file")
        exit(1)

    try:
        conn = psycopg2.connect(
            dbname=DBNAME,
            user=USER,
            host=HOST,
            password=passwd
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
    # FIXME
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
    os.system("curl https://github.com/owid/covid-19-data/blob/master/public/data/owid-covid-data.csv --output /owid-covid-data.csv")
    
    #os.system('cmd /k "Your Command Prompt Command"')
    #curl http://some.url --output some.file
    with open('owid-covid-data.csv', 'r') as read_obj:
        owid = reader(read_obj)
        print(str(owid))
        list_of_rows = (list(owid))
    cmd = "DROP table if exists owid_data;"
    cur.execute(cmd, )
    
    # create table for owid data
    cmd = "CREATE TABLE IF NOT EXISTS owid_data ( iso_code char(8), continent text, location text NOT NULL, date text NOT NULL, total_cases real, new_cases real, new_cases_smoothed real, total_deaths real, new_deaths real,new_deaths_smoothed real,total_cases_per_million real,new_cases_per_million real,new_cases_smoothed_per_million real,total_deaths_per_million real,new_deaths_per_million real,new_deaths_smoothed_per_million real,reproduction_rate real,icu_patients real,icu_patients_per_million real,hosp_patients real,hosp_patients_per_million real,weekly_icu_admissions real,weekly_icu_admissions_per_million real,weekly_hosp_admissions real,weekly_hosp_admissions_per_million real,total_tests real,new_tests real,total_tests_per_thousand real,new_tests_per_thousand real,new_tests_smoothed real,new_tests_smoothed_per_thousand real,positive_rate real,tests_per_case real,tests_units text,total_vaccinations real,people_vaccinated real,people_fully_vaccinated real,new_vaccinations real,new_vaccinations_smoothed real,total_vaccinations_per_hundred real, people_vaccinated_per_hundred real,people_fully_vaccinated_per_hundred real, new_vaccinations_smoothed_per_million real, stringency_index real, population real, population_density real, median_age real, aged_65_older real,aged_70_older real,gdp_per_capita real,extreme_poverty real,cardiovasc_death_rate real,diabetes_prevalence real,female_smokers real,male_smokers real,handwashing_facilities real,hospital_beds_per_thousand real,life_expectancy real,human_development_index real,primary key (location, date));"

    cur.execute(cmd,)

    #keep track of the type for each data column 
    col_types = ["char(8)", "text", "text NOT NULL", "date NOT NULL", "real", "real", "real","real","real","real","real","real","real","real","real","real","real","real","real","real","real","real","real","real", "real","real","real", "real","real","real","real","real","real","text", "real","real","real","real","real", "real", "real","real", "real", "real", "real", "real", "real", "real","real","real","real","real","real", "real", "real","real","real","real","real"]

    #loop through and copy the list of lists into the table 
    # I am ding this because we cannot run any / psql commands like /copy in this python file 
    split_pattn = re.compile("\,", re.VERBOSE)
    for r in range(2, len(list_of_rows)):
        
        line = "("
        #l = list()
        #for v in split_pattn.split(str(list_of_rows[r])):
         #   if v != "[" and v != "]" and type(v) not in [float, int]:
          #      v = str(v)
           # l.append(v)
        row = list_of_rows[r]
        for i in range(len(row)):
            if row[i] == "":
                line += "NULL,"
            elif i in [0,1,2,3,33]:
                line += "\'" + row[i] + "\',"
            else:
                line += row[i] + ","
        #line = line[:-1]
        line += ")"
        print(line)
        cmd = "insert into owid_data values {0};".format(line)
        cur.execute(cmd, )
        line = line[:-1]
    cmd = "CREATE TABLE IF NOT EXISTS tmp_table ( iso_code char(8), continent text, location text NOT NULL, date text NOT NULL, total_cases real, new_cases real, new_cases_smoothed real, total_deaths real, new_deaths real,new_deaths_smoothed real,total_cases_per_million real,new_cases_per_million real,new_cases_smoothed_per_million real,total_deaths_per_million real,new_deaths_per_million real,new_deaths_smoothed_per_million real,reproduction_rate real,icu_patients real,icu_patients_per_million real,hosp_patients real,hosp_patients_per_million real,weekly_icu_admissions real,weekly_icu_admissions_per_million real,weekly_hosp_admissions real,weekly_hosp_admissions_per_million real,total_tests real,new_tests real,total_tests_per_thousand real,new_tests_per_thousand real,new_tests_smoothed real,new_tests_smoothed_per_thousand real,positive_rate real,tests_per_case real,tests_units text,total_vaccinations real,people_vaccinated real,people_fully_vaccinated real,new_vaccinations real,new_vaccinations_smoothed real,total_vaccinations_per_hundred real, people_vaccinated_per_hundred real,people_fully_vaccinated_per_hundred real, new_vaccinations_smoothed_per_million real, stringency_index real, population real, population_density real, median_age real, aged_65_older real,aged_70_older real,gdp_per_capita real,extreme_poverty real,cardiovasc_death_rate real,diabetes_prevalence real,female_smokers real,male_smokers real,handwashing_facilities real,hospital_beds_per_thousand real,life_expectancy real,human_development_index real,primary key (location, date));"

    cur.execute(cmd, )

    cmd = " select * from owid_data; "
    cur.execute(cmd, )
    owid_row = cur.fetchone()
    print(owid_row)
    


# finish these functions
def lots_of_rows(conn):
    try:
        cur = conn.cursor()
        cmd = "select date from covid where covid.location = 'Zimbabwe';"
        cur.execute(cmd, )
    except psycopg2.Error as e:
        print("Connection lost")
        exit()

    # max rows
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
    try:
        cur = conn.cursor()
        cmd = " select sum(new_deaths_smoothed) / sum(new_cases_smoothed) as fraction from covid where date like '%2020%';"
        cur.execute(cmd, )

    except psycopg2.Error as e:
        print("Connection lost")
        exit()


    value = cur.fetchone()[0]

    output({"Fraction": [value]})


def ex_team_query2(conn):
    try:
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

        cur.execute(cmd, )
    except psycopg2.Error as e:
        print("Connection lost")
        exit()

    value = cur.fetchall()
    table = list()
    for v in value:
        table.append(v[0])

    output({"Countries": table})


def ex_other_query1(conn):
    try:
        cur = conn.cursor()
        cmd = """with most_recent as
        -- get the most recent data
        (select
            location, max(date) as date
        from
            covid
        group by
            location)
    select
        -- (location, median_age, survival_rate)
        location, median_age, (1 - (total_deaths / total_cases)) as survival_rate
    from
        -- get the most recent data
        covid natural join most_recent
    where
        -- get rid of tuples where survival_rate may be null
        total_deaths is not null and
        total_cases is not null
    order by
        survival_rate;"""
        cur.execute(cmd, )
    except psycopg2.Error as e:
        print("Connection lost")
        exit()

    value = cur.fetchall()
    locations = list()
    ages = list()
    survival_rate = list()
    for v in value:
        locations.append(v[0])
        ages.append(v[1])
        survival_rate.append(v[2])

    output({"Location": locations,
            "Median Age": ages,
            "Survival Rate": survival_rate})


def ex_other_query2(conn):
    try:
        cur = conn.cursor()
        cmd = """(select
            -- countries with no people vaccinated as of the most recent date
            location, gdp_per_capita, max(date) as date
        from
            covid
        where
            -- no vaccination
            total_vaccinations is null or 
            total_vaccinations = 0
        group by
            location, gdp_per_capita),
        max_gdp as
        (select
            -- get the max gdp of these countries
            max(gdp_per_capita)
        from
            no_vaccination)
    select
        location, gdp_per_capita
    from
        covid natural join no_vaccination, max_gdp
    where
        -- get the target country
        covid.gdp_per_capita = max_gdp.max;"""
        cur.execute(cmd, )
    except psycopg2.Error as e:
        print("Connection lost")
        exit()

    value = cur.fetchall()
    locations = list()
    gdp = list()

    for v in value:
        locations.append(v[0])
        gdp.append(v[1])

    output({"Location": locations,
            "GDP per capita": gdp})


def gracefully_exit():
    print("exit")
    exit()

    # conn.close()
    # open home page to restart -> this means running the show_table function
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
    login(conn)

    while True:
        print_menu()
        option = str(input("Enter Menu option:"))

        if option == '1':
            update_db(conn, "update_covid.sql")


        elif option == '2':
            lots_of_rows(conn)

        elif option == '3':
            ex_team_query1(conn)

        elif option == '4':
            ex_team_query2(conn)

        elif option == '5':
            ex_other_query1(conn)

        elif option == '6':
            ex_other_query1(conn)

        elif option in ['q', 'Q']:
            done = True
            gracefully_exit()

        # except psycopg2.Error as e:
        #   print(e)
        #    exit(1)

"""
What options should we provide the user?
1) Put yourself in the shoes of the customer
2) analyze the domain (parts)
3) interview potential users and customers
"""
