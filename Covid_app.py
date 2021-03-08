import psycopg2
import getpass
import os
from hashlib import sha256
from tabulate import tabulate
from csv import reader
import requests
# James, Jiusheng, Charlie
# Exam 1

# conn class
class CovidDBConnection:
    def __init__(self, dbname, user, password):
        # name of the database connecting to
        self.dbname = dbname
        # username
        self.user = user
        self.host = "ada.hpc.stlawu.edu"
        self.password = password
        self.conn = self.connect_covid()

    # make a connection to the database
    def connect_covid(self):
        try:
            conn = psycopg2.connect(
                dbname=self.dbname,
                user=self.user,
                host=self.host,
                password=self.password
            )
        except psycopg2.Error as e:
            print(e)
            exit(1)
        return conn

    # Uses the "users" table in the covid database to compare the hashed passwords to the ones that users enter
    def login(self):
        cur = self.conn.cursor()
        # take in username and password
        cmd = "select salt, password_hashed from users where username = %s;"
        user = input("Enter Username: ")
        password = getpass.getpass("Enter Password: ")

        try:
            cur.execute(cmd, (user,))

        except psycopg2.Error as e:
            print("Connection lost")
            self.login()

        # get (salt, password)
        value = cur.fetchone()

        # make sure we have a valid input
        if value is not None:
            salt, password_hashed = value
            # hash the password from the user and see if it matches the one we have on record
            password = sha256(bytes(password + salt, 'utf-8'))
            byte_pass = password.digest()

            # password incorrect
            if str(byte_pass) != password_hashed:
                print("Username or password incorrect")
                # try again
                self.login()

        else:
            print("Username or password incorrect")
            # try again
            self.login()

    # displays a menu
    def print_menu(self):
        menu = [["1)", "Update DB"],
                ["2)", "Lots of Rows"],
                ["3)", "The fraction of cases that are deaths in the past year"],
                ["4)",
                 "Asian countries where the ratio of female smokers to male smokers is less than "
                 "the average ratio of female smokers to male smokers in European countries in 2020 "
                 "(exclude all countries whose extreme poverty rate is below the Asian average)"],
                ["5)", "Survival rate"],
                ["6)", "Median age of each country ordered by ascending survival rate"],
                ["Q)", "Quit/Log Off"]
                ]
        print(tabulate(menu))
    # This function updates the database,
    # it uses curl to download the csv file automatically, then compares the
    # downloaded file to the version pulled from the database.
    def update_db(self):
        try:
            cur = self.conn.cursor()
            cmd = "select * from covid"

            # getting old csv
            outputquery = 'copy ({0}) to stdout with csv header'.format(cmd)
            with open("csv_old.csv", "x") as f:
                cur.copy_expert(outputquery, f)
            print("Preparing to update Database")
            # getting new csv
            self.download("https://raw.githubusercontent.com/owid/covid-19-data/master/public/data/owid-covid-data.csv")
            print("Downloaded new information")
            # comparing old and new
            with open('csv_old.csv', 'r') as t1, open('csv_new.csv', 'r') as t2:
                fileone = t1.readlines()
                filetwo = t2.readlines()

            # looping through to find the differences
            with open('differences.csv', 'x') as outFile:
                for line in filetwo:
                    if line not in fileone:
                        outFile.write(line)

            os.remove("csv_old.csv")
            os.remove("csv_new.csv")

            with open('differences.csv', 'r') as read_obj:
                owid = reader(read_obj)
                list_of_rows = (list(owid))

            #loop through the differences to update them
            for r in range(1, len(list_of_rows)):
                line = "("
                row = list_of_rows[r]
                for i in range(len(row)):
                    if row[i] == "":
                        line += "NULL,"
                    elif i in [0, 1, 2, 3, 33]:
                        line += "\'" + row[i].replace('\'', '\'\'') + "\',"
                        if i == 3:
                            date = row[i]
                        if i == 2:
                            location = row[i].replace('\'', '\'\'')
                    else:
                        line += row[i] + ","
                line = line[:-1]
                line += ")"
                # Cote d'Ivoire is the only location in the database that has a ' in the name
                # this causes a problem because we need to escape it in the sql query
                if location == "Cote d'Ivoire":
                    line = line[:23] + "'" + line[23:]
                    cmd = "delete from covid " \
                        "where date = '{0}' and location = 'Cote d''Ivoire';" \
                        "insert into covid values {1};".format(date, line)
                    cur.execute(cmd, )
                    cmd = "select location from covid where date = '{0}' and location = 'Cote d''Ivoire';".format(date)
                    cur.execute(cmd, )
                else:
                    cmd = "delete from covid " \
                        "where date = '{0}' and location = '{1}';" \
                        "insert into covid values {2};".format(date,location, line)
                    cur.execute(cmd,)
                    cmd = "select location from covid where date = '{0}' and location = '{1}';".format(date,location)
                    cur.execute(cmd,)
                existance = cur.fetchall()
                if existance == ():
                    cur.execute("insert into covid values {0}").format(line)
            os.remove("differences.csv")
            print("Updated")
        except psycopg2.Error as e:
            print("Connection lost, incomplete update")
            os.remove("differences.csv")
            exit()

    # method to download csv file from a website
    def download(self, url: str):
        filename = "csv_new.csv"
        file_path = os.path.join(filename)
        r = requests.get(url, stream=True)
        if r.ok:
            print("saving to", os.path.abspath(file_path))
            with open(file_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=1024 * 8):
                    if chunk:
                        f.write(chunk)
                        f.flush()
                        os.fsync(f.fileno())
        else:
            print("Download failed: status code {}\n{}".format(r.status_code, r.text))

    # execute a query that will output lots of rows
    def lots_of_rows(self):
        try:
            cur = self.conn.cursor()
            cmd = "select date from covid where covid.location = 'Zimbabwe';"
            cur.execute(cmd, )
        except psycopg2.Error as e:
            print("Connection lost")
            exit()

        # max rows
        size = os.get_terminal_size()[1] - 1
        self.scroll(cur, size, cur.rowcount)

    # a method to restrict the number of rows
    def scroll(self, cur, size, count):
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
        # check if rows left are less than the terminal size
        count = count - size
        if count > 0 and len(keypress) == 0:
            self.scroll(cur, size, count)

    # Execute our team's first query
    def ex_team_query1(self):
        try:
            cur = self.conn.cursor()
            cmd = " select sum(new_deaths_smoothed) / sum(new_cases_smoothed) as fraction " \
                  "from covid where date like '%2020%';"
            cur.execute(cmd, )

        except psycopg2.Error as e:
            print("Connection lost")
            exit()

        value = cur.fetchone()[0]

        self.output({"Fraction": [value]})

    # Execute our team's first query
    def ex_team_query2(self):
        try:
            cur = self.conn.cursor()
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

        self.output({"Countries": table})

    # Execute our team's first query
    def ex_other_query1(self):
        try:
            cur = self.conn.cursor()
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

        self.output({"Location": locations,
                "Median Age": ages,
                "Survival Rate": survival_rate})

    # Execute our team's first query
    def ex_other_query2(self):
        try:
            cur = self.conn.cursor()
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

        self.output({"Location": locations,
                "GDP per capita": gdp})

    # Closes the connection, logging the user out.
    def gracefully_exit(self):
        print("exit")
        self.conn.close()
        exit()


    # values: a dictionary where keys are headers and values are lists of values
    def output(self, values):
        print(tabulate(values, headers="keys", tablefmt="simple"))


# M a i n    P r o g r a m
if __name__ == "__main__":
    DBNAME = "jrwhit18_covid"

    conn = CovidDBConnection(DBNAME,
                             input("Enter username: "),
                             getpass.getpass("Enter password: "))

    # Assumpption: We have a valid connection
    # Watchout, cannot assume connection stays valid, could lose connection
    # Keep checking the closed attribute on conn
    conn.login()

    while True:
        conn.print_menu()
        option = str(input("Enter Menu option:"))

        if option == '1':
            conn.update_db()

        elif option == '2':
            conn.lots_of_rows()

        elif option == '3':
            conn.ex_team_query1()

        elif option == '4':
            conn.ex_team_query2()

        elif option == '5':
            conn.ex_other_query1()

        elif option == '6':
            conn.ex_other_query1()

        elif option in ['q', 'Q']:
            done = True
            conn.gracefully_exit()
