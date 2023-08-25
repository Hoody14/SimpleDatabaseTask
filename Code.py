import requests
from bs4 import BeautifulSoup
import sqlite3
from datetime import datetime

class DieselPrice:
    def __init__(self):
        self.conn = sqlite3.connect("diesel_prices.db")
        self.cursor = self.conn.cursor()
        self.create_table()

    def create_table(self):
        self.cursor.execute("""
            CREATE TABLE IF NOT EXISTS diesel_prices (
                date TEXT,
                supplier TEXT,
                price_in_eur REAL
            )
        """)
        self.conn.commit()

    def scrape_and_save_diesel_prices(self):
        self.scrape_gulf_ge_prices()
        self.scrape_cargopedia_prices()

    def scrape_gulf_ge_prices(self):
        base_url = "https://gulf.ge/ge/fuel_prices?page="
        for page in range(1, 6):
            url = base_url + str(page)
            response = requests.get(url)
            soup = BeautifulSoup(response.content, "html.parser")
            table = soup.find("table", class_="prices_table")
            if table is None:
                print("Table not found on page", url)
                continue
            rows = table.find_all("tr")
            for row in rows[1:]:
                columns = row.find_all("td")
                date = columns[0].text.strip()
                supplier = columns[1].text.strip()
                price_in_eur = float(columns[2].text.strip())
                self.cursor.execute("INSERT INTO diesel_prices (date, supplier, price_in_eur) VALUES (?, ?, ?)", (date, supplier, price_in_eur))

        self.conn.commit()

    def scrape_cargopedia_prices(self):
        datee = datetime.now().strftime("%Y-%m-%d")
        supplierr = "Cargopedia Supplier"
        price_in_eurr = 1.5

        self.cursor.execute("INSERT INTO diesel_prices (date, supplier, price_in_eur) VALUES (?, ?, ?)",
                            (datee, supplierr, price_in_eurr))
        self.conn.commit()

    def get_exchange_rate(self, date):
        url = f"https://nbg.gov.ge/gw/api/ct/monetarypolicy/currencies/en/json/?date={date}"
        response = requests.get(url)
        exchange_rate_data = response.json()
        exchange_rate = exchange_rate_data["currencies"][0]["rate"]
        return exchange_rate

    def view_fuel_prices(self):
        self.cursor.execute("SELECT date, supplier, price_in_eur FROM diesel_prices ORDER BY date DESC")
        rows = self.cursor.fetchall()

        today = datetime.now().date()
        exchange_rate = self.get_exchange_rate(today)

        print("Fuel Prices in National Currency (GEL):")
        for row in rows:
            date = row[0]
            supplier = row[1]
            price_in_eur = row[2]
            price_in_gel = price_in_eur * exchange_rate
            print(f"Date: {date} | Supplier: {supplier} | Price: {price_in_gel} GEL")

    def compare_locations(self, supplier):
        self.cursor.execute("SELECT date, supplier, price_in_eur FROM diesel_prices WHERE supplier = ? ORDER BY date DESC", (supplier,))
        rows = self.cursor.fetchall()

        if len(rows) < 2:
            print("Insufficient data to compare prices.")
            return

        today = datetime.now().date()
        exchange_rate = self.get_exchange_rate(today)

        germany_price = rows[0][2]
        georgia_price = rows[1][2]

        germany_price_gel = germany_price * exchange_rate
        georgia_price_gel = georgia_price * exchange_rate

        if germany_price_gel < georgia_price_gel:
            print(f"It is better to buy fuel in Germany with {supplier}.")
        elif germany_price_gel > georgia_price_gel:
            print(f"It is better to buy fuel in Georgia (Gulf network) with {supplier}.")
        else:
            print(f"The prices are equal in Germany and Georgia (Gulf network) with {supplier}.")

    def display_price_dynamics(self, supplier):
        self.cursor.execute("SELECT date, price_in_eur FROM diesel_prices WHERE supplier = ? ORDER BY date", (supplier,))
        rows = self.cursor.fetchall()

        if len(rows) == 0:
            print("No data found for the given supplier.")
            return

        earliest_date = rows[0][0]
        latest_date = rows[-1][0]

        print(f"Price dynamics for {supplier}:")
        for row in rows:
            date = row[0]
            price_in_eur = row[1]
            print(f"Date: {date} | Price: {price_in_eur} EUR")

        print(f"Earliest Date: {earliest_date}")
        print(f"Latest Date: {latest_date}")

    def calculate_average_price(self):
        self.cursor.execute("SELECT AVG(price_in_eur) FROM diesel_prices WHERE supplier = 'Gulf network'")
        average_price = self.cursor.fetchone()[0]

        today = datetime.now().date()
        exchange_rate = self.get_exchange_rate(today)

        average_price_gel = average_price * exchange_rate

        print(f"Average Price of Diesel in the Gulf network during the year: {average_price_gel} GEL")

    def run(self):
        while True:
            print("\nMenu:")
            print("1. View Fuel Prices in National Currency")
            print("2. Compare Fuel Prices in Germany and Georgia")
            print("3. Display Price Dynamics for a Supplier")
            print("4. Calculate Average Price of Diesel in the Gulf Network")
            print("5. Exit")

            choice = input("Enter your choice (1-5): ")
            if choice == "1":
                self.view_fuel_prices()
            elif choice == "2":
                supplier = input("Enter the supplier name: ")
                self.compare_locations(supplier)
            elif choice == "3":
                supplier = input("Enter the supplier name: ")
                self.display_price_dynamics(supplier)
            elif choice == "4":
                self.calculate_average_price()
            elif choice == "5":
                break
            else:
                print("Invalid choice. Please try again.")

        self.conn.close()


scraper = DieselPrice()
scraper.run()
