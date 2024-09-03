import requests
from bs4 import BeautifulSoup
import mysql.connector
from mysql.connector import Error

try:
    # Connect to MySQL database
    conn = mysql.connector.connect(
        host="localhost",
        user="root",
        password="Nightnurse@vinnie44040",
        database="food_inflation",
        port=3306  # Default MySQL port
    )
    
    if conn.is_connected():
        print("Connected to MySQL database")

        # Create a cursor object
        cur = conn.cursor()

        # Create the table if it doesn't exist
        cur.execute("""
            CREATE TABLE IF NOT EXISTS food_inflation (
                id INT AUTO_INCREMENT PRIMARY KEY,
                date DATE,
                inflation_rate DECIMAL(5, 2),
                source TEXT
            );
        """)
        conn.commit()

        # Scrape the data
        url = "https://www.centralbank.go.ke/monthly-economic-indicators/"  # Replace with the correct URL
        try:
            response = requests.get(url)
            response.raise_for_status()  # Raise an error for bad status codes

            if response.status_code == 200:
                soup = BeautifulSoup(response.text, 'html.parser')

                # Print the HTML for debugging
                print(soup.prettify())

                # Find the table
                table = soup.find('table')

                if table is None:
                    print("Table not found on the page.")
                else:
                    inflation_data = []
                    for row in table.find_all('tr')[1:]:  # Skip header row
                        cols = row.find_all('td')
                        print(f"Number of columns: {len(cols)}")
                        if len(cols) >= 2:  # Ensure there are at least 2 columns
                            date = cols[0].text.strip()
                            rate = cols[1].text.strip()
                            inflation_data.append((date, rate))
                        else:
                            print("Row does not have enough columns:", [col.text.strip() for col in cols])

                    # Insert the data into the table
                    for date, rate in inflation_data:
                        cur.execute("""
                            INSERT INTO food_inflation (date, inflation_rate, source)
                            VALUES (%s, %s, %s)
                        """, (date, rate, url))

                    conn.commit()
                    print("Data successfully scraped and saved to the database!")
            else:
                print(f"Failed to retrieve the page. Status code: {response.status_code}")

        except requests.RequestException as e:
            print(f"Request error: {e}")

except Error as e:
    print(f"Database error: {e}")

finally:
    if conn.is_connected():
        cur.close()
        conn.close()
