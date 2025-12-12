#!/usr/bin/env python3
"""
Script to automate Mongo Express login using Selenium and list collections
"""

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import TimeoutException, NoSuchElementException
import time
import sys


def connect_to_selenium():
    """Connect to Selenium container"""
    print("Connecting to Selenium container...")

    try:
        options = webdriver.ChromeOptions()
        # Connect to the Selenium container
        driver = webdriver.Remote(
            command_executor='http://192.168.0.106:4444/wd/hub',
            options=options
        )
        print("✓ Connected to Selenium")
        return driver
    except Exception as e:
        print(f"✗ Error connecting to Selenium: {e}")
        print("Make sure Selenium container is running on port 4444")
        sys.exit(1)


def login_to_mongo_express(driver, base_url, username, password):
    """
    Login to Mongo Express using HTTP Basic Auth
    """
    print(f"\n{'=' * 50}")
    print("Accessing Mongo Express")
    print('=' * 50)

    try:
        # Construct URL with basic auth credentials
        # Format: http://username:password@host:port
        if '://' in base_url:
            protocol, rest = base_url.split('://', 1)
            auth_url = f"{protocol}://{username}:{password}@{rest}"
        else:
            auth_url = f"http://{username}:{password}@{base_url}"

        print(f"Navigating to Mongo Express...")
        driver.get(auth_url)

        # Wait for page to load
        time.sleep(3)

        print("✓ Successfully accessed Mongo Express")
        return True

    except Exception as e:
        print(f"✗ Error accessing Mongo Express: {e}")
        return False


def list_collections(driver):
    """
    Extract list of collections from Mongo Express interface
    """
    print(f"\n{'=' * 50}")
    print("MongoDB Collections")
    print('=' * 50)

    try:
        # Wait for the page to load completely
        wait = WebDriverWait(driver, 10)

        # Mongo Express typically shows databases first, then collections
        # Look for database links or collection listings
        # This may need adjustment based on your Mongo Express version

        # Try to find collection elements
        # Common selectors in Mongo Express
        collections = []

        # Method 1: Look for links with 'viewCollection' in href
        try:
            collection_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'Collections')]")
            for link in collection_links:
                collection_name = link.text.strip()
                if collection_name:
                    collections.append(collection_name)
        except NoSuchElementException:
            pass

        # Method 2: Look for table rows or list items with collection info
        if not collections:
            try:
                collection_elements = driver.find_elements(By.CSS_SELECTOR, "td a, li a")
                for elem in collection_elements:
                    href = elem.get_attribute('href')
                    if href and 'Collections' in href.lower():
                        collection_name = elem.text.strip()
                        if collection_name and collection_name not in collections:
                            collections.append(collection_name)
            except NoSuchElementException:
                pass

        # Print results
        if collections:
            print(f"\nFound {len(collections)} collection(s):\n")
            for i, collection in enumerate(collections, 1):
                print(f"  {i}. {collection}")
        else:
            print("\n⚠ No collections found or unable to parse the page")
            print("You may need to navigate to a specific database first")
            print("\nPage title:", driver.title)

            # Print available databases if on main page
            try:
                db_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'db/')]")
                if db_links:
                    print("\nAvailable databases:")
                    for db in db_links:
                        db_name = db.text.strip()
                        # Remove 'view' tag and clean up
                        db_name = db_name.replace('view', '').replace('View', '').strip()
                        if db_name:
                            print(f"  - {db_name}")
            except:
                pass

        return collections

    except TimeoutException:
        print("✗ Timeout waiting for page to load")
        return []
    except Exception as e:
        print(f"✗ Error listing collections: {e}")
        return []


def navigate_to_database(driver, database_name):
    """
    Navigate to a specific database by clicking on its link
    """
    print(f"\n{'=' * 50}")
    print(f"Navigating to database: {database_name}")
    print('=' * 50)

    try:
        # Find all database links
        db_links = driver.find_elements(By.XPATH, "//a[contains(@href, 'db/')]")

        # Look for the specific database
        for db_link in db_links:
            db_text = db_link.text.strip()
            # Clean up the text (remove 'view' tag)
            clean_db_name = db_text.replace('view', '').replace('View', '').strip()

            if clean_db_name.lower() == database_name.lower():
                print(f"Found database '{clean_db_name}', clicking...")
                db_link.click()

                # Wait for page to load
                time.sleep(2)

                print(f"✓ Successfully navigated to database: {database_name}")
                return True

        print(f"✗ Database '{database_name}' not found")
        return False

    except Exception as e:
        print(f"✗ Error navigating to database: {e}")
        return False


def list_collections_in_database(driver, database_name):
    """
    List all collections in a specific database
    """
    print(f"\n{'=' * 50}")
    print(f"Collections in '{database_name}'")
    print('=' * 50)

    try:
        # Wait for the page to load
        time.sleep(2)

        collections = []

        # Collections are in <h3> tags within table cells
        try:
            # Find all h3 elements in the table
            h3_elements = driver.find_elements(By.TAG_NAME, "h3")

            for h3 in h3_elements:
                # Get the link inside the h3
                try:
                    link = h3.find_element(By.TAG_NAME, "a")
                    collection_name = link.text.strip()

                    if collection_name and collection_name not in collections:
                        collections.append(collection_name)
                except NoSuchElementException:
                    continue

        except Exception as e:
            print(f"✗ Error parsing collections: {e}")

        # Print results
        if collections:
            print(f"\nFound {len(collections)} collection(s):\n")
            for i, collection in enumerate(collections, 1):
                print(f"  {i}. {collection}")
        else:
            print("\n⚠ No collections found in this database")
            print("The database might be empty or the page structure is different")

        return collections

    except Exception as e:
        print(f"✗ Error listing collections: {e}")
        return []


def main():
    """Main function"""
    # Configuration - UPDATE THESE
    MONGO_EXPRESS_URL = "192.168.0.106:8081"  # Without http://
    USERNAME = "admin"  # Change to your username
    PASSWORD = "admin123"  # Change to your password

    # Database to navigate to (set to None to just list all databases)
    DATABASE_NAME = "admin"  # Change this to your database name, or None

    driver = None

    try:
        # Connect to Selenium
        driver = connect_to_selenium()

        # Login to Mongo Express
        if login_to_mongo_express(driver, MONGO_EXPRESS_URL, USERNAME, PASSWORD):

            # If database name is specified, navigate to it
            if DATABASE_NAME:
                if navigate_to_database(driver, DATABASE_NAME):
                    # List collections in the specific database
                    collections = list_collections_in_database(driver, DATABASE_NAME)
                else:
                    print("\nFailed to navigate to database. Showing all databases instead:")
                    list_collections(driver)
            else:
                # Just list all databases (original behavior)
                list_collections(driver)

            # Optional: Take a screenshot for debugging
            # driver.save_screenshot("/tmp/mongo_express.png")
            # print("\nScreenshot saved to /tmp/mongo_express.png")

        print(f"\n{'=' * 50}")
        print("Complete!")
        print('=' * 50)

    except KeyboardInterrupt:
        print("\n\nInterrupted by user")
    except Exception as e:
        print(f"\n✗ Unexpected error: {e}")
    finally:
        # Clean up
        if driver:
            print("\nClosing browser...")
            driver.quit()


if __name__ == "__main__":
    main()
