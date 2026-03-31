from app.main import refresh_database


if __name__ == "__main__":
    result = refresh_database()
    print(f"Data refreshed: {result['rows']} rows using {result['source']}")
