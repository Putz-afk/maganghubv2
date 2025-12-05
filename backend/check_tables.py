import sqlite3

print("Checking database tables...")

try:
    # Connect to database
    conn = sqlite3.connect('data.db')
    cursor = conn.cursor()
    
    # Get all tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    
    print(f"Found {len(tables)} tables:")
    for table in tables:
        print(f"  - {table[0]}")
    
    # Check if 'jobs' table exists
    job_tables = [table for table in tables if table[0] == 'jobs']
    if job_tables:
        print("\n✅ 'jobs' table exists!")
        
        # Check columns in jobs table
        cursor.execute("PRAGMA table_info(jobs);")
        columns = cursor.fetchall()
        print(f"\nColumns in 'jobs' table ({len(columns)} total):")
        for col in columns:
            print(f"  - {col[1]} ({col[2]})")
    else:
        print("\n❌ 'jobs' table NOT found!")
    
    # Check if 'job_tags' table exists  
    tag_tables = [table for table in tables if table[0] == 'job_tags']
    if tag_tables:
        print("\n✅ 'job_tags' table exists!")
    else:
        print("\n❌ 'job_tags' table NOT found!")
    
    conn.close()
    
except Exception as e:
    print(f"Error: {e}")