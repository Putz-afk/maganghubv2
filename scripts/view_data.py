import sqlite3
import os

print("📋 Viewing Jobs in Database")
print("=" * 60)

# Database path
db_path = os.path.join('..', 'backend', 'data.db')

try:
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row  # This allows column access by name
    cursor = conn.cursor()
    
    # Get total count
    cursor.execute("SELECT COUNT(*) as total FROM jobs")
    total = cursor.fetchone()['total']
    print(f"Total jobs in database: {total}\n")
    
    # Get all jobs with details
    cursor.execute('''
        SELECT 
            id, source_id, job_title, company_name, province, city,
            recommended_degree, quota, registered, application_deadline,
            LENGTH(job_description) as desc_length
        FROM jobs 
        ORDER BY id
    ''')
    
    jobs = cursor.fetchall()
    
    for job in jobs:
        print(f"🔹 ID: {job['id']}")
        print(f"   Source ID: {job['source_id']}")
        print(f"   Title: {job['job_title']}")
        print(f"   Company: {job['company_name']}")
        print(f"   Location: {job['city']}, {job['province']}")
        print(f"   Recommended Degree: {job['recommended_degree']}")
        print(f"   Quota: {job['quota']}, Registered: {job['registered']}")
        print(f"   Deadline: {job['application_deadline']}")
        print(f"   Description Length: {job['desc_length']} characters")
        print("-" * 60)
    
    # Show some statistics
    print("\n📊 Statistics:")
    cursor.execute("SELECT COUNT(DISTINCT company_name) as unique_companies FROM jobs")
    unique_companies = cursor.fetchone()['unique_companies']
    print(f"Unique companies: {unique_companies}")
    
    cursor.execute("SELECT COUNT(DISTINCT job_title) as unique_titles FROM jobs")
    unique_titles = cursor.fetchone()['unique_titles']
    print(f"Unique job titles: {unique_titles}")
    
    cursor.execute("SELECT AVG(quota) as avg_quota, AVG(registered) as avg_registered FROM jobs")
    stats = cursor.fetchone()
    print(f"Average quota: {stats['avg_quota']:.1f}")
    print(f"Average registered: {stats['avg_registered']:.1f}")
    
    conn.close()
    
except sqlite3.Error as e:
    print(f"❌ Database error: {e}")
except Exception as e:
    print(f"❌ Unexpected error: {e}")