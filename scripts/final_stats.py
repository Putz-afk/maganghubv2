# Go to scripts folder and run a quick stats check
import sqlite3
import os

db_path = os.path.join('..', 'backend', 'data.db')

print("="*60)
print("FINAL DATABASE STATISTICS")
print("="*60)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Total jobs
cursor.execute("SELECT COUNT(*) FROM jobs")
total_jobs = cursor.fetchone()[0]

# Jobs by province
cursor.execute("SELECT province, COUNT(*) FROM jobs GROUP BY province ORDER BY COUNT(*) DESC")
provinces = cursor.fetchall()

# Unique companies
cursor.execute("SELECT COUNT(DISTINCT company_name) FROM jobs")
unique_companies = cursor.fetchone()[0]

# Total quota and registered
cursor.execute("SELECT SUM(quota), SUM(registered) FROM jobs")
total_quota, total_registered = cursor.fetchone()
total_quota = total_quota or 0
total_registered = total_registered or 0

# Date range
cursor.execute("SELECT MIN(application_deadline), MAX(application_deadline) FROM jobs WHERE application_deadline IS NOT NULL")
min_date, max_date = cursor.fetchone()

print(f"📊 Total Jobs: {total_jobs}")
print(f"🏢 Unique Companies: {unique_companies}")
print(f"🎯 Total Positions Available: {total_quota}")
print(f"👥 Total Registered Applicants: {total_registered}")
print(f"📈 Application Rate: {total_registered}/{total_quota} = {(total_registered/total_quota*100 if total_quota > 0 else 0):.1f}%")

print(f"\n📅 Date Range:")
print(f"   Earliest Deadline: {min_date}")
print(f"   Latest Deadline: {max_date}")

print(f"\n🌍 Jobs by Province:")
for province, count in provinces:
    print(f"   {province}: {count} jobs")

# Top 10 job titles
cursor.execute("""
    SELECT job_title, COUNT(*) as count 
    FROM jobs 
    GROUP BY job_title 
    ORDER BY count DESC 
    LIMIT 10
""")
top_titles = cursor.fetchall()

print(f"\n🔝 Top 10 Job Titles:")
for title, count in top_titles:
    print(f"   {title[:40]}...: {count} jobs")

conn.close()

print("\n" + "="*60)
print("✅ Database is ready for frontend development!")
print("="*60)