import sqlite3
import sys

# Fix Unicode encoding for Windows console
if sys.platform == 'win32':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except AttributeError:
        # Python < 3.7
        import codecs
        sys.stdout = codecs.getwriter('utf-8')(sys.stdout.buffer, 'strict')

conn = sqlite3.connect('backend/data.db')
c = conn.cursor()

print("🔍 Starting deduplication...\n")

# Find all duplicate job title + company combinations
c.execute("""
    SELECT job_title, company_name, COUNT(*) as count
    FROM jobs 
    GROUP BY job_title, company_name 
    HAVING count > 1 
    ORDER BY count DESC
""")

duplicates = c.fetchall()
print(f"Found {len(duplicates)} duplicate positions\n")

total_deleted = 0
total_kept = 0

for job_title, company_name, count in duplicates:
    # Get all records for this duplicate group
    c.execute("""
        SELECT id, registered, quota
        FROM jobs
        WHERE job_title = ? AND company_name = ?
        ORDER BY registered DESC
    """, (job_title, company_name))
    
    records = c.fetchall()
    keep_id = records[0][0]  # ID with highest registered count
    delete_ids = [r[0] for r in records[1:]]
    
    print(f"📌 {count}x {job_title[:40]} @ {company_name[:35]}")
    print(f"   Keep (highest): ID {keep_id} with {records[0][1]} registered, {records[0][2]} quota")
    print(f"   Delete: IDs {delete_ids}")
    
    # Delete the duplicates (keeping the one with most applicants)
    for delete_id in delete_ids:
        c.execute("DELETE FROM jobs WHERE id = ?", (delete_id,))
        total_deleted += 1
    
    total_kept += 1
    print()

conn.commit()

# Verify results
c.execute("SELECT COUNT(*) FROM jobs")
remaining = c.fetchone()[0]

print("="*60)
print(f"✅ Deduplication complete!")
print(f"   Deleted: {total_deleted} duplicate records")
print(f"   Kept: {total_kept} primary records")
print(f"   Total jobs remaining: {remaining}")

# Show new statistics
c.execute("SELECT COUNT(*), MIN(registered), MAX(registered), AVG(registered) FROM jobs")
result = c.fetchone()
print(f"\nNew statistics:")
print(f"   Total jobs: {result[0]}")
print(f"   Min registered: {result[1]}")
print(f"   Max registered: {result[2]}")
print(f"   Avg registered: {result[3]:.1f}")

conn.close()
