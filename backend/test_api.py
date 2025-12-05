import requests
import json

print('Testing new API endpoints...')
print('='*60)

try:
    # Test root
    response = requests.get('http://127.0.0.1:8000/')
    print('1. Root endpoint:', json.dumps(response.json(), indent=2)[:200] + '...')

    # Test stats
    response = requests.get('http://127.0.0.1:8000/stats')
    stats = response.json()
    print(f'\n2. Stats:')
    # Notice: No backslashes here anymore
    print(f"   Total jobs: {stats['total_jobs']}")
    print(f"   Total companies: {stats['total_companies']}")
    print(f"   Total quota: {stats['total_quota']} positions")
    
    # Check if 'jobs_by_province' exists safely
    prov_count = len(stats.get('jobs_by_province', []))
    print(f"   Provinces: {prov_count}")

    # Test jobs with filter
    print(f'\n3. Testing filters:')
    response = requests.get('http://127.0.0.1:8000/jobs?limit=5')
    jobs = response.json()
    print(f"   First 5 jobs: {len(jobs)} returned")
    
    for job in jobs[:2]:
        print(f"   - {job['job_title']} at {job['company_name']}")

    # Test province filter
    print(f'\n4. Testing province filter:')
    response = requests.get('http://127.0.0.1:8000/provinces')
    provinces = response.json()
    print(f"   Available provinces: {len(provinces)}")
    
    for prov in provinces[:3]:
        print(f"   - {prov['province']}: {prov['job_count']} jobs")

    print('\n✅ All tests passed!')

except Exception as e:
    print(f"\n❌ Error: {e}")
    print("Ensure your server is running at http://127.0.0.1:8000")