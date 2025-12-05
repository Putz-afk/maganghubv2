# Auto-Update Setup Guide

## Overview
Your job listings can now be automatically updated every 5 hours with fresh data from all Indonesian provinces.

## Features
✅ Scrapes **all 34 provinces** (not just Sulawesi Selatan)  
✅ Auto-updates **every 5 hours**  
✅ Automatically deduplicates positions  
✅ Regenerates static data  
✅ Rebuilds frontend automatically  

## How to Use

### Option 1: Automatic Setup (Recommended - Windows Only)
1. **Right-click** `setup-auto-update.bat`
2. Select **"Run as administrator"**
3. The task will be created and run every 5 hours automatically

**That's it!** Your data will update automatically.

### Option 2: Manual One-Time Update
Run this command from the project root:
```powershell
python scrape_all_provinces.py
```

### Option 3: Manual Auto-Update (All Platforms)
Run this in a terminal and it will keep updating every 5 hours:
```powershell
python scrape_all_provinces.py --auto
```
(Press Ctrl+C to stop)

---

## Understanding the Process

### What Happens During Auto-Update:
1. **Scrape** - Fetches latest job data from all 34 provinces
2. **Deduplicate** - Removes duplicate position entries
3. **Generate** - Creates optimized JSON files for frontend
4. **Build** - Rebuilds the frontend with fresh data

### Provinces Included:
- Aceh
- Sumatera Utara, Sumatera Barat, Riau, Jambi, Sumatera Selatan, Bengkulu, Lampung
- Kepulauan Bangka Belitung, Kepulauan Riau
- Jawa Barat, Jawa Tengah, DIY, Jawa Timur, Banten
- Bali
- Nusa Tenggara Barat, Nusa Tenggara Timur
- Kalimantan Barat, Kalimantan Tengah, Kalimantan Selatan, Kalimantan Timur, Kalimantan Utara
- Sulawesi Utara, Sulawesi Tengah, Sulawesi Selatan, Sulawesi Tenggara, Gorontalo, Sulawesi Barat
- Maluku, Maluku Utara
- Papua Barat, Papua

---

## Managing the Task (Windows Task Scheduler)

### View the Task:
1. Press **Windows + R**
2. Type `taskschd.msc` and press Enter
3. Look for "MagangHub-AutoUpdate"

### Disable/Enable Updates:
```powershell
# Disable (pauses updates)
schtasks /change /tn "MagangHub-AutoUpdate" /disable

# Enable (resumes updates)
schtasks /change /tn "MagangHub-AutoUpdate" /enable
```

### Delete the Task:
```powershell
schtasks /delete /tn "MagangHub-AutoUpdate" /f
```

### View Task History:
In Task Scheduler → Select "MagangHub-AutoUpdate" → View "History" tab

---

## Logs and Monitoring

Updates run silently in the background. To monitor:

### Windows Event Viewer:
1. Open **Event Viewer** (eventvwr.msc)
2. Go to **Windows Logs** → **System**
3. Filter for Task Scheduler events

### Manual Monitoring:
Modify `scrape_all_provinces.py` to log to a file for debugging

---

## Troubleshooting

### Task Not Running?
1. Make sure you ran `setup-auto-update.bat` as **Administrator**
2. Check Task Scheduler for errors
3. Verify Python path: `C:\Custom\Project2\maganghubv2\backend\venv\Scripts\python.exe`
4. Run manually first: `python scrape_all_provinces.py`

### What if Scraping Fails?
- Retries are built in (5-second retry on error)
- Previous data remains in database if update fails
- Check internet connection and MagangHub API status

### Want to Change Update Frequency?
Edit `setup-auto-update.bat` line with `/mo 5` (5 hours):
```batch
/mo 3        # Every 3 hours
/mo 6        # Every 6 hours
/mo 12       # Every 12 hours
/mo 24       # Every 24 hours (once daily)
```

---

## Performance Notes

- **Scraping time**: ~5-15 minutes (depending on total jobs across all provinces)
- **Database size**: ~1-2 MB for 1000+ jobs
- **Build time**: ~3-5 seconds
- **Total update time**: ~10-20 minutes

---

## Database

All data stored in: `backend/data.db` (SQLite)

To check database:
```powershell
python check_job.py  # View statistics
```

To backup before update:
```powershell
Copy-Item backend/data.db backend/data.db.backup
```

---

## Questions?

If something goes wrong, check:
1. Python installation: `python --version`
2. Database integrity: Open `backend/data.db` with SQLite
3. API connectivity: Try manually visiting the MagangHub API
4. Disk space: Ensure enough free space for database and static files
