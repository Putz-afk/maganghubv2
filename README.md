# 🚀 MagangHub v2 (Unofficial)

> A high-performance, user-friendly interface for browsing "Maganghub" internships. Built with Astro for speed and enhanced with smart filtering capabilities.

![Project Preview](public/screenshot.png)

## ⚠️ Important: Data Storage & Git
**Please read before cloning:**
This project uses a **Flat-File Architecture**. Instead of a database, all internship data is stored as individual JSON files.
* **Location:** `public/data/jobs/`
* **Volume:** This folder contains **tens of thousands** of small JSON files (one for each job).
* **Impact:** Cloning this repository might take longer than usual due to the high file count. 
* **Note:** If you are deploying to Vercel/Netlify, ensure your build command doesn't time out processing these files.

## 📋 Overview

The official internship portal can be overwhelming. **MagangHub v2** solves this by providing a lightning-fast static site that allows students to filter thousands of internships instantly. 

It features a custom-built **Smart Degree Filter** that cleans up messy data (e.g., merging "Bisnis" and "BIsnis") and I make it so its prioritizes relevant majors (like IT/Software) for easier discovery.

## 💾 Data Architecture & Scraping

This project does **not** fetch data live from the official API to avoid rate limits and slow loading times.

1.  **Data Collection (Scraping):** Data is scraped/fetched in bulk from the official portal.
    * Raw data is processed to create a lightweight `jobs-index.json` (for the search/filter grid).
    * Detailed data is saved into individual `[id].json` files.
2.  **Storage:** All data is stored locally in the `public/data` directory.
    * This allows the site to run **offline** (in development) and serve traffic with **zero database latency**.
3.  **Updates:** Data is a "snapshot" of the portal at the time of the last build.

## ✨ Key Features

* **⚡ Blazing Fast Performance:** Built with **Astro** (Static Site Generation), ensuring near-instant page loads.
* **🔍 Advanced Filtering:**
    * **Smart Major Filter:** Multi-select logic with "OR" capabilities.
    * **Data Cleaning:** Automatically normalizes inconsistent data inputs (e.g., removing brackets, fixing capitalization).
    * **Priority Sorting:** IT and Tech majors appear at the top of the list for quick access.
* **🗺️ Location Filtering:** Filter jobs by Province.
* **📄 Detailed Job Views:** dedicated static pages for every internship position.

## 🛠️ Tech Stack

* **Framework:** [Astro](https://astro.build/)
* **Language:** TypeScript / JavaScript
* **Data Source:** JSON (Static File System)
* **Deployment:** Vercel

## 📂 Project Structure

```bash
maganghub-v2/
├── 📂 frontend/                # [THE WEBSITE] Built with Astro
│   ├── 📂 public/              # Static assets served directly
│   │   ├── 📂 data/            # ⚠️ The "Bridge": Scripts output JSONs here, Astro reads them here
│   │   └── 📂 styles/          # Global CSS styles
│   ├── 📂 src/                 # Source code for the frontend application
│   │   ├── 📂 pages/           # Astro file-based routing
│   │   │   ├── 📂 jobs/
│   │   │   │   └── [id].astro  # Dynamic route: Displays details for a single job
│   │   │   ├── all-jobs.astro  # Page listing all available internships
│   │   │   └── index.astro     # Homepage with Search & Smart Filters
│   │   ├── 📂 components/      # Reusable UI elements
│   │   │   └── JobCard.astro   # Component for individual job preview cards
│   │   └── 📂 layouts/         # Shared page templates
│   │       └── Layout.astro    # Main shell (Header, Footer, Meta tags)
│   ├── astro.config.mjs        # Astro configuration file
│   └── package.json            # Frontend dependencies and scripts
│
├── 📂 scripts/                   # [THE COLLECTORS] Python scripts for data acquisition
│   ├── api_scraper.py            # Primary scraper logic for official API
│   ├── batch_scraper.py          # Optimized scraper using batching for speed
│   ├── deduplicate_jobs.py       # Maintenance: Removes duplicate job entries
│   ├── final_stats.py            # Debugging: Displays summary of scraped data
│   ├── generate_static_data.py   # Critical: Builds `index.json` & `stats.json` for frontend
│   ├── scrape_all_provinces.py   # Orchestrator: Runs scraping across all regions
│   ├── scrape_sulsel_complete.py # Targeted scraper: Fetches deep data for South Sulawesi only
│   ├── simple_scraper.py         # Minimal scraper for testing connectivity
│   └── view_data.py              # Utility: Inspects local JSON contents via CLI
│
└── 📂 backend/                # [THE PROCESSORS] Data enhancement logic
    ├── model.py               # ML Logic: Calculates "Recommended Degree" similarity
    ├── requirements.txt       # Python dependencies for scripts & backend
    └── run_init.py            # Utility: Applies ML model to existing JSON data
```

## 🚀 Getting Started

Follow these steps to run the project locally:

### 1. Clone the Repository

```bash
git clone [https://github.com/your-username/maganghub-v2.git](https://github.com/your-username/maganghub-v2.git)
cd maganghub-v2/frontend

```

### 2. Install Dependencies

```bash
npm install

```

### 3. Run Development Server

```bash
npm run dev

```

Open your browser and navigate to `http://localhost:4321`.

### 4. Build for Production

To generate the static site files:

```bash
npm run build

```

## 🧠 How the Smart Filter Works

The filter logic handles raw, messy data from the source:

1. **Normalization:** It strips `(Sarjana)`, `(Diploma)` and fixes Title Case.
2. **Aggregation:** It counts how many jobs exist for each major in the *current view*.
3. **Priority Sorting:** Majors defined in the priority list (e.g., `Informatika`, `Sistem Informasi`) are forced to the top of the filter list.

## 🤝 Contributing

Contributions are welcome! If you have better data sources or UI improvements:

1. Fork the Project
2. Create your Feature Branch (`git checkout -b feature/AmazingFeature`)
3. Commit your Changes (`git commit -m 'Add some AmazingFeature'`)
4. Push to the Branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📝 License

Distributed under the MIT License.

---

*Created with ❤️ from a jobseeker.*
