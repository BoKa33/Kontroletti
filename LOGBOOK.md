# 📓 Kontroletti Engineering Logbook

## 🕵️ Research Phase: The "Universal ID Bridge"
**Date:** March 11, 2026
**Goal:** Create a high-integrity "Raw Data Block" that merges official (DELFI) and community (GTFS.DE) transit data.

---

### 🔬 Experiment 1: The ID Mismatch (DELFI vs. GTFS.DE)
*   **Discovery:** DELFI uses standardized zHV string IDs (e.g., `de:06412:1502`), while GTFS.DE uses numeric, unstable IDs (e.g., `175272`).
*   **Outcome:** Direct ID matching is impossible. A "Bridge Table" is mandatory.

### 🔬 Experiment 2: Simple Spatial Matching
*   **Method:** Match stops within 25m with identical names.
*   **Result:** **Success Rate: 11.69%**.
*   **Reason for failure:** Name formatting (e.g., `S+U Alexanderplatz` vs `Alexanderplatz (Berlin)`) and "Parent/Child" hierarchy (Station vs. Platform).

### 🔬 Experiment 3: The Deep Matcher (cKDTree + Normalization)
*   **Method:** Used `scipy.cKDTree` for nationwide spatial indexing. Implemented "Forensic Normalization" (stripping `S+U`, `Bhf`, expanding `A.-Bebel` -> `August-Bebel`).
*   **Result:** **Success Rate: 91.00%** (~619k stops).
*   **Outcome:** Proved that a "Canonical Registry" is the correct way forward.

### 🔬 Experiment 4: The Rescue Mission (96.8%)
*   **Method:** Target the remaining 9% using `RapidFuzz` (Levenshtein) and directional normalization (`Ri.` -> `Richtung`).
*   **Result:** **Success Rate: 96.79%**.
*   **Conclusion:** The remaining 3.2% are mostly "Data Noise" (Private points, construction stops) and will be handled via **Synthetic IDs** (Stable IDs derived from lat/lon hashes).

---

### 🏗️ Architectural Rationale: Why we solve it this way

#### 1. The Canonical Registry
We do not trust external IDs. We generate our own **UUID Space**. If GTFS.DE changes its numeric IDs next month, our "Lat/Lon + Name" hash will ensure the station keeps its identity in our database.

#### 2. The "Trip DNA" Matcher
Real-time GPS updates (GTFS-RT) come with GTFS.DE IDs. To use the refined DELFI schedule, we match the **DNA of a trip**: the sequence of Canonical Stop IDs and their relative departure times. This bridges the "Community Real-time" world with the "Official Schedule" world.

#### 3. Modular "Raw Data Block"
The service is built with FastAPI/PostgreSQL to act as a **Data Refiner**. It provides a clean, standardized API for the **Matching Engine** (Block 2) so that block doesn't have to care about raw GTFS complexity.

---

### 🚀 What's next?
- [ ] Implement the `Trip DNA` background worker in production.
- [ ] Integrate the unofficial DB API for long-distance rail coverage.
- [ ] Build the first REST API endpoint: `GET /live/nearby`.
