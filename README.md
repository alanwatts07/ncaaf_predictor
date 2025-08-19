# NCAA Football Prediction Engine

![Build Status](https://img.shields.io/badge/build-passing-brightgreen)
![Python Version](https://img.shields.io/badge/python-3.9%2B-blue)
![License](https://img.shields.io/badge/license-MIT-lightgrey)

A machine learning pipeline to predict NCAA Division I football game outcomes, including win/loss, point spread, and game totals. This project leverages an unsupervised neural network to learn team embeddings from historical data, which are then used by a predictive model to generate weekly forecasts and simulate full seasons.

## Features

-   **Weekly Predictions:** Generates predictions for win probability, point spread, and total points for upcoming games.
-   **Team Embeddings:** Utilizes a Variational Autoencoder (VME) to learn rich, numerical representations of team strength and play style from 2008-2024 data.
-   **Season Simulation:** Runs Monte Carlo simulations (100 runs per matchup) for the entire 2025 season to forecast team records and stats.
-   **Data Pipeline:** Includes a complete data pipeline for scraping, cleaning, and feature engineering.
-   **Backtesting:** Provides a robust framework for evaluating model performance against historical data.

## High-Level Architecture

The prediction process is a two-step system designed to separate feature learning from prediction:

1.  **Unsupervised Embedding Model (VME):** A Variational Autoencoder is trained on historical team statistics without using game outcomes. Its sole purpose is to create a dense, 32-dimension "embedding" or "fingerprint" that numerically represents a team's identity for a given season. Recent seasons (2022-2024) are weighted more heavily.

2.  **Prediction Head:** A simple and fast non-parametric model (like k-Nearest Neighbors) takes the embeddings of the two competing teams, adds matchup context (e.g., home-field advantage), and maps this information to a predicted point spread and total.

This separation allows the complex neural network to focus on understanding *what a team is*, while the simpler model focuses on predicting *what that team will do* in a specific game.

## Project Structure

```
ncaaf_predictor/
├── .gitignore
├── README.md
├── config.yml
├── requirements.txt
│
├── data/
│   ├── raw/
│   └── processed/
│
├── notebooks/
│   ├── 01_data_exploration.ipynb
│   └── 02_model_prototyping.ipynb
│
└── src/
    ├── data_pipeline/
    │   ├── collectors.py
    │   └── preprocessing.py
    │
    ├── features/
    │   └── build_features.py
    │
    ├── models/
    │   ├── vme_model.py
    │   └── prediction_head.py
    │
    ├── analysis/
    │   └── evaluate.py
    │
    └── main.py
```

## Setup and Installation

Follow these steps to set up the project environment.

**1. Clone the Repository**

```bash
git clone [https://github.com/your_username/ncaaf_predictor.git](https://github.com/your_username/ncaaf_predictor.git)
cd ncaaf_predictor
```

**2. Create a Virtual Environment**

It is highly recommended to use a virtual environment to manage project dependencies.

```bash
# For Mac/Linux
python3 -m venv venv
source venv/bin/activate

# For Windows
python -m venv venv
.\venv\Scripts\activate
```

**3. Install Dependencies**

Install all required Python libraries from the `requirements.txt` file.

```bash
pip install -r requirements.txt
```

**4. Configure Settings**

Before running the pipeline, you must configure your API keys and database settings.

-   Rename the `config.example.yml` file to `config.yml`.
-   Open `config.yml` and fill in your API keys for the College Football Data API, The Odds API, etc.
-   Update the database connection URL.

**Important:** The `config.yml` file is included in `.gitignore` and should **never** be committed to version control.

## Usage

The entire pipeline can be run from the `main.py` script in the `src/` directory. The script uses the `config.yml` file to determine which steps of the pipeline to execute.

To run the full pipeline (ingest, feature engineering, training, and simulation):

```bash
python src/main.py
```

You can control the pipeline's execution by modifying the `run_*` flags in your `config.yml` file. For example, to only run the data ingestion step:

```yaml
# in config.yml
run_ingest: true
run_training: false
run_sim2025: false
```

Prediction outputs and reports will be saved to the `out/` and `reports/` directories, respectively.

## Roadmap (Future Work)

-   **Player-Level Modeling:** Incorporate player-level data, injuries, and roster changes.
-   **Advanced Feature Engineering:** Integrate weather data, coaching changes, and recruiting class rankings.
-   **Market Line Modeling:** Use betting market data (lines and odds) as features to improve the model.
-   **Web Interface:** Build a simple web front-end (using FastAPI or Flask) to serve and visualize predictions.
