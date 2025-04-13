# src/analysis.py (Example)
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import os

PROCESSED_DATA_DIR = "data/processed/earthquakes"

def analyze_processed_data():
    if not os.path.exists(PROCESSED_DATA_DIR):
         print(f"Error: Processed data directory not found at {PROCESSED_DATA_DIR}")
         return

    # Read Parquet directory into Pandas DataFrame
    try:
        df = pd.read_parquet(PROCESSED_DATA_DIR)
        print(f"Loaded {len(df)} records into Pandas DataFrame.")
        print("\nDataFrame Info:")
        df.info()
        print("\nSample Data:")
        print(df.head())

        # Basic Analysis
        print("\nBasic Statistics (Magnitude):")
        print(df['magnitude'].describe())

        print("\nEarthquake Counts by Magnitude Type:")
        print(df['magnitude_type'].value_counts())

        # --- Basic Plotting (Requires matplotlib/seaborn) ---
        try:
            # Distribution of Magnitudes
            plt.figure(figsize=(10, 6))
            sns.histplot(df['magnitude'].dropna(), bins=30, kde=True)
            plt.title('Distribution of Earthquake Magnitudes')
            plt.xlabel('Magnitude')
            plt.ylabel('Frequency')
            plt.grid(axis='y', alpha=0.75)
            plt.savefig('magnitude_distribution.png') # Save the plot
            print("\nSaved magnitude distribution plot to magnitude_distribution.png")
            # plt.show() # Uncomment to display plot interactively

            # Scatter plot of earthquakes (Longitude vs Latitude)
            plt.figure(figsize=(12, 8))
            sns.scatterplot(x='longitude', y='latitude', size='magnitude', hue='magnitude',
                            data=df, palette='viridis', sizes=(10, 200), alpha=0.6)
            plt.title('Earthquake Locations and Magnitudes')
            plt.xlabel('Longitude')
            plt.ylabel('Latitude')
            plt.grid(True)
            plt.savefig('earthquake_locations.png') # Save the plot
            print("Saved earthquake location plot to earthquake_locations.png")
            # plt.show() # Uncomment to display plot interactively

        except ImportError:
            print("\nPlotting libraries (matplotlib, seaborn) not installed. Skipping plots.")
        # --- End Plotting ---

    except Exception as e:
        print(f"An error occurred during analysis: {e}")

if __name__ == "__main__":
    analyze_processed_data()