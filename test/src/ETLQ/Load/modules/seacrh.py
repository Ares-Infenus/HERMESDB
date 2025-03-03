import os
import pandas as pd
from rich.progress import Progress, SpinnerColumn, TextColumn

def search_by_registro_ids_efficient(file_path: str, target_ids: list, chunksize: int = 100000) -> list:
    """
    Returns a list of rows (as dictionaries) from a CSV file that contain
    'registro_id' values within target_ids, processing the file in chunks
    to reduce memory usage. A Rich progress bar is displayed to track progress.

    Parameters:
        file_path (str): Path to the CSV file.
        target_ids (list): List of registro_id values to search for.
        chunksize (int): Number of rows to read per chunk.
    
    Returns:
        List[dict]: A list of matching rows.
    """
    results = []
    
    # Initialize the rich progress bar with an indefinite total
    progress = Progress(
        SpinnerColumn(),
        TextColumn("[progress.description]{task.description}")
    )
    task = progress.add_task("Processing file...", total=None)
    
    with progress:
        # Process the CSV file in chunks
        for chunk in pd.read_csv(file_path, chunksize=chunksize):
            if 'registro_id' not in chunk.columns:
                raise ValueError("El CSV no contiene la columna 'registro_id'.")
            
            # Filter rows in the current chunk where 'registro_id' is in target_ids
            filtered = chunk[chunk['registro_id'].isin(target_ids)]
            if not filtered.empty:
                results.append(filtered)
            
            # Advance the progress bar for each processed chunk
            progress.advance(task)
    
    # Concatenate all matching chunks and convert to a list of dictionaries
    if results:
        return pd.concat(results).to_dict(orient='records')
    return []

if __name__ == "__main__":
    file_path = r"C:\Users\spinz\OneDrive\Documentos\Portafolio oficial\HERMESDB\HERMESDB\test\data\backup\datos_pre_mapping.csv"
    target_values = [77027691, 77051752]  # List of registro_id values to search for
    
    matching_rows = search_by_registro_ids_efficient(file_path, target_values, chunksize=100000)
    print(matching_rows)
