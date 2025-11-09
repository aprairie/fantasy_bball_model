import pandas as pd
from models import engine # Assuming your models are in models.py

def dump_all_elo_to_csv_pandas(output_filename="elo_stats_all.csv"):
    """
    Queries player names and ALL their ELO stats (regular and guaranteed)
    and dumps them to a CSV file using pandas.
    """
    print(f"Connecting to the database...")
    
    # Updated query to include all the "_guaranteed" elo columns
    query = """
    SELECT
        p.name AS player_name,
        es.overall_elo,
        es.fg_pct_elo,
        es.ft_pct_elo,
        es.pts_elo,
        es.reb_elo,
        es.ast_elo,
        es.stl_elo,
        es.blk_elo,
        es.to_elo,
        es.tpm_elo,
        es.overall_elo_guaranteed,
        es.fg_pct_elo_guaranteed,
        es.ft_pct_elo_guaranteed,
        es.pts_elo_guaranteed,
        es.reb_elo_guaranteed,
        es.ast_elo_guaranteed,
        es.stl_elo_guaranteed,
        es.blk_elo_guaranteed,
        es.to_elo_guaranteed,
        es.tpm_elo_guaranteed,
        es.dropped_player
    FROM
        elo_stats AS es
    JOIN
        players AS p ON es.player_id = p.id
    ORDER BY
        p.name;
    """
    
    df = pd.read_sql(query, engine)
    df.to_csv(output_filename, index=False)
    
    print(f"Successfully dumped {len(df)} players to {output_filename}")

if __name__ == "__main__":
    dump_all_elo_to_csv_pandas()