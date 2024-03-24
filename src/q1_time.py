from typing import List, Tuple
from datetime import datetime
import pandas as pd
import sys
from memory_profiler import profile

@profile
def q1_time(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Leer el alrchivo y convertirlo a Dataframe
    df = pd.read_json(file_path, lines=True)

    # Se crean/editan los campos necesarios para la consulta
    df['date'] = pd.to_datetime(df['date'])
    df['date_convert'] = df['date'].dt.date
    df['username'] = pd.json_normalize(df['user'])['username']

    # tomamos las 10 fechas con mayor cantidad de tweets
    top10_dates  = df.groupby(['date_convert']).size().reset_index(name='cant').sort_values(by='cant', ascending=False).head(10)

    # para cada una de esas fechas, tomamos los usuarios con mas tweets
    results = []
    for top_date in top10_dates['date_convert']:
        results.append((top_date, df[df['date_convert'] == top_date].groupby('username').size().reset_index(name='cant').sort_values(by='cant', ascending=False).iloc[0]['username']))
        
    return results


if __name__ == "__main__":
    file_path = sys.argv[1]
    print(q1_time(file_path))