from typing import List, Tuple
from datetime import datetime
import pandas as pd
import sys
from memory_profiler import profile

@profile
def q1_memory(file_path: str) -> List[Tuple[datetime.date, str]]:
    # Leer el alrchivo y convertirlo a Dataframe
    df = pd.read_json(file_path, lines=True, chunksize=1000)
    top_dates_aggregated = {}

    for chunk in df:
        # Se crean/editan los campos necesarios para la consulta
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunk['date_convert'] = chunk['date'].dt.date
        chunk['username'] = pd.json_normalize(chunk['user'])['username']

        # tomamos las 10 fechas con mayor cantidad de tweets
        top10_dates = chunk.groupby(['date_convert']).size().reset_index(name='cant').sort_values(by='cant', ascending=False).head(10)
        # si ya existe en aggregated, sumar, si no existe, comparar para decidir si se queda o no
        for index, row in top10_dates.iterrows():
            if row['date_convert'] in top_dates_aggregated:
                top_dates_aggregated[row['date_convert']] += row['cant']
            else:
                top_dates_aggregated[row['date_convert']] = row['cant']
    
    top10_dates_aggregated = sorted(top_dates_aggregated.items(), key=lambda item: item[1], reverse=True)[:10]

    # para cada una de esas fechas, tomamos los usuarios con mas tweets
    df = pd.read_json(file_path, lines=True, chunksize=1000)
    results = {}
    for chunk in df:
        chunk = chunk.reset_index()
        chunk['date'] = pd.to_datetime(chunk['date'])
        chunk['date_convert'] = chunk['date'].dt.date
        chunk['username'] = pd.json_normalize(chunk['user'])['username']
        for top_date, _ in top10_dates_aggregated:
            date_users = chunk[chunk['date_convert'] == top_date].groupby('username').size().reset_index(name='cant').sort_values(by='cant', ascending=False)
            if top_date in results:
                pass
            else:
                results[top_date] = {}
            for _, row in date_users.iterrows():
                if row['username'] in results[top_date]:
                    results[top_date][row['username']] += row['cant']
                else:
                    results[top_date][row['username']] = row['cant']

    results_aggregated = []
    for top_date, users in results.items():
        top_user = sorted(users.items(), key=lambda item: item[1], reverse=True)[:1]
        results_aggregated.append((top_date, top_user[0][0]))

    return results_aggregated

if __name__ == "__main__":
    file_path = sys.argv[1]
    print(q1_memory(file_path))